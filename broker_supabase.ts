#!/usr/bin/env bun
/**
 * claude-peers broker daemon — Supabase backend
 *
 * Drop-in replacement for broker.ts using Supabase (PostgreSQL) instead of SQLite.
 * Enables cross-machine peer communication (company PC ↔ home PC).
 *
 * Same HTTP API as the original broker.ts.
 * Run: bun broker_supabase.ts
 */

import postgres from "postgres";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_URL =
  process.env.CLAUDE_PEERS_SUPABASE_URL ??
  "postgresql://postgres.gfbxccxhnazkgfgqcijl:66659570Pp!@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres";

// Hostname to distinguish company PC vs home PC
const HOSTNAME = process.env.COMPUTERNAME ?? process.env.HOSTNAME ?? "unknown";

// Peers not seen for > 5 minutes are considered stale (for cross-machine peers)
const STALE_TIMEOUT_MS = 5 * 60 * 1000;

const sql = postgres(DB_URL, { max: 5 });

// --- Cleanup helpers ---

async function cleanStalePeers() {
  // For same-machine peers: check PID
  const sameMachinePeers = await sql<{ id: string; pid: number }[]>`
    SELECT id, pid FROM claude_peers WHERE hostname = ${HOSTNAME}
  `;
  for (const peer of sameMachinePeers) {
    try {
      process.kill(peer.pid, 0);
    } catch {
      await sql`DELETE FROM claude_peers WHERE id = ${peer.id}`;
    }
  }

  // For cross-machine peers: expire by last_seen timeout
  const cutoff = new Date(Date.now() - STALE_TIMEOUT_MS).toISOString();
  await sql`
    DELETE FROM claude_peers
    WHERE hostname != ${HOSTNAME} AND last_seen < ${cutoff}
  `;
}

cleanStalePeers();
setInterval(cleanStalePeers, 30_000);

// --- ID generator ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

// --- Handlers ---

async function handleRegister(body: RegisterRequest): Promise<RegisterResponse> {
  const id = generateId();

  // Remove existing registration for same PID on same host
  await sql`
    DELETE FROM claude_peers WHERE pid = ${body.pid} AND hostname = ${HOSTNAME}
  `;

  await sql`
    INSERT INTO claude_peers (id, pid, hostname, cwd, git_root, tty, summary, registered_at, last_seen)
    VALUES (${id}, ${body.pid}, ${HOSTNAME}, ${body.cwd}, ${body.git_root ?? null},
            ${body.tty ?? null}, ${body.summary ?? ""}, now(), now())
  `;
  return { id };
}

async function handleHeartbeat(body: HeartbeatRequest): Promise<void> {
  await sql`UPDATE claude_peers SET last_seen = now() WHERE id = ${body.id}`;
}

async function handleSetSummary(body: SetSummaryRequest): Promise<void> {
  await sql`UPDATE claude_peers SET summary = ${body.summary} WHERE id = ${body.id}`;
}

async function handleListPeers(body: ListPeersRequest): Promise<Peer[]> {
  let peers: any[];

  switch (body.scope) {
    case "directory":
      peers = await sql`SELECT * FROM claude_peers WHERE cwd = ${body.cwd}`;
      break;
    case "repo":
      if (body.git_root) {
        peers = await sql`SELECT * FROM claude_peers WHERE git_root = ${body.git_root}`;
      } else {
        peers = await sql`SELECT * FROM claude_peers WHERE cwd = ${body.cwd}`;
      }
      break;
    default: // "machine" or undefined → all peers
      peers = await sql`SELECT * FROM claude_peers`;
  }

  if (body.exclude_id) {
    peers = peers.filter((p: any) => p.id !== body.exclude_id);
  }

  // For same-machine peers, verify PID; for remote peers, trust last_seen
  const alive: Peer[] = [];
  for (const p of peers) {
    if (p.hostname === HOSTNAME) {
      try {
        process.kill(p.pid, 0);
        alive.push(p as Peer);
      } catch {
        await sql`DELETE FROM claude_peers WHERE id = ${p.id}`;
      }
    } else {
      alive.push(p as Peer);
    }
  }
  return alive;
}

async function handleSendMessage(body: SendMessageRequest): Promise<{ ok: boolean; error?: string }> {
  const target = await sql`SELECT id FROM claude_peers WHERE id = ${body.to_id}`;
  if (target.length === 0) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }
  await sql`
    INSERT INTO claude_peer_messages (from_id, to_id, text, sent_at, delivered)
    VALUES (${body.from_id}, ${body.to_id}, ${body.text}, now(), false)
  `;
  return { ok: true };
}

async function handlePollMessages(body: PollMessagesRequest): Promise<PollMessagesResponse> {
  const messages = await sql<Message[]>`
    SELECT * FROM claude_peer_messages
    WHERE to_id = ${body.id} AND delivered = false
    ORDER BY sent_at ASC
  `;
  if (messages.length > 0) {
    const ids = messages.map((m: any) => m.id);
    await sql`UPDATE claude_peer_messages SET delivered = true WHERE id = ANY(${ids})`;
  }
  return { messages };
}

async function handleUnregister(body: { id: string }): Promise<void> {
  await sql`DELETE FROM claude_peers WHERE id = ${body.id}`;
}

// Rate limit: 5 sec per peer per room (drop, not debounce)
const roomRateLimit = new Map<string, number>();

async function handleSendToRoom(body: {
  from_id: string;
  room_id: string;
  message: string;
}): Promise<{ ok: boolean; sent_to: number; error?: string }> {
  const key = `${body.from_id}:${body.room_id}`;
  const now = Date.now();
  const last = roomRateLimit.get(key) ?? 0;
  if (now - last < 5000) {
    return { ok: false, sent_to: 0, error: "Rate limited (5s per peer per room)" };
  }
  roomRateLimit.set(key, now);

  // Get all peers except sender
  const peers = await sql<{ id: string }[]>`
    SELECT id FROM claude_peers WHERE id != ${body.from_id}
  `;
  if (peers.length === 0) {
    return { ok: true, sent_to: 0 };
  }

  for (const peer of peers) {
    await sql`
      INSERT INTO claude_peer_messages (from_id, to_id, text, sent_at, delivered, room_id)
      VALUES (${body.from_id}, ${peer.id}, ${body.message}, now(), false, ${body.room_id})
    `;
  }
  return { ok: true, sent_to: peers.length };
}

async function handleGetRoomMessages(body: {
  room_id: string;
  since_timestamp?: string;
  limit?: number;
}): Promise<{ messages: Message[] }> {
  const limit = body.limit ?? 50;
  const since = body.since_timestamp ?? new Date(0).toISOString();

  const messages = await sql<Message[]>`
    SELECT * FROM claude_peer_messages
    WHERE room_id = ${body.room_id}
      AND sent_at > ${since}
    ORDER BY sent_at ASC
    LIMIT ${limit}
  `;
  return { messages };
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        const count = await sql`SELECT count(*) FROM claude_peers`;
        return Response.json({ status: "ok", peers: Number(count[0].count), backend: "supabase", hostname: HOSTNAME });
      }
      return new Response("claude-peers broker (supabase)", { status: 200 });
    }

    try {
      const body = await req.json();
      switch (path) {
        case "/register":
          return Response.json(await handleRegister(body as RegisterRequest));
        case "/heartbeat":
          await handleHeartbeat(body as HeartbeatRequest);
          return Response.json({ ok: true });
        case "/set-summary":
          await handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(await handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(await handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(await handlePollMessages(body as PollMessagesRequest));
        case "/unregister":
          await handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        case "/send-to-room":
          return Response.json(await handleSendToRoom(body as { from_id: string; room_id: string; message: string }));
        case "/get-room-messages":
          return Response.json(await handleGetRoomMessages(body as { room_id: string; since_timestamp?: string; limit?: number }));
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (backend: supabase, host: ${HOSTNAME})`);
