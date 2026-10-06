/*
 * Copyright (c) 2025-2026, s0up and the autobrr contributors.
 * SPDX-License-Identifier: GPL-2.0-or-later
 */

// Cursor and control-event helpers for the log SSE stream
// (GET /api/logs/stream). The server frames every log line with
// "id: <epoch>-<seq>"; named "reset"/"gap" events carry a JSON reason.

export interface LogCursor {
  // Hub epoch, 16 hex characters. Changes when the server restarts.
  epoch: string
  // Monotonically increasing log sequence within an epoch (>= 1).
  seq: number
}

const CURSOR_RE = /^([0-9a-f]{16})-(\d+)$/

// parseLogCursor parses an SSE id ("<epoch>-<sequence>"). Returns null for
// malformed ids and sequence zero.
export function parseLogCursor(value: string): LogCursor | null {
  const match = CURSOR_RE.exec(value)
  if (!match) {
    return null
  }
  const seq = Number.parseInt(match[2], 10)
  if (!Number.isSafeInteger(seq) || seq <= 0) {
    return null
  }
  return { epoch: match[1], seq }
}

// cursorKey renders the cursor in the wire form used by the "after" query
// parameter.
export function cursorKey(cursor: LogCursor): string {
  return `${cursor.epoch}-${cursor.seq}`
}

// shouldAppendLine decides whether an incoming line continues the stream
// relative to the last line already shown. Same-epoch replays at or behind
// the last cursor are duplicates; a different epoch only arrives together
// with a reset event (the view was cleared) and is always accepted.
export function shouldAppendLine(cursor: LogCursor, last: LogCursor | null): boolean {
  if (!last || cursor.epoch !== last.epoch) {
    return true
  }
  return cursor.seq > last.seq
}

export type LogStreamSignalKind = "reset" | "gap"

export interface LogStreamSignal {
  kind: LogStreamSignalKind
  reason: string
}

// parseStreamSignal parses the JSON data payload of a named reset/gap event.
export function parseStreamSignal(kind: LogStreamSignalKind, data: string): LogStreamSignal | null {
  try {
    const parsed = JSON.parse(data) as { reason?: unknown }
    if (typeof parsed.reason !== "string" || parsed.reason === "") {
      return null
    }
    return { kind, reason: parsed.reason }
  } catch {
    return null
  }
}
