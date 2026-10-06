/*
 * Copyright (c) 2025-2026, s0up and the autobrr contributors.
 * SPDX-License-Identifier: GPL-2.0-or-later
 */

import { describe, expect, it } from "vitest"

import {
  cursorKey,
  parseLogCursor,
  parseStreamSignal,
  shouldAppendLine
} from "@/lib/log-stream"

describe("parseLogCursor", () => {
  it("parses a valid cursor", () => {
    expect(parseLogCursor("0123456789abcdef-42")).toEqual({ epoch: "0123456789abcdef", seq: 42 })
  })

  it("rejects malformed cursors", () => {
    expect(parseLogCursor("")).toBeNull()
    expect(parseLogCursor("not-a-cursor")).toBeNull()
    expect(parseLogCursor("0123-42")).toBeNull()
    expect(parseLogCursor("g123456789abcdef-42")).toBeNull()
    expect(parseLogCursor("0123456789abcdef-0")).toBeNull()
    expect(parseLogCursor("0123456789abcdef-x")).toBeNull()
  })
})

describe("cursorKey", () => {
  it("round-trips through parseLogCursor", () => {
    const cursor = { epoch: "abcdef0123456789", seq: 7 }
    expect(parseLogCursor(cursorKey(cursor))).toEqual(cursor)
  })
})

describe("shouldAppendLine", () => {
  const epoch = "0123456789abcdef"

  it("accepts the first line", () => {
    expect(shouldAppendLine({ epoch, seq: 1 }, null)).toBe(true)
  })

  it("accepts a newer line from the same epoch", () => {
    expect(shouldAppendLine({ epoch, seq: 11 }, { epoch, seq: 10 })).toBe(true)
  })

  it("drops a duplicate or older line from the same epoch", () => {
    expect(shouldAppendLine({ epoch, seq: 10 }, { epoch, seq: 10 })).toBe(false)
    expect(shouldAppendLine({ epoch, seq: 9 }, { epoch, seq: 10 })).toBe(false)
  })

  it("accepts a line from a new epoch arriving after a reset", () => {
    expect(shouldAppendLine({ epoch: "fedcba9876543210", seq: 1 }, { epoch, seq: 1000 })).toBe(true)
  })
})

describe("parseStreamSignal", () => {
  it("parses a reset payload", () => {
    expect(parseStreamSignal("reset", "{\"reason\":\"restart\"}")).toEqual({
      kind: "reset",
      reason: "restart",
    })
  })

  it("parses a gap payload", () => {
    expect(parseStreamSignal("gap", "{\"reason\":\"buffer_overflow\"}")).toEqual({
      kind: "gap",
      reason: "buffer_overflow",
    })
  })

  it("rejects invalid JSON or missing reason", () => {
    expect(parseStreamSignal("gap", "not json")).toBeNull()
    expect(parseStreamSignal("gap", "{}")).toBeNull()
    expect(parseStreamSignal("reset", "{\"reason\":\"\"}")).toBeNull()
    expect(parseStreamSignal("reset", "{\"reason\":5}")).toBeNull()
  })
})
