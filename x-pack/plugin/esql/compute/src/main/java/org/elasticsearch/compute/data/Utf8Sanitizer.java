/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;

import java.util.Arrays;

/**
 * Validates and, if necessary, repairs UTF-8 byte sequences that enter ES|QL as {@code KEYWORD}
 * values from external sources (Parquet, ORC, Arrow).
 * <p>
 * ES|QL assumes every {@code KEYWORD}/{@code TEXT} {@link BytesRef} holds well-formed UTF-8: for
 * example {@code Utf8AscTopNEncoder}/{@code Utf8DescTopNEncoder} index a 248-entry code-length table
 * by the raw lead byte, so a byte in {@code 0xF8..0xFF} throws {@link ArrayIndexOutOfBoundsException}
 * on the sort path. External writers (notably Spark) can emit malformed UTF-8 in string-annotated
 * columns, so the byte hygiene ES|QL relies on cannot be assumed at the source boundary.
 * <p>
 * {@link #sanitize(BytesRef)} returns the input unchanged when it is already well-formed (the common,
 * fast path with no allocation) and otherwise returns a copy in which each maximal ill-formed
 * subsequence is replaced by a single Unicode replacement character {@code U+FFFD} ({@code EF BF BD}).
 * The "substitution of maximal subparts" behaviour matches the Unicode recommendation and the output
 * of ICU / Spark, so downstream {@code KEYWORD} operations become total without silently dropping or
 * merging distinct malformed inputs.
 */
public final class Utf8Sanitizer {

    private Utf8Sanitizer() {}

    /** {@code U+FFFD} REPLACEMENT CHARACTER encoded as UTF-8. */
    private static final byte[] REPLACEMENT = { (byte) 0xEF, (byte) 0xBF, (byte) 0xBD };

    /** Mask selecting the high bit of every byte in a {@code long}; a zero result means all-ASCII. */
    private static final long ASCII_MASK = 0x8080808080808080L;

    /** ASCII skip block: eight little-endian {@code long} loads, one mask test. */
    private static final int ASCII_BLOCK_BYTES = 64;

    /**
     * Advances {@code i} past consecutive 64-byte all-ASCII blocks. On the first block that contains a
     * non-ASCII byte, returns {@code i} at that block's start (does not skip it) so the caller can
     * resume at the current offset — including mid-block after a 2-byte/3-byte run.
     */
    private static int skipAsciiBlocks(byte[] bytes, int i, int end) {
        while (i + ASCII_BLOCK_BYTES <= end) {
            long acc = (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i) | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i + 8)
                | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i + 16) | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i + 24)
                | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i + 32) | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i + 40)
                | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i + 48) | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i + 56);
            if ((acc & ASCII_MASK) != 0) {
                return i;
            }
            i += ASCII_BLOCK_BYTES;
        }
        return i;
    }

    /**
     * Advances {@code i} past a maximal run of all-ASCII 8-byte words, stopping at the first word
     * containing a non-ASCII byte or at {@code wordEnd} (the exclusive bound past which an 8-byte read
     * would cross {@code end}). Returns {@code i} unchanged when no full ASCII word starts at {@code i}.
     */
    private static int skipAsciiWords(byte[] bytes, int i, int wordEnd) {
        while (i < wordEnd && (((long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i)) & ASCII_MASK) == 0) {
            i += Long.BYTES;
        }
        return i;
    }

    /**
     * Allocation-free scan reporting whether {@code [off, off+len)} is well-formed UTF-8.
     * <p>
     * Long ASCII runs skip 64-byte blocks (eight {@code long} loads, one mask), then the existing
     * 8-byte word loop for the tail. The 64-byte skip is not entered when {@code len < 64}: short
     * per-value checks (dict, ORC, fallback) must not pay block setup they can never fill.
     * Consecutive 2-byte ({@code C2..DF}), unambiguous 3-byte ({@code E1..EC}/{@code EE..EF}) and
     * unambiguous 4-byte ({@code F0} with second byte {@code 90..BF}, plus {@code F1..F3}) sequences
     * are consumed as runs; after a run the scan tries an 8-byte ASCII word at the current offset
     * and only resumes 64-byte blocks after a clean word. A one-byte ASCII gap (a space in a
     * Cyrillic phrase) therefore does not pay eight loads that cannot succeed. {@code E0}/{@code ED}/
     * {@code F0} overlong / {@code F4}, truncation and junk go through {@link #sequenceLength}.
     * <p>
     * A lookup-table DFA stays out: it was never measured against this 2-byte/3-byte run shape,
     * and {@code sequenceLength} remains the oracle for the remaining leads. Revisit only if the
     * existing {@code ascii_short}/{@code ascii_url}/{@code cyrillic} microbenches lose on mixed
     * or CJK text.
     */
    public static boolean isWellFormed(byte[] bytes, int off, int len) {
        int i = off;
        int end = off + len;
        int wordEnd = end - Long.BYTES + 1;
        // Short inputs never fill a 64-byte block; skipAsciiBlocks would no-op but still costs
        // outer-loop setup on every per-value sanitize (dict / ORC / fallback).
        final boolean useBlocks = len >= ASCII_BLOCK_BYTES;
        outer: while (i < end) {
            if (useBlocks) {
                i = skipAsciiBlocks(bytes, i, end);
            }
            while (i < end) {
                i = skipAsciiWords(bytes, i, wordEnd);
                if (i >= end) {
                    break outer;
                }
                int b0 = bytes[i] & 0xFF;
                if (b0 < 0x80) {
                    // Leftover ASCII in a dirty word: stay in the inner loop so we do not re-issue
                    // a 64-byte probe that cannot succeed while that word is still ahead.
                    i++;
                    continue;
                }
                if (b0 >= 0xC2 && b0 < 0xE0) {
                    // 2-byte run C2..DF 80..BF (Cyrillic and similar). Stay here until the run ends.
                    do {
                        if (i + 1 >= end || isCont(bytes[i + 1]) == false) {
                            return false;
                        }
                        i += 2;
                        if (i >= end) {
                            return true;
                        }
                        b0 = bytes[i] & 0xFF;
                    } while (b0 >= 0xC2 && b0 < 0xE0);
                    if (useBlocks && resumeAsciiBlocksAfterRun(bytes, i, wordEnd)) {
                        continue outer;
                    }
                    continue;
                }
                if (b0 >= 0xE1 && b0 <= 0xEF && b0 != 0xED) {
                    // 3-byte run with an unambiguous lead. E0/ED stay in sequenceLength forever:
                    // isCont alone would accept overlong (E0 80..9F) and surrogates (ED A0..BF).
                    do {
                        if (i + 2 >= end || isCont(bytes[i + 1]) == false || isCont(bytes[i + 2]) == false) {
                            return false;
                        }
                        i += 3;
                        if (i >= end) {
                            return true;
                        }
                        b0 = bytes[i] & 0xFF;
                    } while (b0 >= 0xE1 && b0 <= 0xEF && b0 != 0xED);
                    if (useBlocks && resumeAsciiBlocksAfterRun(bytes, i, wordEnd)) {
                        continue outer;
                    }
                    continue;
                }
                if (b0 == 0xF0 || (b0 >= 0xF1 && b0 <= 0xF3)) {
                    // 4-byte run: F0 90..BF (U+10000..U+3FFFF, emoji / CJK Ext. B) and F1..F3
                    // 80..BF ×3. F0 80..8F is overlong; F4 >U+10FFFF stays in sequenceLength.
                    do {
                        if (i + 3 >= end || isCont(bytes[i + 2]) == false || isCont(bytes[i + 3]) == false) {
                            return false;
                        }
                        int b1 = bytes[i + 1] & 0xFF;
                        if (b0 == 0xF0) {
                            if (b1 < 0x90 || b1 > 0xBF) {
                                return false;
                            }
                        } else if (isCont(bytes[i + 1]) == false) {
                            return false;
                        }
                        i += 4;
                        if (i >= end) {
                            return true;
                        }
                        b0 = bytes[i] & 0xFF;
                    } while (b0 == 0xF0 || (b0 >= 0xF1 && b0 <= 0xF3));
                    if (useBlocks && resumeAsciiBlocksAfterRun(bytes, i, wordEnd)) {
                        continue outer;
                    }
                    continue;
                }
                int consumed = sequenceLength(bytes, i, end);
                if (consumed < 0) {
                    return false;
                }
                i += consumed;
                if (useBlocks && resumeAsciiBlocksAfterRun(bytes, i, wordEnd)) {
                    continue outer;
                }
            }
        }
        return true;
    }

    /**
     * {@code true} when {@code [off, off+len)} is shorter than a 64-byte block, or the first 64
     * bytes are all ASCII. The PLAIN page path uses this to avoid a region scan of Cyrillic/CJK
     * pages, where length prefixes sitting between multi-byte chars are slower than per-value.
     */
    public static boolean startsWithAsciiBlock(byte[] bytes, int off, int len) {
        if (len < ASCII_BLOCK_BYTES) {
            return true;
        }
        // Fail fast on the first dirty word (Cyrillic/CJK pages). The kernel's skipAsciiBlocks
        // still ORs all eight loads: that shape is for all-ASCII skip, not this reject gate.
        if ((((long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, off)) & ASCII_MASK) != 0) {
            return false;
        }
        long acc = (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, off + 8) | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, off + 16)
            | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, off + 24) | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, off + 32)
            | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, off + 40) | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, off + 48)
            | (long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, off + 56);
        return (acc & ASCII_MASK) == 0;
    }

    /**
     * {@code true} when an 8-byte all-ASCII word starts at {@code i}, so a 64-byte block skip is
     * worth trying. A failed probe here is one load, not eight.
     */
    private static boolean resumeAsciiBlocksAfterRun(byte[] bytes, int i, int wordEnd) {
        return i < wordEnd && (((long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, i)) & ASCII_MASK) == 0;
    }

    /**
     * Returns {@code in} unchanged when it is well-formed UTF-8, otherwise a new {@link BytesRef}
     * (offset 0) with every maximal ill-formed subsequence replaced by {@code U+FFFD}.
     */
    public static BytesRef sanitize(BytesRef in) {
        if (in == null) {
            return null;
        }
        if (isWellFormed(in.bytes, in.offset, in.length)) {
            return in;
        }
        return new BytesRef(repair(in.bytes, in.offset, in.length));
    }

    /**
     * Length of the well-formed UTF-8 sequence starting at {@code i}, or the negated length of the
     * maximal ill-formed subpart (always {@code <= -1}) when the sequence is malformed. The negated
     * length lets {@link #repair} advance by exactly the maximal subpart, emitting one {@code U+FFFD}
     * per Unicode's substitution recommendation.
     * <p>
     * Package-private so tests can walk this as the {@link #isWellFormed} oracle. Do not change the
     * contract without updating that differential.
     */
    static int sequenceLength(byte[] bytes, int i, int end) {
        int b0 = bytes[i] & 0xFF;
        if (b0 < 0x80) {
            return 1;
        }
        // Continuation byte or C0/C1 (always overlong) as a lead: ill-formed, maximal subpart is one byte.
        if (b0 < 0xC2) {
            return -1;
        }
        if (b0 < 0xE0) {
            // 2-byte sequence C2..DF 80..BF
            if (i + 1 >= end || isCont(bytes[i + 1]) == false) {
                return -1;
            }
            return 2;
        }
        if (b0 < 0xF0) {
            // 3-byte sequence; the second byte range excludes overlong (E0) and surrogates (ED).
            int lo = (b0 == 0xE0) ? 0xA0 : 0x80;
            int hi = (b0 == 0xED) ? 0x9F : 0xBF;
            if (i + 1 >= end || inRange(bytes[i + 1], lo, hi) == false) {
                return -1;
            }
            if (i + 2 >= end || isCont(bytes[i + 2]) == false) {
                // Maximal subpart is the two valid leading bytes.
                return -2;
            }
            return 3;
        }
        if (b0 < 0xF5) {
            // 4-byte sequence; the second byte range excludes overlong (F0) and > U+10FFFF (F4).
            int lo = (b0 == 0xF0) ? 0x90 : 0x80;
            int hi = (b0 == 0xF4) ? 0x8F : 0xBF;
            if (i + 1 >= end || inRange(bytes[i + 1], lo, hi) == false) {
                return -1;
            }
            if (i + 2 >= end || isCont(bytes[i + 2]) == false) {
                return -2;
            }
            if (i + 3 >= end || isCont(bytes[i + 3]) == false) {
                return -3;
            }
            return 4;
        }
        // 0xF5..0xFF: never a valid lead byte.
        return -1;
    }

    private static byte[] repair(byte[] bytes, int off, int len) {
        // Worst case each input byte becomes a 3-byte replacement; grow-on-demand keeps the common
        // "few bad bytes" case compact without a second pass.
        byte[] out = new byte[len + 8];
        int outLen = 0;
        int i = off;
        int end = off + len;
        int wordEnd = end - Long.BYTES + 1;
        while (i < end) {
            // Bulk-copy runs of ASCII words instead of re-deriving a 1-byte sequence length per byte.
            int runEnd = skipAsciiWords(bytes, i, wordEnd);
            if (runEnd > i) {
                int runLen = runEnd - i;
                out = ensureCapacity(out, outLen + runLen);
                System.arraycopy(bytes, i, out, outLen, runLen);
                outLen += runLen;
                if (runEnd >= end) {
                    break;
                }
                i = runEnd;
            }
            int consumed = sequenceLength(bytes, i, end);
            if (consumed > 0) {
                out = ensureCapacity(out, outLen + consumed);
                System.arraycopy(bytes, i, out, outLen, consumed);
                outLen += consumed;
                i += consumed;
            } else {
                out = ensureCapacity(out, outLen + REPLACEMENT.length);
                System.arraycopy(REPLACEMENT, 0, out, outLen, REPLACEMENT.length);
                outLen += REPLACEMENT.length;
                i += -consumed;
            }
        }
        return outLen == out.length ? out : Arrays.copyOf(out, outLen);
    }

    private static byte[] ensureCapacity(byte[] out, int needed) {
        if (needed <= out.length) {
            return out;
        }
        return Arrays.copyOf(out, Math.max(needed, out.length + (out.length >> 1)));
    }

    private static boolean isCont(byte b) {
        return (b & 0xC0) == 0x80;
    }

    private static boolean inRange(byte b, int lo, int hi) {
        int v = b & 0xFF;
        return v >= lo && v <= hi;
    }
}
