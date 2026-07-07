/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

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

    /**
     * Allocation-free scan reporting whether {@code [off, off+len)} is well-formed UTF-8.
     */
    public static boolean isWellFormed(byte[] bytes, int off, int len) {
        int i = off;
        int end = off + len;
        while (i < end) {
            int b0 = bytes[i] & 0xFF;
            if (b0 < 0x80) {
                // ASCII
                i++;
                continue;
            }
            int consumed = sequenceLength(bytes, i, end);
            if (consumed < 0) {
                return false;
            }
            i += consumed;
        }
        return true;
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
     */
    private static int sequenceLength(byte[] bytes, int i, int end) {
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
        while (i < end) {
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
