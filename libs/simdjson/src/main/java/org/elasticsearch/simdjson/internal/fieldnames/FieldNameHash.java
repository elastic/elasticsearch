/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Wyhash-based hash functions for JSON field names. These are shared across all
 * {@link FieldNameLookup} implementations.
 *
 * <p>{@link #hashName} computes a 32-bit hash of a byte range. {@link #scanAndHash}
 * finds the closing quote of a field name and computes the hash in a single pass,
 * avoiding the need to re-read the bytes.
 */
public final class FieldNameHash {

    private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private static final long WY_SECRET0 = 0xa0761d6478bd642fL;
    private static final long WY_SECRET1 = 0xe7037ed1a0b428dbL;
    private static final long WY_SECRET2 = 0x8ebc6af09c88c6e3L;

    private FieldNameHash() {}

    /**
     * Computes a 32-bit hash of {@code buf[off, off+len)} using a wyhash-style algorithm.
     * Uses {@link Math#unsignedMultiplyHigh} as the core mixing primitive — a single
     * instruction on x86-64 and AArch64. For field names &lt;= 8 bytes (the common case),
     * only 1 multiply-mix is needed. Returns 1 instead of 0 so that 0 can serve as the
     * empty-slot sentinel.
     */
    public static int hashName(byte[] buf, int off, int len) {
        long seed = WY_SECRET0;
        long a, b;

        if (len <= 8) {
            a = readSmall(buf, off, len);
            b = 0;
        } else if (len <= 16) {
            a = readLE8(buf, off);
            b = readLE8(buf, off + len - 8);
        } else {
            int pos = off;
            int rem = len;
            a = 0;
            b = 0;
            while (rem > 16) {
                seed = wymix(readLE8(buf, pos) ^ WY_SECRET1, readLE8(buf, pos + 8) ^ seed);
                pos += 16;
                rem -= 16;
            }
            // rem is 1..16. For rem >= 9, two overlapping 8-byte reads cover the span
            // without reading past off + len. For rem <= 8, use readSmall to stay in bounds.
            if (rem > 8) {
                a = readLE8(buf, pos);
                b = readLE8(buf, pos + rem - 8);
            } else {
                a = readSmall(buf, pos, rem);
                b = 0;
            }
        }

        long h = wymix(a ^ WY_SECRET1, b ^ seed) ^ WY_SECRET2 ^ len;
        h = wymix(h, h);
        int h32 = (int) (h ^ (h >>> 32));
        return h32 == 0 ? 1 : h32;
    }

    /**
     * Computes the wyhash from an already-loaded little-endian 8-byte word. The word must
     * contain the field name starting at bit 0 (i.e. the word was read at the field name's
     * start offset). Bytes at and beyond the closing quote may be present and are masked off
     * internally using {@code len}.
     *
     * <p>Produces the same hash value as {@link #hashName(byte[], int, int)} for the same
     * byte sequence — both use the same {@code readSmall} encoding. This avoids re-reading
     * the field name bytes from the buffer.
     *
     * @param word the 8-byte little-endian word containing the field name bytes
     * @param len  length of the field name (must be 0..8)
     * @return the 32-bit wyhash, guaranteed non-zero
     */
    public static int hashWord(long word, int len) {
        assert len >= 0 && len <= 8 : "hashWord requires len in 0..8, got " + len;
        long a = readSmallFromWord(word, len);
        long h = wymix(a ^ WY_SECRET1, WY_SECRET0) ^ WY_SECRET2 ^ len;
        h = wymix(h, h);
        int h32 = (int) (h ^ (h >>> 32));
        return h32 == 0 ? 1 : h32;
    }

    /**
     * Extracts the {@link #readSmall}-compatible encoding from an 8-byte LE word.
     * For len >= 4 this mirrors the overlapping-int-read encoding; for len 1..3
     * this mirrors the 3-byte encoding; for len 0 returns 0.
     */
    static long readSmallFromWord(long word, int len) {
        if (len >= 4) {
            long lo = word & 0xFFFFFFFFL;
            long hi = (word >>> ((len - 4) * 8)) & 0xFFFFFFFFL;
            return lo | (hi << 32);
        }
        if (len > 0) {
            int a = (int) (word) & 0xFF;
            int b = (int) (word >>> ((len >>> 1) * 8)) & 0xFF;
            int c = (int) (word >>> ((len - 1) * 8)) & 0xFF;
            return (a << 16) | (b << 8) | c;
        }
        return 0;
    }

    /**
     * Masks an 8-byte little-endian word to zero out bytes at position {@code len} and beyond.
     * The result can be used directly as a prefix8 value for {@link FrozenFieldNameTable} lookup.
     *
     * @param word the 8-byte little-endian word
     * @param len  number of valid bytes (0..8); if >= 8 the word is returned unmodified
     * @return the word with bytes at index {@code len}..7 zeroed
     */
    public static long maskWord(long word, int len) {
        if (len >= 8) return word;
        if (len <= 0) return 0;
        long mask = (1L << (len * 8)) - 1;
        return word & mask;
    }

    /**
     * Scans for the closing quote and computes the wyhash in a single pass. Each 8-byte word
     * is read once, checked for quote/backslash, and the same bytes are fed into
     * {@link #hashName} to avoid re-reading from memory.
     *
     * <p>Returns the hash in the upper 32 bits and the field name length in the lower 32 bits,
     * packed as a single long. Returns -1 if a backslash is found.
     *
     * @param buf      source buffer; the closing quote must be present within the buffer bounds
     * @param startIdx byte index of the first character after the opening quote
     * @return {@code ((long)hash << 32) | len}, or -1 if the name contains a backslash
     */
    public static long scanAndHash(byte[] buf, int startIdx) {
        int pos = startIdx;
        int loopBound = buf.length - 8;

        while (pos <= loopBound) {
            long word = readLE8(buf, pos);
            long xq = word ^ 0x2222222222222222L;
            long xb = word ^ 0x5C5C5C5C5C5C5C5CL;
            long qh = (xq - 0x0101010101010101L) & ~xq & 0x8080808080808080L;
            long bh = (xb - 0x0101010101010101L) & ~xb & 0x8080808080808080L;

            if ((qh | bh) != 0) {
                if (bh != 0 && (qh == 0 || (Long.numberOfTrailingZeros(bh) <= Long.numberOfTrailingZeros(qh)))) {
                    return -1;
                }
                int len = (pos - startIdx) + (Long.numberOfTrailingZeros(qh) >>> 3);
                int h = hashName(buf, startIdx, len);
                return ((long) h << 32) | (len & 0xFFFFFFFFL);
            }
            pos += 8;
        }
        // Scalar tail: scan byte-by-byte for the closing quote or backslash
        while (true) {
            byte b = buf[pos];
            if (b == '"') {
                int len = pos - startIdx;
                int h = hashName(buf, startIdx, len);
                return ((long) h << 32) | (len & 0xFFFFFFFFL);
            }
            if (b == '\\') {
                return -1;
            }
            pos++;
        }
    }

    /**
     * Provides access to the little-endian int VarHandle for implementations that need to
     * compare inline quads against buffer contents.
     */
    static VarHandle intHandle() {
        return INT_LE;
    }

    private static long wymix(long a, long b) {
        long lo = a * b;
        long hi = Math.unsignedMultiplyHigh(a, b);
        return lo ^ hi;
    }

    private static long readLE8(byte[] buf, int off) {
        return (long) LONG_LE.get(buf, off);
    }

    private static long readSmall(byte[] buf, int off, int len) {
        if (len >= 4) {
            long lo = Integer.toUnsignedLong((int) INT_LE.get(buf, off));
            long hi = Integer.toUnsignedLong((int) INT_LE.get(buf, off + len - 4));
            return lo | (hi << 32);
        }
        if (len > 0) {
            int a = buf[off] & 0xFF;
            int b = buf[off + (len >>> 1)] & 0xFF;
            int c = buf[off + len - 1] & 0xFF;
            return (a << 16) | (b << 8) | c;
        }
        return 0;
    }
}
