/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

/**
 * Per-page composition of the {@code _id} metadata column for external datasets. The composed
 * value is a {@code <location>@<mtime>:<rowPosition>} string where {@code location} is a stable
 * file identity (the storage path), {@code mtime} is the file's last-modified epoch millis
 * ({@code 0} when the storage layer reports none), and {@code rowPosition} is the row's physical
 * position within that file, masked off from the optional
 * {@link ColumnExtractor#LOCAL_POSITION_BITS}-encoded extractor id used by the
 * deferred-extraction path. The mtime salt makes ids from a file replaced in place under the
 * same name distinct from the ids its predecessor produced — without it, a consumer caching by
 * {@code _id} would silently conflate rows from two different file generations.
 * <p>
 * Allocation discipline: one {@link BytesRef} per file for the prefix; one {@code byte[]} plus
 * one {@code int[]} of offsets per page; zero per-row allocation (decimal-encoding of the row
 * position runs inline on a stack-resident scratch buffer).
 */
public final class ExternalRowIdentity {

    /**
     * Mask covering only the per-extractor physical row identity bits, used to strip any encoded
     * extractor id off a {@code _rowPosition} value before it is rendered into the {@code _id}
     * string. The deferred-extraction path emits encoded {@code (id << LOCAL_POSITION_BITS) |
     * physical} values; we want only the physical part in the rendered id.
     */
    static final long LOCAL_POSITION_MASK = (1L << ColumnExtractor.LOCAL_POSITION_BITS) - 1L;

    /** Separator between location and row position in the rendered {@code _id}. */
    static final byte SEPARATOR = (byte) ':';

    /** Separator between location and the mtime salt in the rendered {@code _id}. */
    static final byte MTIME_SEPARATOR = (byte) '@';

    /** Maximum decimal digits in a {@code long} (signed, 19 digits for {@code Long.MAX_VALUE}). */
    private static final int MAX_LONG_DIGITS = 19;

    private ExternalRowIdentity() {}

    /**
     * Build the per-file prefix bytes ({@code <location>@<mtime>:}). One allocation per file. The
     * returned {@link BytesRef} is held by the producer iterator for the lifetime of the file
     * and reused across every page. {@code mtimeMillis} is the file's last-modified epoch millis;
     * callers pass {@code 0} when the storage layer reports none (the {@link FileList} convention
     * for a missing mtime), keeping the id shape uniform either way.
     */
    public static BytesRef prefix(StoragePath path, long mtimeMillis) {
        String location = path.toString();
        byte[] base = location.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] mtime = Long.toString(mtimeMillis).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] buf = new byte[base.length + 1 + mtime.length + 1];
        System.arraycopy(base, 0, buf, 0, base.length);
        buf[base.length] = MTIME_SEPARATOR;
        System.arraycopy(mtime, 0, buf, base.length + 1, mtime.length);
        buf[buf.length - 1] = SEPARATOR;
        return new BytesRef(buf, 0, buf.length);
    }

    /**
     * Compose an {@code _id} block for one page. Output position count matches
     * {@code rowPositionBlock.getPositionCount()}; null row-positions yield null {@code _id}. The
     * block allocates against {@code factory} so its breaker bytes follow the producer-thread
     * accounting path used by other constant-block allocations. Two-pass design: pass 1 walks
     * the row-position block and writes {@code prefix + decimal(physical)} for each row directly
     * into a single producer-side {@code backing} array while recording per-row offsets; pass 2
     * walks {@code offsets[]} and feeds an {@code (offset, length)} scratch view per row to the
     * vector / block builder. No {@link Long#toString} allocation.
     */
    public static BytesRefBlock composePage(BytesRef prefix, LongBlock rowPositionBlock, BlockFactory factory) {
        int positions = rowPositionBlock.getPositionCount();
        if (positions == 0) {
            return (BytesRefBlock) factory.newConstantNullBlock(0);
        }
        int prefixLen = prefix.length;
        // Worst-case allocation: every row present + each row's decimal expansion at MAX_LONG_DIGITS.
        // Bound is generous but fixed-size and avoids a second pass to size; the unused tail bytes
        // are dropped via the (offset, length) view inside the per-row BytesRef. multiplyExact
        // surfaces a pathological deep path × large page combination as ArithmeticException in
        // production rather than as a silent int overflow downstream.
        int worstCase = Math.multiplyExact(positions, prefixLen + MAX_LONG_DIGITS);
        byte[] backing = new byte[worstCase];
        int[] offsets = new int[positions + 1];
        byte[] digits = new byte[MAX_LONG_DIGITS];
        int cursor = 0;
        boolean anyNull = false;
        for (int i = 0; i < positions; i++) {
            offsets[i] = cursor;
            if (rowPositionBlock.isNull(i)) {
                anyNull = true;
                continue;
            }
            int valueIdx = rowPositionBlock.getFirstValueIndex(i);
            long encoded = rowPositionBlock.getLong(valueIdx);
            long physical = encoded & LOCAL_POSITION_MASK;
            System.arraycopy(prefix.bytes, prefix.offset, backing, cursor, prefixLen);
            cursor += prefixLen;
            int digitCount = encodeDecimal(physical, digits);
            // digits filled right-to-left in `digits` over the last digitCount positions
            System.arraycopy(digits, MAX_LONG_DIGITS - digitCount, backing, cursor, digitCount);
            cursor += digitCount;
        }
        offsets[positions] = cursor;

        if (anyNull) {
            // Mixed null/non-null: fall back to BytesRefBlock.Builder so per-row null bitmap is
            // recorded. Still single-page allocation; the temporary byte arrays we already built
            // are reused as the input bytes per row.
            try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(positions)) {
                BytesRef scratch = new BytesRef();
                scratch.bytes = backing;
                for (int i = 0; i < positions; i++) {
                    if (rowPositionBlock.isNull(i)) {
                        builder.appendNull();
                    } else {
                        scratch.offset = offsets[i];
                        scratch.length = offsets[i + 1] - offsets[i];
                        builder.appendBytesRef(scratch);
                    }
                }
                return builder.build();
            }
        }

        // Dense path: feed every row to the vector builder via a scratch view over the producer
        // backing array. The builder copies each appended BytesRef into its own internal buffer,
        // so the rendered vector is not literally backed by `backing` — the producer-side
        // allocations here are the decoding scratch (`backing` + `offsets`), not the vector's
        // storage.
        try (BytesRefVector.Builder vectorBuilder = factory.newBytesRefVectorBuilder(positions)) {
            BytesRef scratch = new BytesRef();
            scratch.bytes = backing;
            for (int i = 0; i < positions; i++) {
                scratch.offset = offsets[i];
                scratch.length = offsets[i + 1] - offsets[i];
                vectorBuilder.appendBytesRef(scratch);
            }
            return vectorBuilder.build().asBlock();
        }
    }

    /**
     * Decimal-encode {@code value} into {@code out} right-aligned. {@code out} must have at least
     * {@link #MAX_LONG_DIGITS} bytes. Returns the number of digits written.
     * <p>
     * Row position 0 is legitimate (the first row of a file) and renders as {@code "0"}. Negative
     * inputs can only come from corruption (e.g. an unmasked sentinel) and render as {@code "0"}
     * defensively; the {@code assert false} surfaces the bug class in CI while production stays
     * resilient.
     */
    static int encodeDecimal(long value, byte[] out) {
        if (value < 0L) {
            assert false : "ExternalRowIdentity.encodeDecimal called with negative value " + value;
            out[MAX_LONG_DIGITS - 1] = (byte) '0';
            return 1;
        }
        if (value == 0L) {
            out[MAX_LONG_DIGITS - 1] = (byte) '0';
            return 1;
        }
        int pos = MAX_LONG_DIGITS;
        long v = value;
        while (v > 0) {
            pos--;
            int digit = (int) (v % 10);
            out[pos] = (byte) ('0' + digit);
            v /= 10;
        }
        return MAX_LONG_DIGITS - pos;
    }
}
