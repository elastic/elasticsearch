/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.Arrays;

/**
 * Replaces a block made of runs with the runs themselves, so the terminal packs nothing at all.
 *
 * <p>A column ordered by the field it holds arrives this way: each value repeats for as long as the
 * documents holding it, and a block covers one or two of them. Bit-packing spends a width on every value in
 * such a block to say what a length and a value already said.
 *
 * <p>The runs are written to {@code params} as a length and a value apiece and the block is zeroed, which
 * the terminal packs at no width. How many there are is not written: they cover the block exactly, so the
 * lengths say where they end. They are read back beside the block they belong to, since the encoder lays
 * the params down immediately after it.
 */
public final class RunTransform implements BlockTransform {

    static final byte ID = 6;

    /**
     * The values a run must cover on average for the block to be made of runs. A run costs a length and a
     * value however short it is, so a block of many short runs is not one this describes more cheaply than
     * the terminal packs it, whatever the widths happen to be.
     */
    static final int MIN_RUN = 4;

    private final int[] lengths;
    private final long[] values;

    public RunTransform(int blockSize) {
        if (blockSize < 1) {
            throw new IllegalArgumentException("blockSize must be at least 1, got: " + blockSize);
        }
        final int maxRuns = blockSize / MIN_RUN + 1;
        this.lengths = new int[maxRuns];
        this.values = new long[maxRuns];
    }

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public boolean tryEncode(long[] block, int valueCount, MetadataWriter params) throws IOException {
        int runs = 1;
        for (int i = 1; i < valueCount; ++i) {
            if (block[i] != block[i - 1]) {
                if (++runs * MIN_RUN > valueCount) {
                    return false;
                }
            }
        }
        if (runs == 1) {
            // One run is what the offset already reduces to nothing: the block becomes zeroes and the
            // terminal packs it at no width, for less than a length and a value cost here.
            return false;
        }

        // Collected so the runs are priced as they will be written rather than estimated.
        int at = 0;
        long previous = 0;
        long runBits = 0;
        long or = 0;
        lengths[0] = 0;
        values[0] = block[0];
        for (int i = 0; i < valueCount; ++i) {
            or |= block[i];
            if (i > 0 && block[i] != block[i - 1]) {
                runBits += vIntBits(lengths[at]) + vLongBits(zigZag(values[at] - previous));
                previous = values[at];
                values[++at] = block[i];
                lengths[at] = 0;
            }
            lengths[at]++;
        }
        runBits += vIntBits(lengths[at]) + vLongBits(zigZag(values[at] - previous));
        assert at + 1 == runs : (at + 1) + " != " + runs;

        if (runBits >= (long) DocValuesForUtil.roundBits(PackedInts.unsignedBitsRequired(or)) * valueCount) {
            return false;
        }

        previous = 0;
        for (int r = 0; r < runs; ++r) {
            params.writeVInt(lengths[r]);
            params.writeZLong(values[r] - previous);
            previous = values[r];
        }
        Arrays.fill(block, 0, valueCount, 0L);
        return true;
    }

    @Override
    public void decode(long[] block, int valueCount, MetadataReader params) throws IOException {
        int at = 0;
        long previous = 0;
        // The runs cover the block exactly, so how many there are is what the lengths add up to.
        while (at < valueCount) {
            final int length = params.readVInt();
            final long value = previous + params.readZLong();
            Arrays.fill(block, at, at + length, value);
            at += length;
            previous = value;
        }
    }

    private static long zigZag(long value) {
        return (value << 1) ^ (value >> 63);
    }

    /** Bytes a vint of {@code value} takes, as bits, which is what the run's length costs. */
    private static long vIntBits(int value) {
        return vLongBits(Integer.toUnsignedLong(value));
    }

    /** Bytes a vlong of {@code value} takes, as bits, which is what the run's value costs. */
    private static long vLongBits(long value) {
        return (long) Math.max(1, (Long.SIZE - Long.numberOfLeadingZeros(value) + 6) / 7) * Byte.SIZE;
    }
}
