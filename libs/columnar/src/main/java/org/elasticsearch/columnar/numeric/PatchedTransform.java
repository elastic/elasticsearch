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
 * Sets aside the few values in a block that are much wider than the rest, so the terminal packs at the width
 * the block's common case needs rather than the width its widest value forces.
 *
 * <p>The shape this pays on is a block that is almost all one value. A keyword column where most documents
 * hold the same term names it with the lowest ordinal, and a value the dictionary does not hold takes the
 * highest, so one escaped document widens every value beside it.
 *
 * <p>A value set aside is packed as zero and written to {@code params}, and put back on the way out.
 */
public final class PatchedTransform implements BlockTransform {

    static final byte ID = 5;

    /** Values of each width in the block being encoded, reused across blocks rather than allocated per block. */
    private final int[] atWidth = new int[Long.SIZE + 1];

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public boolean tryEncode(long[] block, int valueCount, MetadataWriter params) throws IOException {
        Arrays.fill(atWidth, 0);
        long max = 0;
        for (int i = 0; i < valueCount; ++i) {
            final long value = block[i];
            // A negative needs its sign bit packed, which no narrower width holds. The pipeline offsets a
            // block into [0, max - min] before this runs, so an ordinary one arrives non-negative.
            if (value < 0) {
                return false;
            }
            max = Math.max(max, value);
            atWidth[PackedInts.unsignedBitsRequired(value)]++;
        }
        final int width = PackedInts.unsignedBitsRequired(max);
        if (width == 0) {
            return false;
        }

        // Turned into the values above each candidate width, which is what packing at that width leaves to
        // set aside, by accumulating downwards from the widest.
        for (int w = width - 1; w >= 0; --w) {
            atWidth[w] += atWidth[w + 1];
        }

        int narrowest = width;
        long cheapest = packedBits(width, valueCount);
        for (int w = 0; w < width; ++w) {
            final int exceptions = atWidth[w + 1];
            if (exceptional(exceptions, valueCount) == false) {
                continue;
            }
            final long cost = packedBits(w, valueCount) + (long) exceptions * exceptionBits(width, valueCount);
            if (cost < cheapest) {
                cheapest = cost;
                narrowest = w;
            }
        }
        if (narrowest == width) {
            return false;
        }

        final long cap = (1L << narrowest) - 1;
        params.writeVInt(atWidth[narrowest + 1]);
        for (int i = 0; i < valueCount; ++i) {
            if (block[i] > cap) {
                params.writeVInt(i);
                params.writeVLong(block[i]);
                block[i] = 0;
            }
        }
        return true;
    }

    @Override
    public void decode(long[] block, int valueCount, MetadataReader params) throws IOException {
        final int exceptions = params.readVInt();
        for (int e = 0; e < exceptions; ++e) {
            block[params.readVInt()] = params.readVLong();
        }
    }

    /**
     * Whether {@code exceptions} of {@code valueCount} are few enough to be exceptional. Not a budget, since
     * the search above prices what is worth setting aside, but the premise: packing at the width a block's
     * common case needs presumes it has one, and two populations of comparable size give it none.
     */
    private static boolean exceptional(int exceptions, int valueCount) {
        return exceptions * 2 < valueCount;
    }

    /** What {@link ForTerminal} spends on a block at this width, which is what it rounds the width up to. */
    private static long packedBits(int width, int valueCount) {
        return (long) DocValuesForUtil.roundBits(width) * valueCount;
    }

    /**
     * What one value set aside costs: its position as a vint and its own bytes as a vlong. The position is
     * charged at the width the block's last one would need, since which positions a candidate width sets
     * aside is not known while it is still a candidate.
     */
    private static long exceptionBits(int width, int valueCount) {
        return (long) (vIntBytes(valueCount - 1) + (width + 6) / 7) * Byte.SIZE;
    }

    /** Bytes a vint of {@code value} occupies. */
    private static int vIntBytes(int value) {
        return (Integer.SIZE - 1 - Integer.numberOfLeadingZeros(value | 1)) / 7 + 1;
    }
}
