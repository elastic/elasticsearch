/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;
import java.util.Objects;

public final class ChimpDoubleTransformEncodeStage implements TransformEncoder {

    public static final int GROUP_SIZE_STORAGE = 8;
    public static final int GROUP_SIZE_BALANCED = 16;
    public static final int GROUP_SIZE_SPEED = 32;
    static final int DEFAULT_GROUP_SIZE = GROUP_SIZE_BALANCED;

    private final int groupSize;
    private final double quantizeStep;

    public ChimpDoubleTransformEncodeStage(int blockSize) {
        this(blockSize, DEFAULT_GROUP_SIZE, 0.0);
    }

    public ChimpDoubleTransformEncodeStage(int blockSize, int groupSize) {
        this(blockSize, groupSize, 0.0);
    }

    public ChimpDoubleTransformEncodeStage(int blockSize, int groupSize, double maxError) {
        assert groupSize > 0 : "groupSize must be positive: " + groupSize;
        assert (groupSize & (groupSize - 1)) == 0 : "groupSize must be a power of 2: " + groupSize;
        assert maxError >= 0 : "maxError must be non-negative: " + maxError;
        this.groupSize = groupSize;
        this.quantizeStep = maxError > 0 ? 2.0 * maxError : 0.0;
    }

    @Override
    public byte id() {
        return StageId.CHIMP_DOUBLE_STAGE.id;
    }

    @Override
    public int maxMetadataBytes(int blockSize) {
        return Byte.BYTES + ((blockSize + groupSize - 1) / groupSize) * Long.BYTES;
    }

    // NOTE: Metadata layout: [1-byte groupSize] [group_0 ref][group_1 ref]...[group_N ref].
    // Each group reference is 8 bytes. The reference for each group is its first value.
    // All values within a group are XORed against the group reference, producing small
    // residuals for downstream BitPack. The decode loop is a simple broadcast-XOR per group —
    // no ring buffer, no selectors, no loop-carried dependencies — which C2 can
    // auto-vectorize into SIMD.
    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        assert valueCount > 0 : "valueCount must be positive";

        if (quantizeStep > 0) {
            QuantizeUtils.quantizeDoubles(values, valueCount, quantizeStep);
        }

        // NOTE: convert sortable-longs to raw IEEE-754 bits so that XOR
        // produces small residuals for consecutive similar doubles.
        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.sortableDoubleBits(values[i]);
        }

        if (shouldSkip(values, valueCount)) {
            // NOTE: convert back — skip means no transform, downstream expects sortable longs
            for (int i = 0; i < valueCount; i++) {
                values[i] = NumericUtils.doubleToSortableLong(Double.longBitsToDouble(values[i]));
            }
            return valueCount;
        }

        final var metadata = context.metadata();
        metadata.writeByte((byte) groupSize);

        for (int g = 0; g < valueCount; g += groupSize) {
            final long ref = values[g];
            metadata.writeLong(ref);
            final int end = Math.min(g + groupSize, valueCount);
            for (int i = g; i < end; i++) {
                values[i] ^= ref;
            }
        }

        return valueCount;
    }

    private static boolean shouldSkip(final long[] values, int valueCount) {
        long rawOr = 0;
        for (int i = 0; i < valueCount; i++) {
            rawOr |= values[i];
        }
        long xorOr = 0;
        for (int i = 1; i < valueCount; i++) {
            xorOr |= values[i] ^ values[i - 1];
        }
        return 64 - Long.numberOfLeadingZeros(xorOr) >= 64 - Long.numberOfLeadingZeros(rawOr);
    }

    public static int encodeStatic(
        final ChimpDoubleTransformEncodeStage stage,
        final long[] values,
        int valueCount,
        final EncodingContext context
    ) throws IOException {
        return stage.encode(values, valueCount, context);
    }

    int groupSize() {
        return groupSize;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
            || (o instanceof ChimpDoubleTransformEncodeStage that
                && groupSize == that.groupSize
                && Double.compare(quantizeStep, that.quantizeStep) == 0);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupSize, quantizeStep);
    }

    @Override
    public String toString() {
        return "ChimpDoubleTransformEncodeStage{groupSize=" + groupSize + ", quantizeStep=" + quantizeStep + "}";
    }
}
