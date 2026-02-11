/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;

public final class DeltaDeltaCodecStage implements TransformEncoder, TransformDecoder {

    public static final DeltaDeltaCodecStage INSTANCE = new DeltaDeltaCodecStage();

    @Override
    public byte id() {
        return StageId.DELTA_DELTA.id;
    }

    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        if (valueCount < 2) {
            return valueCount;
        }

        // NOTE: Fused single-pass delta-of-delta. Computes second-order differences
        // in one forward pass using two scalar accumulators, avoiding two separate
        // backward passes over the array.
        final long firstValue = values[0];
        final long secondValue = values[1];
        final long firstDelta = secondValue - firstValue;

        long prevOriginal = secondValue;
        long prevDelta = firstDelta;

        values[0] = 0;
        values[1] = 0;

        for (int i = 2; i < valueCount; i++) {
            final long currOriginal = values[i];
            final long currDelta = currOriginal - prevOriginal;
            values[i] = currDelta - prevDelta;
            prevOriginal = currOriginal;
            prevDelta = currDelta;
        }

        final MetadataWriter meta = context.metadata();
        meta.writeZLong(firstDelta);
        meta.writeZLong(firstValue - firstDelta);
        return valueCount;
    }

    public static int encodeStatic(final DeltaDeltaCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    public static int decodeStatic(final DeltaDeltaCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        final long firstDelta = context.metadata().readZLong();
        final long base = context.metadata().readZLong();

        // NOTE: Two sequential prefix-sums. First recovers deltas from
        // delta-of-deltas, second recovers values from deltas. Both are
        // branch-free tight loops for SIMD-friendly decode throughput.
        values[0] += firstDelta;
        for (int i = 1; i < valueCount; i++) {
            values[i] += values[i - 1];
        }

        values[0] += base;
        for (int i = 1; i < valueCount; i++) {
            values[i] += values[i - 1];
        }

        return valueCount;
    }

    @Override
    public String toString() {
        return "DeltaDeltaCodecStage";
    }
}
