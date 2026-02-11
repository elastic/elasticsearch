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
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;

public final class QuantizeDoubleCodecStage implements TransformEncoder, TransformDecoder {

    private final double maxError;

    public QuantizeDoubleCodecStage(double maxError) {
        if (maxError <= 0 || Double.isNaN(maxError) || Double.isInfinite(maxError)) {
            throw new IllegalArgumentException("maxError must be a finite positive number, got: " + maxError);
        }
        this.maxError = maxError;
    }

    @Override
    public byte id() {
        return StageId.QUANTIZE_DOUBLE.id;
    }

    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        final double step = 2.0 * maxError;

        for (int i = 0; i < valueCount; i++) {
            double v = NumericUtils.sortableLongToDouble(values[i]);
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                continue;
            }
            double quantized = AlpDoubleUtils.alpRound(v / step) * step;
            values[i] = NumericUtils.doubleToSortableLong(quantized);
        }

        // NOTE: Metadata layout: [step: Long (8 bytes, raw IEEE 754 bits of 2*maxError)].
        // Written as raw long bits via writeLong for exact round-trip fidelity.
        // Decode reads and discards this field to keep the metadata cursor aligned.
        context.metadata().writeLong(Double.doubleToRawLongBits(step));

        return valueCount;
    }

    public static int encodeStatic(final QuantizeDoubleCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    public static int decodeStatic(final QuantizeDoubleCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        // NOTE: Metadata layout: [step: Long (8 bytes)]. Read and discard to advance cursor.
        context.metadata().readLong();
        return valueCount;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof QuantizeDoubleCodecStage that && Double.compare(maxError, that.maxError) == 0);
    }

    @Override
    public int hashCode() {
        return Double.hashCode(maxError);
    }

    @Override
    public String toString() {
        return "QuantizeDoubleCodecStage{maxError=" + maxError + "}";
    }
}
