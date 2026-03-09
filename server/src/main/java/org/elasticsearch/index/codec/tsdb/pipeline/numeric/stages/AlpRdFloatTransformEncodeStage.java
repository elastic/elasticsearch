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

public final class AlpRdFloatTransformEncodeStage implements TransformEncoder {

    static final byte MODE_DECIMAL = 0x01;
    static final byte MODE_ALP_RD = 0x02;
    static final int MIN_PREFIX_LENGTH = 2;

    private final int maxExponent;
    private final float quantizeStep;
    private final int[] efOut = new int[2];
    private final int[] candE = new int[AlpFloatUtils.CAND_POOL_SIZE];
    private final int[] candF = new int[AlpFloatUtils.CAND_POOL_SIZE];
    private final int[] candCount = new int[AlpFloatUtils.CAND_POOL_SIZE];
    private final int[] positions;
    private final int[] exceptions;
    private int cachedAlpE = -1;
    private int cachedAlpF = -1;

    public AlpRdFloatTransformEncodeStage(int blockSize) {
        this.maxExponent = AlpFloatUtils.MAX_EXPONENT;
        this.quantizeStep = 0.0f;
        this.positions = new int[blockSize];
        this.exceptions = new int[blockSize];
    }

    // NOTE: Derives maxExponent = ceil(-log10(maxError)) and fuses quantization
    // (step = 2 * maxError) into this stage, eliminating a separate quantize pass.
    public AlpRdFloatTransformEncodeStage(int blockSize, double maxError) {
        assert maxError > 0 : "maxError must be positive: " + maxError;
        this.maxExponent = Math.min((int) Math.ceil(-Math.log10(maxError)), AlpFloatUtils.MAX_EXPONENT);
        this.quantizeStep = (float) (2.0 * maxError);
        this.positions = new int[blockSize];
        this.exceptions = new int[blockSize];
    }

    @Override
    public byte id() {
        return StageId.ALP_RD_FLOAT_STAGE.id;
    }

    @Override
    public int maxMetadataBytes(int blockSize) {
        final int maxPercent = AlpFloatUtils.maxExceptionPercent(Integer.SIZE, AlpFloatUtils.FLOAT_EXCEPTION_COST);
        final int maxExc = (blockSize * maxPercent) / 100;
        return 8 + maxExc * 10;
    }

    // NOTE: Metadata layout (mode dispatch):
    // MODE_DECIMAL (0x01): [mode: byte] [e: byte] [f: byte] [excCount: VInt]
    // then [excCount × (position: VInt, value: ZInt)].
    // MODE_ALP_RD (0x02): [mode: byte] [prefix: ZInt].
    // No metadata is written when neither mode is beneficial; the pipeline
    // skips this stage on decode. Same as AlpRdDoubleTransformEncodeStage
    // but with 32-bit zigzag-encoded prefixes and exception values.
    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        assert valueCount > 0 : "valueCount must be positive";

        if (quantizeStep > 0) {
            QuantizeUtils.quantizeFloats(values, valueCount, quantizeStep);
        }

        int bestExceptions;
        int bestE;
        int bestF;
        if (cachedAlpE >= 0) {
            bestE = cachedAlpE;
            bestF = cachedAlpF;
            bestExceptions = AlpFloatUtils.countExceptionsFloat(values, valueCount, bestE, bestF);
            final int cacheMaxAllowed = (valueCount * AlpFloatUtils.CACHE_VALIDATION_THRESHOLD) / 100;
            if (bestExceptions > cacheMaxAllowed) {
                bestExceptions = AlpFloatUtils.findBestEFFloatTopK(values, valueCount, maxExponent, efOut, candE, candF, candCount);
                bestE = efOut[0];
                bestF = efOut[1];
                cachedAlpE = bestE;
                cachedAlpF = bestF;
            }
        } else {
            bestExceptions = AlpFloatUtils.findBestEFFloatTopK(values, valueCount, maxExponent, efOut, candE, candF, candCount);
            bestE = efOut[0];
            bestF = efOut[1];
            cachedAlpE = bestE;
            cachedAlpF = bestF;
        }

        final int bitsSaved = AlpFloatUtils.computeBitSavings(values, valueCount, bestE, bestF);
        if (bitsSaved > 0) {
            final int maxAllowed = (valueCount * AlpFloatUtils.maxExceptionPercent(bitsSaved, AlpFloatUtils.FLOAT_EXCEPTION_COST)) / 100;
            if (bestExceptions <= maxAllowed) {
                return encodeDecimal(values, valueCount, context, bestE, bestF);
            }
        }

        return encodeAlpRdOrSkip(values, valueCount, context);
    }

    private int encodeDecimal(final long[] values, int valueCount, final EncodingContext context, int bestE, int bestF) {
        final int excCount = AlpFloatUtils.alpTransformBlock(values, valueCount, bestE, bestF, positions, exceptions);

        final var metadata = context.metadata();
        metadata.writeByte(MODE_DECIMAL);
        metadata.writeByte((byte) bestE);
        metadata.writeByte((byte) bestF);
        metadata.writeVInt(excCount);

        for (int i = 0; i < excCount; i++) {
            metadata.writeVInt(positions[i]);
            metadata.writeZInt(exceptions[i]);
        }
        return valueCount;
    }

    private static int encodeAlpRdOrSkip(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        int orAll = 0;
        final int firstRaw = NumericUtils.sortableFloatBits((int) values[0]);
        for (int i = 1; i < valueCount; i++) {
            final int raw = NumericUtils.sortableFloatBits((int) values[i]);
            orAll |= (raw ^ firstRaw);
        }

        final int prefixLength = Integer.numberOfLeadingZeros(orAll);
        if (prefixLength < MIN_PREFIX_LENGTH) {
            return valueCount;
        }

        final int prefix = firstRaw & (-1 << (32 - prefixLength));
        final int rightBits = 32 - prefixLength;
        final int tailMask = (rightBits == 0) ? 0 : (1 << rightBits) - 1;

        final var metadata = context.metadata();
        // NOTE: prefixLength is not written — the decoder reconstructs values via
        // (prefix | tail), and prefix already has zeros in the tail bit positions.
        metadata.writeByte(MODE_ALP_RD);
        metadata.writeZInt(prefix);

        for (int i = 0; i < valueCount; i++) {
            final int raw = NumericUtils.sortableFloatBits((int) values[i]);
            values[i] = raw & tailMask;
        }

        return valueCount;
    }

    public static int encodeStatic(
        final AlpRdFloatTransformEncodeStage stage,
        final long[] values,
        int valueCount,
        final EncodingContext context
    ) throws IOException {
        return stage.encode(values, valueCount, context);
    }

    @Override
    public boolean equals(Object o) {
        return this == o
            || (o instanceof AlpRdFloatTransformEncodeStage that
                && maxExponent == that.maxExponent
                && Float.compare(quantizeStep, that.quantizeStep) == 0);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxExponent, quantizeStep);
    }

    @Override
    public String toString() {
        return "AlpRdFloatTransformEncodeStage{maxExponent=" + maxExponent + ", quantizeStep=" + quantizeStep + "}";
    }
}
