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

public final class AlpRdDoubleTransformEncodeStage implements TransformEncoder {

    static final byte MODE_DECIMAL = 0x01;
    static final byte MODE_ALP_RD = 0x02;
    static final int MIN_PREFIX_LENGTH = 2;

    private final int maxExponent;
    private final double quantizeStep;
    private final int[] efOut = new int[2];
    private final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] positions;
    private final long[] exceptions;
    private int cachedAlpE = -1;
    private int cachedAlpF = -1;

    public AlpRdDoubleTransformEncodeStage(int blockSize) {
        this.maxExponent = AlpDoubleUtils.MAX_EXPONENT;
        this.quantizeStep = 0.0;
        this.positions = new int[blockSize];
        this.exceptions = new long[blockSize];
    }

    // NOTE: Derives maxExponent = ceil(-log10(maxError)) and fuses quantization
    // (step = 2 * maxError) into this stage, eliminating a separate quantize pass.
    public AlpRdDoubleTransformEncodeStage(int blockSize, double maxError) {
        assert maxError > 0 : "maxError must be positive: " + maxError;
        this.maxExponent = Math.min((int) Math.ceil(-Math.log10(maxError)), AlpDoubleUtils.MAX_EXPONENT);
        this.quantizeStep = 2.0 * maxError;
        this.positions = new int[blockSize];
        this.exceptions = new long[blockSize];
    }

    @Override
    public byte id() {
        return StageId.ALP_RD_DOUBLE_STAGE.id;
    }

    @Override
    public int maxMetadataBytes(int blockSize) {
        final int maxPercent = AlpDoubleUtils.maxExceptionPercent(Long.SIZE, AlpDoubleUtils.DOUBLE_EXCEPTION_COST);
        final int maxExc = (blockSize * maxPercent) / 100;
        return 8 + maxExc * 13;
    }

    // NOTE: Metadata layout (mode dispatch):
    // MODE_DECIMAL (0x01): [mode: byte] [e: byte] [f: byte] [excCount: VInt]
    // then [excCount × (position: VInt, value: Long)].
    // MODE_ALP_RD (0x02): [mode: byte] [prefix: Long].
    // No metadata is written when neither mode is beneficial; the pipeline
    // skips this stage on decode. Values are transformed in-place.
    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        assert valueCount > 0 : "valueCount must be positive";

        if (quantizeStep > 0) {
            QuantizeUtils.quantizeDoubles(values, valueCount, quantizeStep);
        }

        int bestExceptions;
        int bestE;
        int bestF;
        if (cachedAlpE >= 0) {
            bestE = cachedAlpE;
            bestF = cachedAlpF;
            bestExceptions = AlpDoubleUtils.countExceptions(values, valueCount, bestE, bestF);
            final int cacheMaxAllowed = (valueCount * AlpDoubleUtils.CACHE_VALIDATION_THRESHOLD) / 100;
            if (bestExceptions > cacheMaxAllowed) {
                bestExceptions = AlpDoubleUtils.findBestEFDoubleTopK(values, valueCount, maxExponent, efOut, candE, candF, candCount);
                bestE = efOut[0];
                bestF = efOut[1];
                cachedAlpE = bestE;
                cachedAlpF = bestF;
            }
        } else {
            bestExceptions = AlpDoubleUtils.findBestEFDoubleTopK(
                values,
                valueCount,
                AlpDoubleUtils.MAX_EXPONENT,
                efOut,
                candE,
                candF,
                candCount
            );
            bestE = efOut[0];
            bestF = efOut[1];
            cachedAlpE = bestE;
            cachedAlpF = bestF;
        }

        final int bitsSaved = AlpDoubleUtils.computeBitSavings(values, valueCount, bestE, bestF);
        if (bitsSaved > 0) {
            final int maxAllowed = (valueCount * AlpDoubleUtils.maxExceptionPercent(bitsSaved, AlpDoubleUtils.DOUBLE_EXCEPTION_COST)) / 100;
            if (bestExceptions <= maxAllowed) {
                return encodeDecimal(values, valueCount, context, bestE, bestF);
            }
        }

        return encodeAlpRdOrSkip(values, valueCount, context);
    }

    private int encodeDecimal(final long[] values, int valueCount, final EncodingContext context, int bestE, int bestF) throws IOException {
        final int excCount = AlpDoubleUtils.alpTransformBlock(values, valueCount, bestE, bestF, positions, exceptions);

        final var metadata = context.metadata();
        metadata.writeByte(MODE_DECIMAL);
        metadata.writeByte((byte) bestE);
        metadata.writeByte((byte) bestF);
        metadata.writeVInt(excCount);

        for (int i = 0; i < excCount; i++) {
            metadata.writeVInt(positions[i]);
            metadata.writeLong(exceptions[i]);
        }
        return valueCount;
    }

    private static int encodeAlpRdOrSkip(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        long orAll = 0;
        final long firstRaw = NumericUtils.sortableDoubleBits(values[0]);
        for (int i = 1; i < valueCount; i++) {
            final long raw = NumericUtils.sortableDoubleBits(values[i]);
            orAll |= (raw ^ firstRaw);
        }

        final int prefixLength = Long.numberOfLeadingZeros(orAll);
        if (prefixLength < MIN_PREFIX_LENGTH) {
            return valueCount;
        }

        final long prefix = firstRaw & (-1L << (64 - prefixLength));
        final int rightBits = 64 - prefixLength;
        final long tailMask = (rightBits == 0) ? 0L : (1L << rightBits) - 1;

        final var metadata = context.metadata();
        // NOTE: prefixLength is not written — the decoder reconstructs values via
        // (prefix | tail), and prefix already has zeros in the tail bit positions.
        metadata.writeByte(MODE_ALP_RD);
        metadata.writeLong(prefix);

        for (int i = 0; i < valueCount; i++) {
            final long raw = NumericUtils.sortableDoubleBits(values[i]);
            values[i] = raw & tailMask;
        }

        return valueCount;
    }

    public static int encodeStatic(
        final AlpRdDoubleTransformEncodeStage stage,
        final long[] values,
        int valueCount,
        final EncodingContext context
    ) throws IOException {
        return stage.encode(values, valueCount, context);
    }

    @Override
    public boolean equals(Object o) {
        return this == o
            || (o instanceof AlpRdDoubleTransformEncodeStage that
                && maxExponent == that.maxExponent
                && Double.compare(quantizeStep, that.quantizeStep) == 0);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxExponent, quantizeStep);
    }

    @Override
    public String toString() {
        return "AlpRdDoubleTransformEncodeStage{maxExponent=" + maxExponent + ", quantizeStep=" + quantizeStep + "}";
    }
}
