/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class AlpDoubleEncodeStage implements PayloadEncoder {

    static final byte MODE_RAW = 0x00;
    static final byte MODE_ALP = 0x01;

    private final int maxExponent;
    private final double quantizeStep;
    private final int blockSize;
    private final DocValuesForUtil forUtil;
    private final int[] efOut = new int[2];
    private final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] positions;
    private final long[] exceptions;
    private int cachedAlpE = -1;
    private int cachedAlpF = -1;

    public AlpDoubleEncodeStage(int blockSize) {
        this.maxExponent = AlpDoubleUtils.MAX_EXPONENT;
        this.quantizeStep = 0.0;
        this.blockSize = blockSize;
        this.forUtil = new DocValuesForUtil(blockSize);
        this.positions = new int[blockSize];
        this.exceptions = new long[blockSize];
    }

    public AlpDoubleEncodeStage(int blockSize, double maxError) {
        assert maxError > 0 : "maxError must be positive: " + maxError;
        this.maxExponent = Math.min((int) Math.ceil(-Math.log10(maxError)), AlpDoubleUtils.MAX_EXPONENT);
        this.quantizeStep = 2.0 * maxError;
        this.blockSize = blockSize;
        this.forUtil = new DocValuesForUtil(blockSize);
        this.positions = new int[blockSize];
        this.exceptions = new long[blockSize];
    }

    @Override
    public byte id() {
        return StageId.ALP_DOUBLE.id;
    }

    // NOTE: Payload layout (mode dispatch):
    // MODE_RAW (0x00): [mode: byte] [bitsPerValue: VInt] [packed data via DocValuesForUtil].
    // MODE_ALP (0x01): [mode: byte] [e: byte] [f: byte] [excCount: VInt]
    // [bitsPerValue: VInt] [packed encoded longs] then [excCount × (position: VInt, value: Long)].
    // Exceptions are appended after packed data so the bulk bit-unpack can run
    // first without knowing exception positions upfront.
    @Override
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
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
            bestExceptions = AlpDoubleUtils.findBestEFDoubleTopK(values, valueCount, maxExponent, efOut, candE, candF, candCount);
            bestE = efOut[0];
            bestF = efOut[1];
            cachedAlpE = bestE;
            cachedAlpF = bestF;
        }

        final int bitsSaved = AlpDoubleUtils.computeBitSavings(values, valueCount, bestE, bestF);
        if (bitsSaved <= 0) {
            out.writeByte(MODE_RAW);
            encodeRaw(values, valueCount, out);
            return;
        }
        final int maxAllowed = (valueCount * AlpDoubleUtils.maxExceptionPercent(bitsSaved, AlpDoubleUtils.DOUBLE_EXCEPTION_COST)) / 100;
        if (bestExceptions > maxAllowed) {
            out.writeByte(MODE_RAW);
            encodeRaw(values, valueCount, out);
            return;
        }

        final int excCount = AlpDoubleUtils.alpTransformBlock(values, valueCount, bestE, bestF, positions, exceptions);

        out.writeByte(MODE_ALP);
        out.writeByte((byte) bestE);
        out.writeByte((byte) bestF);
        out.writeVInt(excCount);

        if (valueCount < blockSize) {
            Arrays.fill(values, valueCount, blockSize, 0L);
        }
        final int bitsPerValue = computeBitsPerValue(values, blockSize);
        out.writeVInt(bitsPerValue);
        if (bitsPerValue > 0) {
            forUtil.encode(values, bitsPerValue, out);
        }

        for (int i = 0; i < excCount; i++) {
            out.writeVInt(positions[i]);
            out.writeLong(exceptions[i]);
        }
    }

    private void encodeRaw(final long[] values, int valueCount, final DataOutput out) throws IOException {
        if (valueCount < blockSize) {
            Arrays.fill(values, valueCount, blockSize, 0L);
        }
        final int bitsPerValue = computeBitsPerValue(values, blockSize);
        out.writeVInt(bitsPerValue);
        if (bitsPerValue > 0) {
            forUtil.encode(values, bitsPerValue, out);
        }
    }

    private int computeBitsPerValue(final long[] values, int count) {
        long or = 0;
        for (int i = 0; i < count; i++) {
            or |= values[i];
        }
        return or == 0 ? 0 : DocValuesForUtil.roundBits(PackedInts.unsignedBitsRequired(or));
    }

    @Override
    public boolean equals(Object o) {
        return this == o
            || (o instanceof AlpDoubleEncodeStage that
                && maxExponent == that.maxExponent
                && Double.compare(quantizeStep, that.quantizeStep) == 0
                && blockSize == that.blockSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxExponent, quantizeStep, blockSize);
    }

    @Override
    public String toString() {
        return "AlpDoubleEncodeStage{maxExponent=" + maxExponent + ", quantizeStep=" + quantizeStep + ", blockSize=" + blockSize + "}";
    }
}
