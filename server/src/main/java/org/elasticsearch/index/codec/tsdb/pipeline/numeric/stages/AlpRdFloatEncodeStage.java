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
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class AlpRdFloatEncodeStage implements PayloadEncoder {

    static final byte MODE_RAW = 0x00;
    static final byte MODE_DECIMAL = 0x01;
    static final byte MODE_ALP_RD = 0x02;

    static final int MIN_PREFIX_LENGTH = 2;

    private final int maxExponent;
    private final float quantizeStep;
    private final int blockSize;
    private final DocValuesForUtil forUtil;
    private final int[] efOut = new int[2];
    private final int[] candE = new int[AlpFloatUtils.CAND_POOL_SIZE];
    private final int[] candF = new int[AlpFloatUtils.CAND_POOL_SIZE];
    private final int[] candCount = new int[AlpFloatUtils.CAND_POOL_SIZE];
    private final int[] positions;
    private final int[] exceptions;
    private int cachedAlpE = -1;
    private int cachedAlpF = -1;

    public AlpRdFloatEncodeStage(int blockSize) {
        this.maxExponent = AlpFloatUtils.MAX_EXPONENT;
        this.quantizeStep = 0.0f;
        this.blockSize = blockSize;
        this.forUtil = new DocValuesForUtil(blockSize);
        this.positions = new int[blockSize];
        this.exceptions = new int[blockSize];
    }

    // NOTE: Derives maxExponent = ceil(-log10(maxError)) and fuses quantization
    // (step = 2 * maxError) into this stage, eliminating a separate quantize pass.
    public AlpRdFloatEncodeStage(int blockSize, double maxError) {
        assert maxError > 0 : "maxError must be positive: " + maxError;
        this.maxExponent = Math.min((int) Math.ceil(-Math.log10(maxError)), AlpFloatUtils.MAX_EXPONENT);
        this.quantizeStep = (float) (2.0 * maxError);
        this.blockSize = blockSize;
        this.forUtil = new DocValuesForUtil(blockSize);
        this.positions = new int[blockSize];
        this.exceptions = new int[blockSize];
    }

    @Override
    public byte id() {
        return StageId.ALP_RD_FLOAT.id;
    }

    // NOTE: Payload layout (mode dispatch):
    // MODE_RAW (0x00): [mode: byte] [bitsPerValue: VInt] [packed data via DocValuesForUtil].
    // MODE_DECIMAL (0x01): [mode: byte] [e: byte] [f: byte] [excCount: VInt]
    // [bitsPerValue: VInt] [packed encoded longs] then [excCount × (position: VInt, value: Int)].
    // MODE_ALP_RD (0x02): [mode: byte] [prefix: Int]
    // [bitsPerValue: VInt] [packed tail bits via DocValuesForUtil].
    // Same as AlpRdDoubleEncodeStage but with 32-bit prefixes and exception values.
    @Override
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
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
                encodeDecimal(values, valueCount, out, bestE, bestF, context);
                return;
            }
        }

        encodeAlpRdOrRaw(values, valueCount, out);
    }

    private void encodeDecimal(
        final long[] values,
        int valueCount,
        final DataOutput out,
        int bestE,
        int bestF,
        final EncodingContext context
    ) throws IOException {
        final int excCount = AlpFloatUtils.alpTransformBlock(values, valueCount, bestE, bestF, positions, exceptions);

        out.writeByte(MODE_DECIMAL);
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
            out.writeInt(exceptions[i]);
        }
    }

    private void encodeAlpRdOrRaw(final long[] values, int valueCount, final DataOutput out) throws IOException {
        int orAll = 0;
        final int firstRaw = NumericUtils.sortableFloatBits((int) values[0]);
        for (int i = 1; i < valueCount; i++) {
            final int raw = NumericUtils.sortableFloatBits((int) values[i]);
            orAll |= (raw ^ firstRaw);
        }

        final int prefixLength = Integer.numberOfLeadingZeros(orAll);
        if (prefixLength < MIN_PREFIX_LENGTH) {
            out.writeByte(MODE_RAW);
            encodeRaw(values, valueCount, out);
            return;
        }

        final int prefix = firstRaw & (-1 << (32 - prefixLength));
        final int rightBits = 32 - prefixLength;
        final int tailMask = (rightBits == 0) ? 0 : (1 << rightBits) - 1;

        // NOTE: prefixLength is not written — the decoder reconstructs values via
        // (prefix | tail), and prefix already has zeros in the tail bit positions.
        out.writeByte(MODE_ALP_RD);
        out.writeInt(prefix);

        for (int i = 0; i < valueCount; i++) {
            final int raw = NumericUtils.sortableFloatBits((int) values[i]);
            values[i] = raw & tailMask;
        }

        if (valueCount < blockSize) {
            Arrays.fill(values, valueCount, blockSize, 0L);
        }
        final int bitsPerValue = computeBitsPerValue(values, blockSize);
        out.writeVInt(bitsPerValue);
        if (bitsPerValue > 0) {
            forUtil.encode(values, bitsPerValue, out);
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
            || (o instanceof AlpRdFloatEncodeStage that
                && maxExponent == that.maxExponent
                && Float.compare(quantizeStep, that.quantizeStep) == 0
                && blockSize == that.blockSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxExponent, quantizeStep, blockSize);
    }

    @Override
    public String toString() {
        return "AlpRdFloatEncodeStage{maxExponent=" + maxExponent + ", quantizeStep=" + quantizeStep + ", blockSize=" + blockSize + "}";
    }
}
