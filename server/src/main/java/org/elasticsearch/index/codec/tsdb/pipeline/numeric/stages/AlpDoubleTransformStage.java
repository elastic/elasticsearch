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
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataReader;
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage;

import java.io.IOException;

/**
 * ALP (Adaptive Lossless floating-Point) transform stage for doubles.
 *
 * <p>Applied when the block can be losslessly encoded as integer mantissas using a
 * shared exponent pair {@code (e, f)} such that {@code encoded = round(v * 10^e * 10^-f)}
 * and {@code v == encoded * 10^f * 10^-e} bit-for-bit. Mantissa positions whose round
 * trip fails become exceptions stored verbatim in metadata; the mantissa slot is filled
 * with the previous value (or zero at position 0) so the downstream bit-pack does not
 * absorb an outlier. Blocks with no bit-width reduction or too many exceptions are
 * skipped so the downstream pipeline sees the original sortable-longs unchanged.
 *
 * <p>The dominant per-block cost is the {@code (e, f)} search. This stage holds the
 * cross-block cache that keeps it off the steady-state path: the previous winner is
 * validated against the new block with a single {@link AlpDoubleUtils#countExceptions}
 * pass against both the 5% freshness threshold and the cached dynamic threshold. On
 * cache miss the search runs via {@link AlpDoubleUtils#findBestEFForBlock}.
 *
 * <p>Example: a sensor block {@code [22.5, 22.7, 22.6, ...]} encodes with {@code e=1,
 * f=0} into integer mantissas {@code [225, 227, 226, ...]} that downstream
 * {@code offset > gcd > bitPack} compress aggressively.
 *
 * <p>Metadata layout (stage metadata section):
 * <pre>
 *   byte(e), byte(f), VInt(excCount), excCount * (VInt(position), Long(originalSortableLong))
 * </pre>
 *
 * <p>Not thread-safe: scratch state is allocated once in the constructor and reused
 * across blocks. Each pipeline must own its own instance.
 */
public final class AlpDoubleTransformStage implements NumericCodecStage {

    private final int[] efOut = new int[2];
    private final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] excPositions;
    private final long[] excValues;
    private int cachedE = -1;
    private int cachedF = -1;
    private int cachedMaxAllowed = -1;

    /**
     * Creates a stage with the standard exponent range and scratch buffers sized to the
     * encoder block size.
     *
     * @param blockSize the number of values per block; must be at least one
     * @throws IllegalArgumentException if {@code blockSize} is less than one
     */
    public AlpDoubleTransformStage(int blockSize) {
        if (blockSize < 1) {
            throw new IllegalArgumentException("blockSize must be at least 1, got: " + blockSize);
        }
        this.excPositions = new int[blockSize];
        this.excValues = new long[blockSize];
    }

    @Override
    public byte id() {
        return StageId.ALP_DOUBLE_STAGE.id;
    }

    @Override
    public void encode(final long[] values, final int valueCount, final EncodingContext context) {
        assert valueCount >= 1 : "valueCount must be at least 1";
        assert valueCount <= excPositions.length
            : "valueCount (" + valueCount + ") must not exceed blockSize (" + excPositions.length + ")";

        // Skip ALP on blocks the integer pipeline already compresses to ~1 bit per value.
        if (AlpDoubleUtils.hasNearConstantStride(values, valueCount)) {
            return;
        }

        // Cache hit: validate via countExceptions only, skip computeBitSavings.
        if (cachedE >= 0) {
            final int bestExceptions = AlpDoubleUtils.countExceptions(values, valueCount, cachedE, cachedF);
            final int cacheMaxAllowed = (valueCount * AlpDoubleUtils.CACHE_VALIDATION_THRESHOLD) / 100;
            if (bestExceptions <= cacheMaxAllowed && bestExceptions <= cachedMaxAllowed) {
                writeAlpBlock(values, valueCount, cachedE, cachedF, context);
                return;
            }
        }

        // Cache miss: full top-K search, validate, refresh cache.
        final int bestExceptions = AlpDoubleUtils.findBestEFForBlock(values, valueCount, efOut, candCounts);
        final int bestE = efOut[0];
        final int bestF = efOut[1];

        final int bitsSaved = AlpDoubleUtils.computeBitSavings(values, valueCount, bestE, bestF);
        if (bitsSaved <= 0) {
            return;
        }
        final int maxAllowed = AlpDoubleUtils.maxExceptions(bitsSaved, valueCount, AlpDoubleUtils.DOUBLE_EXCEPTION_COST);
        if (bestExceptions > maxAllowed) {
            return;
        }

        cachedE = bestE;
        cachedF = bestF;
        cachedMaxAllowed = maxAllowed;

        writeAlpBlock(values, valueCount, bestE, bestF, context);
    }

    private void writeAlpBlock(final long[] values, final int valueCount, final int e, final int f, final EncodingContext context) {
        final int excCount = AlpDoubleUtils.alpTransformBlock(values, valueCount, e, f, excPositions, excValues);
        final MetadataWriter metadata = context.metadata();
        metadata.writeByte((byte) e);
        metadata.writeByte((byte) f);
        metadata.writeVInt(excCount);
        for (int i = 0; i < excCount; i++) {
            metadata.writeVInt(excPositions[i]);
            metadata.writeLong(excValues[i]);
        }
    }

    @Override
    public void decode(final long[] values, final int valueCount, final DecodingContext context) throws IOException {
        assert valueCount >= 1 : "valueCount must be at least 1";
        final MetadataReader metadata = context.metadata();
        final int e = metadata.readByte() & 0xFF;
        final int f = metadata.readByte() & 0xFF;
        final int excCount = metadata.readVInt();

        final double decodeMul = AlpDoubleUtils.POWERS_OF_TEN[f] * AlpDoubleUtils.NEG_POWERS_OF_TEN[e];
        // Inlines NumericUtils.doubleToSortableLong: (bits >> 63) >>> 1 is 0x7FFF...F when
        // bits is negative and 0 otherwise, so the XOR flips the lower 63 bits when the
        // sign bit is set, preserving order.
        for (int i = 0; i < valueCount; i++) {
            final long bits = Double.doubleToRawLongBits(values[i] * decodeMul);
            values[i] = bits ^ ((bits >> 63) >>> 1);
        }

        for (int i = 0; i < excCount; i++) {
            values[metadata.readVInt()] = metadata.readLong();
        }
    }

    public static void encodeStatic(final AlpDoubleTransformStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        stage.encode(values, valueCount, context);
    }

    public static void decodeStatic(final AlpDoubleTransformStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        stage.decode(values, valueCount, context);
    }
}
