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
 * <h2>Effectiveness</h2>
 * <p>Applied when the block can be losslessly encoded as integer mantissas using a shared
 * exponent pair {@code (e, f)} such that {@code encoded = round(v * 10^e * 10^-f)} and
 * {@code v == encoded * 10^f * 10^-e} bit-for-bit. Values whose round trip fails become
 * exceptions stored verbatim in metadata; the mantissa slot is filled with the previous
 * value (or zero at position 0) so the downstream bit-pack does not absorb an outlier.
 *
 * <p>The block passes through unchanged (ALP is skipped) under any of:
 * <ul>
 *   <li>the cache fast path's speculative transform observes near-constant-stride.
 *       The block's doubles are not literally constant but they advance in
 *       near-uniform small increments, so the integer pipeline
 *       {@code delta > offset > gcd > bitPack} reduces the residuals to a tight
 *       range that fits in 5 bits per value or fewer (see
 *       {@link AlpDoubleUtils#DELTA_SPREAD_THRESHOLD}), which is at or below
 *       ALP's bit-width floor. The cache entry is invalidated on this branch so
 *       subsequent near-constant blocks short-circuit on the cheap pre-scan;
 *   <li>no cache entry exists (first block, or invalidated above) and
 *       {@link AlpDoubleUtils#hasNearConstantStride} returns true on the dedicated
 *       pre-scan, for the same reason;
 *   <li>{@link AlpDoubleUtils#findBestEFForBlock} produces an {@code (e, f)} with no
 *       positive bit-width reduction ({@code bitsSaved <= 0});
 *   <li>{@link AlpDoubleUtils#findBestEFForBlock} produces an exception count that
 *       exceeds the budget allowed for the bit-width savings
 *       ({@code bestExceptions > maxAllowed}).
 * </ul>
 *
 * <p>The dominant per-block cost is the {@code (e, f)} search. The stage caches the
 * previous block's winner and revalidates by running a speculative transform whose
 * exception count must clear both a 5% freshness threshold and the cached dynamic
 * threshold; the same pass also produces the near-constant-stride observation, so the
 * dedicated pre-scan runs only on cache miss. On cache miss the search runs via
 * {@link AlpDoubleUtils#findBestEFForBlock}.
 *
 * <h2>Example</h2>
 * <p>A sensor block {@code [22.5, 22.7, 22.6, ...]} encodes with {@code e=1, f=0} into
 * integer mantissas {@code [225, 227, 226, ...]} that downstream
 * {@code offset > gcd > bitPack} compress aggressively.
 *
 * <h2>Metadata layout</h2>
 * <p>Written to the stage metadata section (see {@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat}):
 * <pre>
 *   +----------+----------+-----------------+--------------------------------------------+
 *   | byte(e)  | byte(f)  | VInt(excCount)  | excCount * (VInt(pos), Long(sortableLong)) |
 *   | 1 byte   | 1 byte   | 1-5 bytes       | 9-13 bytes per exception                   |
 *   +----------+----------+-----------------+--------------------------------------------+
 * </pre>
 * <p>Exception positions are stored as absolute VInts (1-2 bytes typical for block sizes
 * up to 16K); exception values are 8-byte sortable longs. The trailing section is empty
 * when the chosen {@code (e, f)} round-trips every value.
 *
 * <h2>Wire format note</h2>
 * <p>{@link StageId#ALP_DOUBLE_STAGE} = {@code 0x05} is permanent from this point on;
 * once a segment carries the byte the layout above is fixed. Delta-encoding consecutive
 * positions could save metadata on blocks with clustered exceptions (e.g. a sensor pegged
 * to NaN for a run) but adds encode/decode work on every block and only pays off above a
 * data-dependent exception-count threshold; worth evaluating with a dedicated
 * burst-exception storage row before changing the format.
 *
 * <h2>Thread safety</h2>
 * <p>Not thread-safe: scratch state is allocated once in the constructor and reused
 * across blocks. Each pipeline must own its own instance.
 */
public final class AlpDoubleTransformStage implements NumericCodecStage {

    private final int[] efOut = new int[2];
    private final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] excPositions;
    private final long[] excValues;
    private final long[] sortableScratch;
    private final boolean[] nearConstStrideOut = new boolean[1];
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
        this.sortableScratch = new long[blockSize];
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

        if (cachedE >= 0) {
            System.arraycopy(values, 0, sortableScratch, 0, valueCount);
            nearConstStrideOut[0] = false;
            final int excCount = AlpDoubleUtils.alpTransformBlock(
                values,
                valueCount,
                cachedE,
                cachedF,
                excPositions,
                excValues,
                nearConstStrideOut
            );
            if (nearConstStrideOut[0]) {
                System.arraycopy(sortableScratch, 0, values, 0, valueCount);
                cachedE = -1;
                cachedF = -1;
                cachedMaxAllowed = -1;
                return;
            }
            final int cacheMaxAllowed = (valueCount * AlpDoubleUtils.CACHE_VALIDATION_THRESHOLD) / 100;
            if (excCount <= cacheMaxAllowed && excCount <= cachedMaxAllowed) {
                writeAlpMetadata(excCount, cachedE, cachedF, context);
                return;
            }
            System.arraycopy(sortableScratch, 0, values, 0, valueCount);
        }

        if (AlpDoubleUtils.hasNearConstantStride(values, valueCount)) {
            return;
        }

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
        final int excCount = AlpDoubleUtils.alpTransformBlock(values, valueCount, e, f, excPositions, excValues, null);
        writeAlpMetadata(excCount, e, f, context);
    }

    private void writeAlpMetadata(final int excCount, final int e, final int f, final EncodingContext context) {
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
