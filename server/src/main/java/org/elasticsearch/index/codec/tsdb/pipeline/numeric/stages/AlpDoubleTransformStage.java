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
 * <p>Applied when the block can be losslessly encoded as integer mantissas using a
 * shared exponent pair {@code (e, f)} such that {@code encoded = round(v * 10^e * 10^-f)}
 * and {@code v == encoded * 10^f * 10^-e} bit-for-bit. Blocks where ALP yields no
 * bit-width reduction, or where the surviving exception fraction exceeds the dynamic
 * threshold {@link AlpDoubleUtils#maxExceptionPercent}, are skipped so the downstream
 * pipeline sees the original sortable-longs unchanged.
 *
 * <h2>Cross-block (e, f) cache</h2>
 * <p>The per-block {@code (e, f)} search is the dominant cost of ALP (see
 * {@link AlpDoubleUtils} for the algorithmic detail and the paper reference). This
 * stage owns the cross-block policy that keeps the search out of the steady-state path:
 * <ul>
 *   <li>The {@code (e, f)} that won the previous block is remembered in
 *     {@link #cachedE}/{@link #cachedF} and validated against the new block with a
 *     single {@link AlpDoubleUtils#countExceptions} pass. When the exception count
 *     stays below {@link AlpDoubleUtils#CACHE_VALIDATION_THRESHOLD} percent the search
 *     is skipped entirely; this is the common path for TSDB metrics where adjacent
 *     blocks share precision.
 *   <li>On cache miss the stage falls through to
 *     {@link AlpDoubleUtils#findBestEFDoubleTopK}, accepts its result, and updates the
 *     cache with the winner.
 * </ul>
 *
 * <p>The encoder block size therefore directly affects how often the search runs per
 * field. Larger blocks amortize the per-block work over more values and shift the
 * steady-state cost toward the single {@code O(N)} cache-validation pass; smaller
 * blocks pay the search more frequently.
 *
 * <h2>Skip policy</h2>
 * <p>The stage applies only when both conditions hold: {@link
 * AlpDoubleUtils#computeBitSavings} reports a strictly positive bit-width reduction,
 * and the exception fraction stays below the bit-saving-dependent budget returned by
 * {@link AlpDoubleUtils#maxExceptionPercent}. When either fails the encode returns
 * without touching {@link EncodingContext#metadata()}, so the per-block bitmap leaves
 * the stage marked inactive and downstream stages see the unmodified sortable-longs.
 *
 * <h2>Example</h2>
 * <p>Block {@code [22.5, 22.7, 22.6, ...]} (sensor-like decimals) is encoded with
 * {@code e=1, f=0}, yielding integer mantissas {@code [225, 227, 226, ...]} that
 * downstream stages ({@code offset}, {@code gcd}, {@code bitPack}) can compress aggressively.
 *
 * <h2>Metadata layout</h2>
 * <p>Written to the stage metadata section:
 * <pre>
 *   +----------+----------+--------------------+
 *   | byte(e)  | byte(f)  | VInt(excCount)     |
 *   +----------+----------+--------------------+
 *   for each exception:
 *     +-----------------+-----------------------------+
 *     | VInt(position)  | Long(originalSortableLong)  |
 *     +-----------------+-----------------------------+
 * </pre>
 *
 * <h2>Exception trick</h2>
 * <p>Values for which the round-trip check fails are stored verbatim in metadata. The
 * mantissa slot at the failing position is overwritten with the previous slot (or zero at
 * position zero) so that the integer mantissa stream stays low-width and the downstream
 * bit-pack does not have to absorb an outlier.
 *
 * <h2>Thread safety</h2>
 * <p>Not thread-safe: scratch buffers (candidate pool, exception arrays, cached
 * {@code (e, f)}) are allocated once in the constructor and reused across blocks. Each
 * pipeline must own its own instance.
 */
public final class AlpDoubleTransformStage implements NumericCodecStage {

    private final int maxExponent;
    private final int[] efOut = new int[2];
    private final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] excPositions;
    private final long[] excValues;
    private int cachedE = -1;
    private int cachedF = -1;
    // Dynamic threshold (absolute exception count) from the last successful encode,
    // reused on cache hit so we skip recomputing computeBitSavings every block.
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
        this.maxExponent = AlpDoubleUtils.MAX_EXPONENT;
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

        if (baselineAlreadyNearOptimal(values, valueCount)) {
            return;
        }

        // Cache-hit fast path: count exceptions against the cached (e, f) and accept when
        // the count fits the dynamic threshold cached from the last successful encode.
        // Skips computeBitSavings on cache hit so the encode runs as count + transform
        // (two passes) instead of count + bits-savings + transform (three).
        if (cachedE >= 0) {
            final int bestExceptions = AlpDoubleUtils.countExceptions(values, valueCount, cachedE, cachedF);
            final int cacheMaxAllowed = (valueCount * AlpDoubleUtils.CACHE_VALIDATION_THRESHOLD) / 100;
            if (bestExceptions <= cacheMaxAllowed && bestExceptions <= cachedMaxAllowed) {
                writeAlpBlock(values, valueCount, cachedE, cachedF, context);
                return;
            }
        }

        // Cache miss: search for a new (e, f), validate against the dynamic threshold,
        // and refresh the cache before writing.
        final int bestExceptions = AlpDoubleUtils.findBestEFDoubleTopK(values, valueCount, maxExponent, efOut, candCounts);
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
        for (int i = 0; i < valueCount; i++) {
            final long bits = Double.doubleToRawLongBits(values[i] * decodeMul);
            // Inlines NumericUtils.doubleToSortableLong: the (bits >> 63) >>> 1 idiom
            // produces 0x7FFF...F when bits is negative and 0 otherwise, so the XOR
            // flips the lower 63 bits exactly when the sign bit is set.
            values[i] = bits ^ ((bits >> 63) >>> 1);
        }

        for (int i = 0; i < excCount; i++) {
            values[metadata.readVInt()] = metadata.readLong();
        }
    }

    /**
     * Returns {@code true} when the sortable-long deltas have a non-zero base stride and
     * a spread no larger than {@link AlpDoubleUtils#DELTA_SPREAD_THRESHOLD}. In
     * sortable-long space this characterises monotonic doubles that stay inside a single
     * IEEE 754 exponent (e.g., a slow drift gauge in a narrow value range); IEEE 754
     * division can perturb the stride by a few ULPs, hence the bounded tolerance rather
     * than exact equality. For these blocks the downstream baseline already compresses
     * to roughly one bit per value, so ALP can only add exception overhead and the stage
     * opts out.
     *
     * <p>Constant blocks (stride zero) are deliberately excluded; ALP handles them in
     * seven bytes versus the baseline's twelve.
     */
    private static boolean baselineAlreadyNearOptimal(final long[] values, int valueCount) {
        if (valueCount < 3) {
            return false;
        }
        final long firstStride = values[1] - values[0];
        if (firstStride == 0) {
            return false;
        }
        long min = firstStride;
        long max = firstStride;
        for (int i = 2; i < valueCount; i++) {
            final long stride = values[i] - values[i - 1];
            min = Math.min(min, stride);
            max = Math.max(max, stride);
        }
        final long spread = max - min;
        if (spread < 0) {
            return false;
        }
        return spread <= AlpDoubleUtils.DELTA_SPREAD_THRESHOLD;
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
