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
 * Segmented delta encoding for piecewise-monotonic sequences.
 *
 * <h2>Effectiveness</h2>
 * <p>Applied when a block consists of multiple monotonic sub-runs separated by direction
 * flips. For TSDB indices sorted by {@code [_tsid asc, @timestamp desc]} this captures
 * blocks that cross one or more {@code _tsid} boundaries: timestamps decrease within a
 * {@code _tsid} run and jump up at the boundary. {@link DeltaCodecStage} declines such
 * blocks because it requires a single direction throughout the entire block, leaving the
 * downstream stages to bit-pack the raw range at {@code log2(time_range)} bits per value.
 *
 * <p>This stage splits the block at every direction flip and delta-encodes each sub-run
 * independently. The block is accepted when it contains between {@code 1} and
 * {@link #kMax()} flips inclusive and every resulting sub-run has at least two values.
 * Blocks with zero flips (fully monotonic) are left to {@link DeltaCodecStage}. Blocks
 * with more flips than the cap, or with any length-one sub-run, fall through to the rest
 * of the pipeline.
 *
 * <h2>Example</h2>
 * <p>The boundary block {@code [10, 9, 8, 7, 200, 199, 198]} has one flip at index four.
 * The two sub-runs delta-encode to {@code [-1, -1, -1, -1]} and {@code [-1, -1, -1]}, with
 * the anchor recovery values stored in metadata. The bit-packed payload is uniform in
 * magnitude regardless of the original time range.
 *
 * <h2>Metadata layout</h2>
 * <p>Written to the stage metadata section (see {@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat}):
 * <pre>
 *   +-------------------+----------------------------+-------------------------------+
 *   | VInt(k)           | VInt(splitPosition) * k    | ZLong(firstDelta) * (k+1)     |
 *   +-------------------+----------------------------+-------------------------------+
 * </pre>
 * <p>For a single-flip block in a 128-value block this is roughly eight bytes, compared
 * to {@code 128 * log2(time_range) / 8} bytes (typically 300+ bytes for millisecond
 * precision timestamps) when falling through to bit-packing the raw values.
 *
 * <h2>Wire format note</h2>
 * <p>{@code kMax} is an encode-time threshold only and is not persisted. The decoder
 * reads the actual {@code k} from per-block metadata. Different encoders may therefore
 * choose different {@code kMax} values without breaking decoders for previously written
 * segments.
 *
 * <h2>Thread safety</h2>
 * <p>Not thread-safe: scratch buffers are allocated once in the constructor and reused
 * across blocks. Each pipeline must own its own instance.
 */
public final class SplitDeltaCodecStage implements NumericCodecStage {

    private final int kMax;
    // NOTE: size kMax+1 so decode can place valueCount in splits[k] as a sentinel,
    // removing the per-sub-run bound check from the inner prefix-sum loop.
    private final int[] splits;
    private final long[] firstDeltas;

    /**
     * Creates a stage that accepts up to {@code kMax} direction flips per block.
     *
     * @param kMax the maximum number of flips accepted per block; must be at least one
     * @throws IllegalArgumentException if {@code kMax} is less than one
     */
    public SplitDeltaCodecStage(int kMax) {
        if (kMax < 1) {
            throw new IllegalArgumentException("kMax must be at least 1, got: " + kMax);
        }
        this.kMax = kMax;
        this.splits = new int[kMax + 1];
        this.firstDeltas = new long[kMax + 1];
    }

    /**
     * Returns a stage with a {@code kMax} derived from the encoder block size.
     *
     * <p>The formula is {@code kMax = clamp(blockSize / 32, 4, 64)}, which scales
     * the flip cap with the block size so accepted sub-runs have roughly 32 values
     * each at the cap. This keeps per sub-run metadata overhead amortized regardless
     * of {@code blockSize}: at the TSDB production {@code blockSize=512} the formula
     * yields {@code kMax=16}, matching the historical default. Larger block sizes
     * unlock proportionally larger flip caps without changing per sub-run economics.
     *
     * @param blockSize the encoder block size in values; must be at least one
     * @return a stage configured with a block size derived {@code kMax}
     * @throws IllegalArgumentException if {@code blockSize} is less than one
     */
    public static SplitDeltaCodecStage forBlockSize(int blockSize) {
        if (blockSize < 1) {
            throw new IllegalArgumentException("blockSize must be at least 1, got: " + blockSize);
        }
        return new SplitDeltaCodecStage(Math.min(64, Math.max(4, blockSize / 32)));
    }

    /**
     * Returns the maximum number of direction flips this stage accepts per block.
     *
     * @return the per-block flip cap
     */
    public int kMax() {
        return kMax;
    }

    @Override
    public byte id() {
        return StageId.SPLIT_DELTA_STAGE.id;
    }

    @Override
    public void encode(final long[] values, final int valueCount, final EncodingContext context) {
        assert valueCount >= 1 : "valueCount must be at least 1";
        if (valueCount < 4) {
            return;
        }

        final int k = countFlips(values, valueCount);
        if (k <= 0) {
            return;
        }
        if (hasShortSubRun(k, valueCount)) {
            return;
        }

        int lo = 0;
        for (int j = 0; j < k; j++) {
            deltaEncodeSubRun(values, lo, splits[j], j);
            lo = splits[j];
        }
        deltaEncodeSubRun(values, lo, valueCount, k);

        final MetadataWriter meta = context.metadata();
        meta.writeVInt(k);
        for (int j = 0; j < k; j++) {
            meta.writeVInt(splits[j]);
        }
        for (int j = 0; j <= k; j++) {
            meta.writeZLong(firstDeltas[j]);
        }
    }

    @Override
    public void decode(final long[] values, final int valueCount, final DecodingContext context) throws IOException {
        assert valueCount >= 1 : "valueCount must be at least 1";
        final MetadataReader meta = context.metadata();
        final int k = meta.readVInt();
        for (int j = 0; j < k; j++) {
            splits[j] = meta.readVInt();
        }
        splits[k] = valueCount;
        for (int j = 0; j <= k; j++) {
            firstDeltas[j] = meta.readZLong();
        }

        int lo = 0;
        for (int j = 0; j <= k; j++) {
            final int hi = splits[j];
            long sum = firstDeltas[j];
            // NOTE: 4-wide ILP unroll. Computing the four partial prefix sums (v0, v0+v1,
            // v0+v1+v2, v0+v1+v2+v3) as a balanced tree cuts the dependency chain from
            // four serial adds to two, letting the CPU issue the four writes in parallel.
            final int unrollEnd = lo + ((hi - lo) & ~3);
            int i = lo;
            for (; i < unrollEnd; i += 4) {
                final long v0 = values[i];
                final long v1 = values[i + 1];
                final long v2 = values[i + 2];
                final long v3 = values[i + 3];
                final long s1 = v0 + v1;
                final long t23 = v2 + v3;
                final long s2 = s1 + v2;
                final long s3 = s1 + t23;
                values[i] = sum + v0;
                values[i + 1] = sum + s1;
                values[i + 2] = sum + s2;
                values[i + 3] = sum + s3;
                sum += s3;
            }
            for (; i < hi; i++) {
                sum += values[i];
                values[i] = sum;
            }
            lo = hi;
        }
    }

    public static void encodeStatic(final SplitDeltaCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        stage.encode(values, valueCount, context);
    }

    public static void decodeStatic(final SplitDeltaCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        stage.decode(values, valueCount, context);
    }

    // NOTE: Direction changes are committed lazily. A candidate flip at position p is
    // recorded as pending and resolved on the next non-zero direction:
    // - reverts to the previous primary direction -> boundary jump (single split at p,
    // primary direction unchanged; the anomaly value at p belongs to the next sub-run).
    // - sustains the new direction -> true reversal (split at p, primary direction
    // becomes the new one).
    // This makes the classic TSDB pattern [desc, desc, ..., UP, desc, desc, ...] resolve
    // to a single split rather than two adjacent splits with a length-1 middle sub-run.
    private int countFlips(final long[] values, final int valueCount) {
        int k = 0;
        int prev = 0;
        int pendingFlip = -1;
        int pendingDir = 0;
        for (int i = 1; i < valueCount; i++) {
            final long diff = values[i] - values[i - 1];
            final int cur = Long.signum(diff);
            if (cur == 0) {
                continue;
            }
            if (prev == 0) {
                prev = cur;
                continue;
            }
            if (pendingFlip < 0) {
                if (cur != prev) {
                    pendingFlip = i;
                    pendingDir = cur;
                }
                continue;
            }
            if (k == kMax) {
                return -1;
            }
            splits[k++] = pendingFlip;
            if (cur != prev) {
                prev = pendingDir;
            }
            pendingFlip = -1;
            pendingDir = 0;
        }
        return k;
    }

    private boolean hasShortSubRun(final int k, final int valueCount) {
        int prevEnd = 0;
        for (int j = 0; j < k; j++) {
            if (splits[j] - prevEnd < 2) {
                return true;
            }
            prevEnd = splits[j];
        }
        return valueCount - prevEnd < 2;
    }

    private void deltaEncodeSubRun(final long[] values, int lo, int hi, int j) {
        for (int i = hi - 1; i > lo; i--) {
            values[i] -= values[i - 1];
        }
        firstDeltas[j] = values[lo] - values[lo + 1];
        values[lo] = values[lo + 1];
    }
}
