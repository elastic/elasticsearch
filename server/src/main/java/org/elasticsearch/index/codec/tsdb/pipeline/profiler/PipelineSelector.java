/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver.OptimizeFor;

public final class PipelineSelector {

    // NOTE: at 4 bits, second-order deltas for a 512-value block cost ~256 bytes (0.5 bytes/value).
    // Above this, the delta-delta transform no longer compresses well enough to justify its overhead
    // (extra metadata + patchedPFor) compared to plain delta encoding.
    private static final int DELTA_DELTA_MAX_BITS_THRESHOLD = 4;

    // NOTE: must match OffsetCodecStage.DEFAULT_MIN_OFFSET_RATIO_PERCENT so estimatePostOffsetBpv
    // agrees with whether the offset stage will actually fire. If these diverge, the RLE cost
    // model may over- or under-estimate post-offset bit-width.
    private static final int DEFAULT_MIN_OFFSET_RATIO_PERCENT = 25;

    // TODO: PatchedPForEncodeStage.maxExceptionPercent (default 10%) is not modeled here.
    // The selector picks patchedPFor without knowing the block's exception distribution. If
    // exceptions exceed the limit, the stage no-ops and outliers inflate bit-width — a graceful
    // degradation but not optimal. Adding exception profiling to BlockProfiler would close this gap.

    public static final PipelineSelector INSTANCE = new PipelineSelector();

    private final int minOffsetRatioPercent;

    public PipelineSelector() {
        this(DEFAULT_MIN_OFFSET_RATIO_PERCENT);
    }

    public PipelineSelector(int minOffsetRatioPercent) {
        if (minOffsetRatioPercent < 1 || minOffsetRatioPercent > 99) {
            throw new IllegalArgumentException("minOffsetRatioPercent must be in [1, 99]: " + minOffsetRatioPercent);
        }
        this.minOffsetRatioPercent = minOffsetRatioPercent;
    }

    public PipelineConfig select(
        final BlockProfile profile,
        int blockSize,
        final PipelineConfig.DataType dataType,
        @Nullable OptimizeFor hint
    ) {
        return switch (dataType) {
            case LONG -> selectLong(profile, blockSize);
            case DOUBLE -> selectDouble(profile, blockSize, hint);
            case FLOAT -> selectFloat(profile, blockSize, hint);
        };
    }

    private PipelineConfig selectLong(final BlockProfile profile, int blockSize) {
        if (profile.range() == 0) {
            return PipelineConfig.forLongs(blockSize).offset().rle().bitPack();
        }
        if (isRleProfitable(profile)) {
            return PipelineConfig.forLongs(blockSize).offset().rle().bitPack();
        }
        if (profile.isMonotonicallyIncreasing() || profile.isMonotonicallyDecreasing()) {
            if (profile.deltaDeltaMaxBits() <= DELTA_DELTA_MAX_BITS_THRESHOLD) {
                // NOTE: nearly linear (e.g. steady-rate timestamps), second-order deltas are very compact
                return PipelineConfig.forLongs(blockSize).deltaDelta().offset().gcd().patchedPFor().bitPack();
            }
            // NOTE: monotonic but irregular steps, plain delta removes the trend
            return PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        }
        if (profile.shiftedGcd() > 1) {
            return PipelineConfig.forLongs(blockSize).offset().gcd().bitPack();
        }
        if (profile.xorMaxBits() < profile.rawMaxBits()) {
            // NOTE: smooth longs — delta captures the slow drift
            return PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        }
        return PipelineConfig.forLongs(blockSize).offset().bitPack();
    }

    private PipelineConfig selectDouble(final BlockProfile profile, int blockSize, @Nullable OptimizeFor hint) {
        if (profile.range() == 0) {
            return PipelineConfig.forDoubles(blockSize).offset().rle().bitPack();
        }
        if (isRleProfitable(profile)) {
            return PipelineConfig.forDoubles(blockSize).offset().rle().bitPack();
        }
        if (profile.isMonotonicallyIncreasing() || profile.isMonotonicallyDecreasing()) {
            // NOTE: monotonic double counters — Gorilla's XOR encoding between consecutive
            // sortable longs captures the steady increment pattern efficiently
            return PipelineConfig.forDoubles(blockSize).gorilla();
        }
        if (profile.shiftedGcd() > 1) {
            return PipelineConfig.forDoubles(blockSize).offset().gcd().bitPack();
        }
        if (profile.xorMaxBits() < profile.rawMaxBits()) {
            // NOTE: smooth double gauges — ALP exploits decimal structure; lossy 6-digit
            // quantization when optimizing for storage, lossless otherwise.
            // XOR + patchedPFor is faster but less compact.
            if (hint == OptimizeFor.SPEED) {
                return PipelineConfig.forDoubles(blockSize).xor().patchedPFor().bitPack();
            }
            return hint == OptimizeFor.STORAGE
                ? PipelineConfig.forDoubles(blockSize).alpDoubleStage(1e-6).offset().gcd().bitPack()
                : PipelineConfig.forDoubles(blockSize).alpDoubleStage().offset().gcd().bitPack();
        }
        // NOTE: noisy doubles — XOR provides no bit-width reduction, use offset + patchedPFor
        return PipelineConfig.forDoubles(blockSize).offset().patchedPFor().bitPack();
    }

    private PipelineConfig selectFloat(final BlockProfile profile, int blockSize, @Nullable OptimizeFor hint) {
        if (profile.range() == 0) {
            return PipelineConfig.forFloats(blockSize).offset().rle().bitPack();
        }
        if (isRleProfitable(profile)) {
            return PipelineConfig.forFloats(blockSize).offset().rle().bitPack();
        }
        if (profile.isMonotonicallyIncreasing() || profile.isMonotonicallyDecreasing()) {
            // NOTE: monotonic float counters — Gorilla's XOR encoding handles non-uniform
            // sortable-int deltas (e.g. power-of-2 boundary crossings) better than deltaDelta
            return PipelineConfig.forFloats(blockSize).gorilla();
        }
        if (profile.shiftedGcd() > 1) {
            return PipelineConfig.forFloats(blockSize).offset().gcd().bitPack();
        }
        if (profile.xorMaxBits() < profile.rawMaxBits()) {
            // NOTE: smooth floats — ALP exploits decimal structure for best compression,
            // XOR + patchedPFor is faster but less compact
            return hint == OptimizeFor.SPEED
                ? PipelineConfig.forFloats(blockSize).xor().patchedPFor().bitPack()
                : PipelineConfig.forFloats(blockSize).alpFloatStage().offset().gcd().bitPack();
        }
        // NOTE: noisy floats — XOR provides no bit-width reduction, use offset + patchedPFor
        return PipelineConfig.forFloats(blockSize).offset().patchedPFor().bitPack();
    }

    // NOTE: estimates whether RLE saves more bytes than its metadata costs, accounting for
    // both block size and bit-width. The offset stage runs before RLE but may skip when min
    // is zero, too small relative to max, or on overflow — so we estimate post-offset bits
    // conservatively. RLE metadata is: runCount VInt + valueCount VInt + one VInt per run
    // length (~1 byte each for typical run lengths < 128).
    private boolean isRleProfitable(final BlockProfile profile) {
        final int runCount = profile.runCount();
        final int valueCount = profile.valueCount();
        final int estimatedBpv = estimatePostOffsetBpv(profile);
        final int estimatedSavings = (valueCount - runCount) * estimatedBpv / 8;
        final int estimatedCost = 2 + runCount;
        return estimatedSavings > estimatedCost;
    }

    // NOTE: estimates bits per value after the offset stage. Offset subtracts min from all
    // values, reducing max to range = max - min. But it skips when: min == 0 (no shift needed),
    // min is small relative to max (below minOffsetRatioPercent), or max - min overflows.
    // When offset skips, values retain their original bit-width.
    private int estimatePostOffsetBpv(final BlockProfile profile) {
        final long range = profile.range();
        final long min = profile.min();
        final long max = profile.max();
        if (range < 0) {
            // NOTE: overflow (max - min wraps), offset will skip
            return profile.rawMaxBits();
        }
        if (min == 0) {
            // NOTE: offset skips when min is zero — no shift to apply
            return profile.rawMaxBits();
        }
        if (min > 0 && min < computeOffsetThreshold(max)) {
            // NOTE: offset skips when min is too small relative to max
            return profile.rawMaxBits();
        }
        return range > 0 ? (64 - Long.numberOfLeadingZeros(range)) : 0;
    }

    // NOTE: mirrors OffsetCodecStage.computeThreshold — must stay in sync.
    private long computeOffsetThreshold(long max) {
        if (minOffsetRatioPercent == DEFAULT_MIN_OFFSET_RATIO_PERCENT) {
            return max >>> 2;
        }
        if (max > Long.MAX_VALUE / minOffsetRatioPercent) {
            return (max / 100) * minOffsetRatioPercent;
        }
        return (max * minOffsetRatioPercent) / 100;
    }
}
