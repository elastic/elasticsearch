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
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;

public final class PipelineSelector {

    // NOTE: at 4 bits, second-order deltas for a 512-value block cost ~256 bytes (0.5 bytes/value).
    // Above this, the delta-delta transform no longer compresses well enough to justify its overhead
    // (extra metadata + patchedPFor) compared to plain delta encoding.
    private static final int DELTA_DELTA_MAX_BITS_THRESHOLD = 4;

    private static final double QUANTIZE_2_DECIMALS = 1e-2;
    private static final double QUANTIZE_4_DECIMALS = 1e-4;
    private static final double QUANTIZE_6_DECIMALS = 1e-6;

    // NOTE: must match OffsetCodecStage.DEFAULT_MIN_OFFSET_RATIO_PERCENT so estimatePostOffsetBpv
    // agrees with whether the offset stage will actually fire. If these diverge, the RLE cost
    // model may over- or under-estimate post-offset bit-width.
    private static final int DEFAULT_MIN_OFFSET_RATIO_PERCENT = 25;

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
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        return switch (dataType) {
            case LONG -> selectLong(profile, blockSize);
            case DOUBLE -> selectDouble(profile, blockSize, hint, metricType);
            case FLOAT -> selectFloat(profile, blockSize, hint, metricType);
        };
    }

    private PipelineConfig selectLong(final BlockProfile profile, int blockSize) {
        if (isRleProfitable(profile)) {
            return PipelineConfig.forLongs(blockSize).delta().offset().gcd().rle().bitPack();
        }
        if (profile.isMonotonicallyIncreasing() || profile.isMonotonicallyDecreasing()) {
            if (profile.deltaDeltaMaxBits() <= DELTA_DELTA_MAX_BITS_THRESHOLD) {
                // NOTE: nearly linear (e.g. steady-rate timestamps), second-order deltas are very compact
                return PipelineConfig.forLongs(blockSize).deltaDelta().offset().gcd().patchedPFor().bitPack();
            }
            // NOTE: monotonic but irregular steps, plain delta removes the trend
            return PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        }
        // NOTE: wide general-purpose pipeline — each stage has skip logic so the
        // overhead for unused stages is just 1 bitmap bit. This is safe even when
        // the sample block is uninformative (e.g. constant or low-cardinality)
        // because later blocks may have different data shapes.
        return PipelineConfig.forLongs(blockSize).delta().offset().gcd().rle().bitPack();
    }

    private PipelineConfig selectDouble(
        final BlockProfile profile,
        int blockSize,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        if (profile.range() == 0 || isRleProfitable(profile)) {
            return PipelineConfig.forDoubles(blockSize).offset().gcd().rle().bitPack();
        }
        if (metricType == MetricType.COUNTER || profile.xorMaxBits() < profile.rawMaxBits()) {
            return selectXorDouble(blockSize, hint);
        }
        // NOTE: ALP exploits decimal structure in the IEEE domain — works regardless of XOR stats.
        // Quantization is graduated by hint: aggressive for storage, none for speed, mild by default.
        // ALP's internal skip mechanism handles blocks where it doesn't help.
        return selectAlpDouble(blockSize, hint);
    }

    private static PipelineConfig selectAlpDouble(int blockSize, @Nullable OptimizeFor hint) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_2_DECIMALS).offset().gcd().bitPack();
        }
        if (hint == OptimizeFor.BALANCED) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_4_DECIMALS).offset().gcd().bitPack();
        }
        if (hint == OptimizeFor.SPEED) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage().offset().gcd().bitPack();
        }
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_6_DECIMALS).offset().gcd().bitPack();
    }

    private static PipelineConfig selectXorDouble(int blockSize, @Nullable OptimizeFor hint) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forDoubles(blockSize).gorilla();
        }
        if (hint == OptimizeFor.SPEED) {
            return PipelineConfig.forDoubles(blockSize).chimpDoubleStage().offset().gcd().bitPack();
        }
        return PipelineConfig.forDoubles(blockSize).fpcStage().offset().gcd().bitPack();
    }

    private PipelineConfig selectFloat(
        final BlockProfile profile,
        int blockSize,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        if (profile.range() == 0 || isRleProfitable(profile)) {
            return PipelineConfig.forFloats(blockSize).offset().gcd().rle().bitPack();
        }
        if (metricType == MetricType.COUNTER || profile.xorMaxBits() < profile.rawMaxBits()) {
            return selectXorFloat(blockSize, hint);
        }
        // NOTE: ALP exploits decimal structure in the IEEE domain — works regardless of XOR stats.
        // Quantization is graduated by hint: aggressive for storage, none for speed, mild by default.
        // ALP's internal skip mechanism handles blocks where it doesn't help.
        return selectAlpFloat(blockSize, hint);
    }

    private static PipelineConfig selectAlpFloat(int blockSize, @Nullable OptimizeFor hint) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage(QUANTIZE_2_DECIMALS).offset().gcd().bitPack();
        }
        if (hint == OptimizeFor.BALANCED) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage(QUANTIZE_4_DECIMALS).offset().gcd().bitPack();
        }
        if (hint == OptimizeFor.SPEED) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage().offset().gcd().bitPack();
        }
        return PipelineConfig.forFloats(blockSize).alpFloatStage(QUANTIZE_6_DECIMALS).offset().gcd().bitPack();
    }

    private static PipelineConfig selectXorFloat(int blockSize, @Nullable OptimizeFor hint) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forFloats(blockSize).gorilla();
        }
        if (hint == OptimizeFor.SPEED) {
            return PipelineConfig.forFloats(blockSize).chimpFloatStage().offset().gcd().bitPack();
        }
        return PipelineConfig.forFloats(blockSize).fpcStage().offset().gcd().bitPack();
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
