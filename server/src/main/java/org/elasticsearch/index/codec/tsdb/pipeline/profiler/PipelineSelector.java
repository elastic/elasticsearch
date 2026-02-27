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

    private static final double QUANTIZE_2_DECIMALS = 1e-2;
    private static final double QUANTIZE_4_DECIMALS = 1e-4;
    private static final double QUANTIZE_6_DECIMALS = 1e-6;

    private static final int FPC_THRESHOLD_DEFAULT = 16;
    private static final int FPC_THRESHOLD_SPEED = 24;
    private static final double GORILLA_THRESHOLD_DEFAULT = 0.4;
    private static final double GORILLA_THRESHOLD_SPEED = 0.6;

    public static final PipelineSelector INSTANCE = new PipelineSelector();

    public PipelineConfig select(
        final BlockProfile profile,
        int blockSize,
        final PipelineConfig.DataType dataType,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        return switch (dataType) {
            case LONG -> selectLong(blockSize);
            case DOUBLE -> selectDouble(profile, blockSize, hint, metricType);
            case FLOAT -> selectFloat(profile, blockSize, hint, metricType);
        };
    }

    // NOTE: wide general-purpose pipeline — each stage has skip logic so the
    // overhead for unused stages is just 1 bitmap bit. Two chained deltas replace
    // the fused deltaDelta stage: the first fires on monotonic data, the second
    // fires only if resulting deltas are themselves monotonic. For constant-rate
    // timestamps, the second delta skips and offset handles the constant residuals.
    private static PipelineConfig selectLong(int blockSize) {
        return PipelineConfig.forLongs(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
    }

    private static PipelineConfig selectDouble(
        final BlockProfile profile,
        int blockSize,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        // NOTE: constant data has no decimal structure for ALP, no trends for FPC,
        // and no XOR variation for Gorilla/Chimp128. The wide integer pipeline handles
        // it trivially: delta produces zeros, offset stores the base, everything else skips.
        if (profile.range() == 0) {
            return PipelineConfig.forDoubles(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
        }

        // NOTE: counters are monotonic numeric sequences (e.g. bytes_sent, request_count).
        // They never have decimal structure — ALP's (e,f) search would waste time and
        // fall back to raw encoding. XOR-based prediction (especially FPC's DFCM) is
        // the right family because counters have constant or near-constant strides.
        if (metricType == MetricType.COUNTER) {
            return selectXorDouble(profile, blockSize, hint);
        }

        // NOTE: the XOR gate decides whether consecutive-value XOR produces meaningfully
        // smaller residuals than the raw bit-width. A simple "xorMaxBits < rawMaxBits"
        // is too permissive — saving 1-2 bits doesn't justify the overhead of XOR algorithms
        // vs ALP's simpler integer arithmetic path. Requiring at least 25% reduction filters
        // out marginal cases where ALP would do as well or better via decimal structure.
        final long xorReduction = profile.rawMaxBits() - profile.xorMaxBits();
        if (xorReduction >= profile.rawMaxBits() / 4) {
            return selectXorDouble(profile, blockSize, hint);
        }

        // NOTE: ALP exploits decimal structure in the IEEE domain — it finds integer
        // exponent/factor pairs (e,f) that convert doubles to integers with minimal
        // exceptions. This works regardless of XOR statistics and is the right choice
        // when data originates from decimal sources (sensor readings, prices, percentages).
        // Quantization aggressiveness is graduated by the optimize_for hint.
        return selectAlpDouble(blockSize, hint);
    }

    private static PipelineConfig selectAlpDouble(int blockSize, @Nullable OptimizeFor hint) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_2_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
        }
        if (hint == OptimizeFor.BALANCED) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_4_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
        }
        if (hint == OptimizeFor.SPEED) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage().delta().offset().gcd().patchedPFor().bitPack();
        }
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_6_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
    }

    private static PipelineConfig selectXorDouble(final BlockProfile profile, int blockSize, @Nullable OptimizeFor hint) {
        // NOTE: deltaDeltaMaxBits measures the max bit-width of second-order differences
        // (values[i] - 2*values[i-1] + values[i-2]). When small, the stride between
        // consecutive values is nearly constant — exactly the pattern FPC's DFCM predictor
        // learns. DFCM converges by i=3 for constant strides, producing near-zero residuals.
        // When hint is SPEED, widen the threshold — FPC's downstream BitPack uses SIMD bulk
        // operations, making FPC+BitPack faster to decode than Chimp128's sequential decode.
        final int fpcThreshold = (hint == OptimizeFor.SPEED) ? FPC_THRESHOLD_SPEED : FPC_THRESHOLD_DEFAULT;
        if (profile.deltaDeltaMaxBits() <= fpcThreshold) {
            return PipelineConfig.forDoubles(blockSize).fpcStage().delta().offset().gcd().patchedPFor().bitPack();
        }

        // NOTE: xorZeroCount counts consecutive identical values (XOR = 0). Gorilla's
        // Case 0 encodes these in just 1 bit — no other algorithm matches this for
        // repeated values. When hint is SPEED, raise the threshold — Gorilla's sequential
        // bit-stream decode is slower than FPC+BitPack, so we only pick Gorilla when the
        // repeat advantage is overwhelming enough to outweigh the decode speed cost.
        if (profile.valueCount() > 1) {
            final double xorZeroFraction = (double) profile.xorZeroCount() / (profile.valueCount() - 1);
            final double gorillaThreshold = (hint == OptimizeFor.SPEED) ? GORILLA_THRESHOLD_SPEED : GORILLA_THRESHOLD_DEFAULT;
            if (xorZeroFraction >= gorillaThreshold) {
                return PipelineConfig.forDoubles(blockSize).gorilla();
            }
        }

        // NOTE: Chimp128 is the general-purpose XOR fallback. It maintains a ring buffer
        // of the last 128 values and selects the reference that maximizes trailing zeros
        // in the XOR. For periodic data the ring buffer contains exact matches from previous
        // cycles, achieving near-zero encoding cost.
        return PipelineConfig.forDoubles(blockSize).chimp128();
    }

    private static PipelineConfig selectFloat(
        final BlockProfile profile,
        int blockSize,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        if (profile.range() == 0) {
            return PipelineConfig.forFloats(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
        }

        if (metricType == MetricType.COUNTER) {
            return selectXorFloat(profile, blockSize, hint);
        }

        final long xorReduction = profile.rawMaxBits() - profile.xorMaxBits();
        if (xorReduction >= profile.rawMaxBits() / 4) {
            return selectXorFloat(profile, blockSize, hint);
        }

        return selectAlpFloat(blockSize, hint);
    }

    private static PipelineConfig selectAlpFloat(int blockSize, @Nullable OptimizeFor hint) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage(QUANTIZE_2_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
        }
        if (hint == OptimizeFor.BALANCED) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage(QUANTIZE_4_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
        }
        if (hint == OptimizeFor.SPEED) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage().delta().offset().gcd().patchedPFor().bitPack();
        }
        return PipelineConfig.forFloats(blockSize).alpFloatStage(QUANTIZE_6_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
    }

    private static PipelineConfig selectXorFloat(final BlockProfile profile, int blockSize, @Nullable OptimizeFor hint) {
        final int fpcThreshold = (hint == OptimizeFor.SPEED) ? FPC_THRESHOLD_SPEED : FPC_THRESHOLD_DEFAULT;
        if (profile.deltaDeltaMaxBits() <= fpcThreshold) {
            return PipelineConfig.forFloats(blockSize).fpcStage().delta().offset().gcd().patchedPFor().bitPack();
        }

        if (profile.valueCount() > 1) {
            final double xorZeroFraction = (double) profile.xorZeroCount() / (profile.valueCount() - 1);
            final double gorillaThreshold = (hint == OptimizeFor.SPEED) ? GORILLA_THRESHOLD_SPEED : GORILLA_THRESHOLD_DEFAULT;
            if (xorZeroFraction >= gorillaThreshold) {
                return PipelineConfig.forFloats(blockSize).gorilla();
            }
        }

        return PipelineConfig.forFloats(blockSize).chimp128();
    }
}
