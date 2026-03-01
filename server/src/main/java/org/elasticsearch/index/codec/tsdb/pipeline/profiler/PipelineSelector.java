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
    private static final double QUANTIZE_6_DECIMALS = 1e-6;

    private static final int FPC_THRESHOLD = 16;
    private static final double GORILLA_THRESHOLD = 0.4;

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

        // NOTE: SPEED uses a self-contained fast path that avoids ALP's expensive (e,f)
        // search entirely. Monotonic data (counters) goes through the integer delta pipeline
        // which exploits near-constant strides in sortable-long space — no predictor tables
        // or warm-up cost. Non-monotonic data uses FPC whose FCM/DFCM predictors learn
        // value patterns via simple hash lookups, feeding residuals into the same fast
        // integer pipeline.
        if (hint == OptimizeFor.SPEED) {
            if (profile.isMonotonicallyIncreasing() || profile.isMonotonicallyDecreasing()) {
                return PipelineConfig.forDoubles(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
            }
            return PipelineConfig.forDoubles(blockSize).fpcStage().delta().offset().gcd().patchedPFor().bitPack();
        }

        // NOTE: the XOR gate decides whether consecutive-value XOR produces meaningfully
        // smaller residuals than the raw bit-width. A simple "xorMaxBits < rawMaxBits"
        // is too permissive — saving 1-2 bits doesn't justify the overhead of XOR algorithms
        // vs ALP's simpler integer arithmetic path. Requiring at least 25% reduction filters
        // out marginal cases where ALP would do as well or better via decimal structure.
        // This applies equally to monotonic and non-monotonic data — monotonic counters with
        // poor XOR statistics but good decimal structure benefit from lossless ALP.
        final long xorReduction = profile.rawMaxBits() - profile.xorMaxBits();
        if (xorReduction >= profile.rawMaxBits() / 4) {
            return selectXorDouble(profile, blockSize);
        }

        // NOTE: ALP exploits decimal structure in the IEEE domain — it finds integer
        // exponent/factor pairs (e,f) that convert doubles to integers with minimal
        // exceptions. This works regardless of XOR statistics and is the right choice
        // when data originates from decimal sources (sensor readings, prices, percentages).
        // Counters must never be quantized — precision artifacts break rate calculations
        // (rate = (c[t2] - c[t1]) / dt). Force lossless ALP for counter fields.
        return selectAlpDouble(blockSize, metricType == MetricType.COUNTER ? null : hint);
    }

    private static PipelineConfig selectAlpDouble(int blockSize, @Nullable OptimizeFor hint) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_2_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
        }
        if (hint == OptimizeFor.BALANCED) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_6_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
        }
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage().delta().offset().gcd().patchedPFor().bitPack();
    }

    private static PipelineConfig selectXorDouble(final BlockProfile profile, int blockSize) {
        // NOTE: deltaDeltaMaxBits measures the max bit-width of second-order differences
        // (values[i] - 2*values[i-1] + values[i-2]). When small, the stride between
        // consecutive values is nearly constant — exactly the pattern FPC's DFCM predictor
        // learns. DFCM converges by i=3 for constant strides, producing near-zero residuals.
        if (profile.deltaDeltaMaxBits() <= FPC_THRESHOLD) {
            return PipelineConfig.forDoubles(blockSize).fpcStage().delta().offset().gcd().patchedPFor().bitPack();
        }

        // NOTE: xorZeroCount counts consecutive identical values (XOR = 0). Gorilla's
        // Case 0 encodes these in just 1 bit — no other algorithm matches this for
        // repeated values.
        if (profile.valueCount() > 1) {
            final double xorZeroFraction = (double) profile.xorZeroCount() / (profile.valueCount() - 1);
            if (xorZeroFraction >= GORILLA_THRESHOLD) {
                return PipelineConfig.forDoubles(blockSize).gorilla();
            }
        }

        // NOTE: when xorMaxBits is at most half of rawMaxBits, the previous-value XOR
        // already produces compact residuals — the ring buffer won't improve much but
        // costs index bits per value. Streaming Chimp saves that overhead.
        if (profile.xorMaxBits() <= profile.rawMaxBits() / 2) {
            return PipelineConfig.forDoubles(blockSize).chimp();
        }

        return PipelineConfig.forDoubles(blockSize).chimp128();
    }

    // NOTE: mirrors selectDouble with float-specific builder and stage types.
    // See selectDouble for detailed rationale on each decision point.
    private static PipelineConfig selectFloat(
        final BlockProfile profile,
        int blockSize,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        if (profile.range() == 0) {
            return PipelineConfig.forFloats(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
        }

        if (hint == OptimizeFor.SPEED) {
            if (profile.isMonotonicallyIncreasing() || profile.isMonotonicallyDecreasing()) {
                return PipelineConfig.forFloats(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
            }
            return PipelineConfig.forFloats(blockSize).fpcStage().delta().offset().gcd().patchedPFor().bitPack();
        }

        // NOTE: see selectDouble for rationale on XOR gate and counter protection.
        final long xorReduction = profile.rawMaxBits() - profile.xorMaxBits();
        if (xorReduction >= profile.rawMaxBits() / 4) {
            return selectXorFloat(profile, blockSize);
        }

        return selectAlpFloat(blockSize, metricType == MetricType.COUNTER ? null : hint);
    }

    private static PipelineConfig selectAlpFloat(int blockSize, @Nullable OptimizeFor hint) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage(QUANTIZE_2_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
        }
        if (hint == OptimizeFor.BALANCED) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage(QUANTIZE_6_DECIMALS).delta().offset().gcd().patchedPFor().bitPack();
        }
        return PipelineConfig.forFloats(blockSize).alpFloatStage().delta().offset().gcd().patchedPFor().bitPack();
    }

    private static PipelineConfig selectXorFloat(final BlockProfile profile, int blockSize) {
        if (profile.deltaDeltaMaxBits() <= FPC_THRESHOLD) {
            return PipelineConfig.forFloats(blockSize).fpcStage().delta().offset().gcd().patchedPFor().bitPack();
        }

        if (profile.valueCount() > 1) {
            final double xorZeroFraction = (double) profile.xorZeroCount() / (profile.valueCount() - 1);
            if (xorZeroFraction >= GORILLA_THRESHOLD) {
                return PipelineConfig.forFloats(blockSize).gorilla();
            }
        }

        if (profile.xorMaxBits() <= profile.rawMaxBits() / 2) {
            return PipelineConfig.forFloats(blockSize).chimp();
        }

        return PipelineConfig.forFloats(blockSize).chimp128();
    }
}
