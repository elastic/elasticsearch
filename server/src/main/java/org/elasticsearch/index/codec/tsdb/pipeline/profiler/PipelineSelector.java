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

    public static final PipelineSelector INSTANCE = new PipelineSelector();

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

    private static PipelineConfig selectLong(final BlockProfile profile, int blockSize) {
        // NOTE: wide general-purpose pipeline — each stage has skip logic so the
        // overhead for unused stages is just 1 bitmap bit. This is safe even when
        // the sample block is uninformative (e.g. constant or low-cardinality)
        // because later blocks may have different data shapes. Two chained deltas
        // replace the fused deltaDelta stage: the first delta fires on monotonic
        // data, and the second fires only if the resulting deltas are themselves
        // monotonic (e.g. accelerating sequences). For constant-rate timestamps,
        // the second delta skips and offset handles the constant residuals.
        return PipelineConfig.forLongs(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
    }

    private static PipelineConfig selectDouble(
        final BlockProfile profile,
        int blockSize,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        if (profile.range() == 0) {
            return PipelineConfig.forDoubles(blockSize).offset().gcd().bitPack();
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

    private static PipelineConfig selectFloat(
        final BlockProfile profile,
        int blockSize,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType
    ) {
        if (profile.range() == 0) {
            return PipelineConfig.forFloats(blockSize).offset().gcd().bitPack();
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
}
