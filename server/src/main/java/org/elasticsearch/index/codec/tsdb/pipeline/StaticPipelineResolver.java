/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;

public final class StaticPipelineResolver implements PipelineResolver {

    private static final Logger logger = LogManager.getLogger(StaticPipelineResolver.class);

    private static final double QUANTIZE_STORAGE = 1e-6;

    private final BlockSizeResolver blockSizeResolver;

    public StaticPipelineResolver(final BlockSizeResolver blockSizeResolver) {
        this.blockSizeResolver = blockSizeResolver;
    }

    @Override
    public PipelineConfig resolve(final FieldContext context, long[] sample, int sampleSize) {
        // NOTE: STANDARD mode always uses the default pipeline; no automatic selection.
        if (context.indexMode() == IndexMode.STANDARD) {
            return PipelineConfig.defaultConfig();
        }

        final int blockSize = blockSizeResolver.resolve(context);
        final PipelineConfig staticResult = resolveFromStaticRules(context, blockSize);
        if (staticResult != null) {
            logger.debug("field [{}] mode=[{}] -> [{}]", context.fieldName(), context.indexMode(), staticResult);
            return staticResult;
        }

        logger.debug("field [{}] mode=[{}] -> default", context.fieldName(), context.indexMode());
        return PipelineConfig.defaultConfig();
    }

    @Nullable
    private static PipelineConfig resolveFromStaticRules(final FieldContext context, int blockSize) {
        if (context.indexMode() == IndexMode.TIME_SERIES) {
            return resolveForTimeSeries(context, blockSize);
        }

        if (context.indexMode() == IndexMode.LOGSDB) {
            return resolveForLogsDb(context, blockSize);
        }

        return null;
    }

    @Nullable
    private static PipelineConfig resolveForTimeSeries(final FieldContext context, int blockSize) {
        final MetricType metricType = extractMetricType(context.fieldType());
        final String typeName = context.fieldType().typeName();
        if (metricType == MetricType.GAUGE) {
            return resolveGauge(typeName, context.hint(), blockSize);
        }
        if (metricType == MetricType.COUNTER) {
            return resolveCounter(typeName, context.hint(), blockSize);
        }
        if (isDateType(typeName)) {
            return resolveDate(blockSize);
        }
        return null;
    }

    @Nullable
    private static PipelineConfig resolveGauge(final String typeName, final OptimizeFor hint, int blockSize) {
        if ("double".equals(typeName)) {
            return resolveDoubleGauge(hint, blockSize);
        }
        if ("float".equals(typeName)) {
            return resolveFloatGauge(hint, blockSize);
        }
        return null;
    }

    private static PipelineConfig resolveDoubleGauge(final OptimizeFor hint, int blockSize) {
        if (hint == OptimizeFor.SPEED) {
            return PipelineConfig.forDoubles(blockSize).xor().patchedPFor().bitPack();
        }
        if (hint == OptimizeFor.BALANCED) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage().offset().gcd().bitPack();
        }
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage(QUANTIZE_STORAGE).offset().gcd().bitPack();
    }

    private static PipelineConfig resolveFloatGauge(final OptimizeFor hint, int blockSize) {
        if (hint == OptimizeFor.SPEED) {
            return PipelineConfig.forFloats(blockSize).xor().patchedPFor().bitPack();
        }
        if (hint == OptimizeFor.BALANCED) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage().offset().gcd().bitPack();
        }
        return PipelineConfig.forFloats(blockSize).alpFloatStage().offset().gcd().bitPack();
    }

    @Nullable
    private static PipelineConfig resolveCounter(final String typeName, final OptimizeFor hint, int blockSize) {
        if ("double".equals(typeName)) {
            return resolveDoubleCounter(hint, blockSize);
        }
        if ("float".equals(typeName)) {
            return resolveFloatCounter(hint, blockSize);
        }
        return null;
    }

    private static PipelineConfig resolveDoubleCounter(final OptimizeFor hint, int blockSize) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forDoubles(blockSize).gorilla();
        }
        return PipelineConfig.forDoubles(blockSize).fpcStage().offset().gcd().bitPack();
    }

    private static PipelineConfig resolveFloatCounter(final OptimizeFor hint, int blockSize) {
        if (hint == OptimizeFor.STORAGE) {
            return PipelineConfig.forFloats(blockSize).gorilla();
        }
        return PipelineConfig.forFloats(blockSize).fpcStage().offset().gcd().bitPack();
    }

    @Nullable
    private static PipelineConfig resolveForLogsDb(final FieldContext context, int blockSize) {
        final String typeName = context.fieldType().typeName();
        if ("double".equals(typeName)) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage().offset().gcd().bitPack();
        }
        if ("float".equals(typeName)) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage().offset().gcd().bitPack();
        }
        if (isDateType(typeName)) {
            return resolveDate(blockSize);
        }
        return null;
    }

    private static PipelineConfig resolveDate(int blockSize) {
        return PipelineConfig.forLongs(blockSize).deltaDelta().offset().gcd().bitPack();
    }

    private static boolean isDateType(final String typeName) {
        return "date".equals(typeName) || "date_nanos".equals(typeName);
    }

    @Nullable
    private static MetricType extractMetricType(final MappedFieldType fieldType) {
        if (fieldType instanceof NumberFieldType numberFieldType) {
            return numberFieldType.getMetricType();
        }
        return null;
    }

}
