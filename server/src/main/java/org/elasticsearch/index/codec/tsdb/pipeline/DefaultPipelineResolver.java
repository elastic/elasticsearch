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

public final class DefaultPipelineResolver implements PipelineResolver {

    private static final Logger logger = LogManager.getLogger(DefaultPipelineResolver.class);

    private static final int TSDB_BLOCK_SIZE = 512;
    private static final int LOGSDB_BLOCK_SIZE = 128;

    private static final double QUANTIZE_STORAGE = 1e-6;
    private static final double QUANTIZE_BALANCED = 1e-12;

    @Override
    public PipelineConfig resolve(final String fieldName, final FieldContext context) {
        // NOTE: STANDARD mode always uses the default pipeline; no automatic selection.
        if (context.indexMode() == IndexMode.STANDARD) {
            return PipelineConfig.defaultConfig();
        }

        final PipelineConfig staticResult = resolveFromStaticRules(fieldName, context);
        if (staticResult != null) {
            logger.debug("field [{}] mode=[{}] -> [{}]", fieldName, context.indexMode(), staticResult);
            return staticResult;
        }

        logger.debug("field [{}] mode=[{}] -> default", fieldName, context.indexMode());
        return PipelineConfig.defaultConfig();
    }

    @Nullable
    private PipelineConfig resolveFromStaticRules(final String fieldName, final FieldContext context) {
        if (context.indexMode() == IndexMode.TIME_SERIES) {
            return resolveForTimeSeries(fieldName, context);
        }

        if (context.indexMode() == IndexMode.LOGSDB) {
            return resolveForLogsDb(fieldName, context);
        }

        return null;
    }

    @Nullable
    private PipelineConfig resolveForTimeSeries(final String fieldName, final FieldContext context) {
        final MappedFieldType fieldType = context.fieldType();
        final String typeName = fieldType.typeName();
        final MetricType metricType = extractMetricType(fieldType);
        if (metricType == MetricType.GAUGE && "double".equals(typeName)) {
            if (context.hint() == OptimizeFor.STORAGE) {
                return PipelineConfig.forDoubles(TSDB_BLOCK_SIZE).alpDoubleStage(QUANTIZE_STORAGE).offset().gcd().bitPack();
            }
            if (context.hint() == OptimizeFor.BALANCED) {
                return PipelineConfig.forDoubles(TSDB_BLOCK_SIZE).alpDoubleStage(QUANTIZE_BALANCED).offset().gcd().bitPack();
            }
            if (context.hint() == OptimizeFor.SPEED) {
                return PipelineConfig.forDoubles(TSDB_BLOCK_SIZE).xor().patchedPFor().bitPack();
            }
            return PipelineConfig.forDoubles(TSDB_BLOCK_SIZE).alpDoubleStage(QUANTIZE_STORAGE).offset().gcd().bitPack();
        }
        if (metricType == MetricType.GAUGE && "float".equals(typeName)) {
            return PipelineConfig.forFloats(TSDB_BLOCK_SIZE).alpFloatStage().offset().gcd().bitPack();
        }
        if (metricType == MetricType.COUNTER && "double".equals(typeName)) {
            return PipelineConfig.forDoubles(TSDB_BLOCK_SIZE).fpcStage().offset().gcd().bitPack();
        }
        if (metricType == MetricType.COUNTER && "float".equals(typeName)) {
            return PipelineConfig.forFloats(TSDB_BLOCK_SIZE).fpcStage().offset().gcd().bitPack();
        }
        return null;
    }

    @Nullable
    private PipelineConfig resolveForLogsDb(final String fieldName, final FieldContext context) {
        final String typeName = context.fieldType().typeName();
        if ("double".equals(typeName)) {
            return PipelineConfig.forDoubles(LOGSDB_BLOCK_SIZE).alpDoubleStage().offset().gcd().bitPack();
        }
        if ("float".equals(typeName)) {
            return PipelineConfig.forFloats(LOGSDB_BLOCK_SIZE).alpFloatStage().offset().gcd().bitPack();
        }
        return null;
    }

    @Nullable
    private static MetricType extractMetricType(final MappedFieldType fieldType) {
        if (fieldType instanceof NumberFieldType numberFieldType) {
            return numberFieldType.getMetricType();
        }
        return null;
    }

}
