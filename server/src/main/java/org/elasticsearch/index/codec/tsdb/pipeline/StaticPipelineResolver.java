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
import org.apache.lucene.store.IOContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;

public final class StaticPipelineResolver implements PipelineResolver {

    private static final Logger logger = LogManager.getLogger(StaticPipelineResolver.class);

    private static final double QUANTIZE_STORAGE = 1e-6;

    public static final StaticPipelineResolver INSTANCE = new StaticPipelineResolver();

    @Override
    public PipelineConfig resolve(final FieldContext context, long[] sample, int sampleSize, IOContext ioContext) {
        if (context.indexMode() == IndexMode.STANDARD) {
            return PipelineConfig.of(context.dataType(), context.blockSize(), ES819_BASELINE_SPECS);
        }

        final PipelineConfig staticResult = resolveFromStaticRules(context);
        if (staticResult != null) {
            logger.debug("field [{}] mode=[{}] -> [{}]", context.fieldName(), context.indexMode(), staticResult);
            return staticResult;
        }

        logger.debug("field [{}] mode=[{}] -> default", context.fieldName(), context.indexMode());
        return PipelineConfig.of(context.dataType(), context.blockSize(), ES819_BASELINE_SPECS);
    }

    @Nullable
    private static PipelineConfig resolveFromStaticRules(final FieldContext context) {
        if (context.indexMode() == IndexMode.TIME_SERIES) {
            return resolveForTimeSeries(context);
        }
        if (context.indexMode() == IndexMode.LOGSDB) {
            return resolveForLogsDb(context);
        }
        return null;
    }

    @Nullable
    private static PipelineConfig resolveForTimeSeries(final FieldContext context) {
        final int blockSize = context.blockSize();
        final MetricType metricType = context.metricType();
        if (metricType == MetricType.GAUGE) {
            return resolveGauge(context.dataType(), context.hint(), blockSize);
        }
        if (metricType == MetricType.COUNTER) {
            return resolveCounter(context.dataType(), context.hint(), blockSize);
        }
        if (context.isDateField()) {
            return resolveDate(blockSize);
        }
        return null;
    }

    @Nullable
    private static PipelineConfig resolveGauge(final PipelineConfig.DataType dataType, final OptimizeFor hint, int blockSize) {
        if (dataType == PipelineConfig.DataType.DOUBLE) {
            return resolveDoubleGauge(hint, blockSize);
        }
        if (dataType == PipelineConfig.DataType.FLOAT) {
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
    private static PipelineConfig resolveCounter(final PipelineConfig.DataType dataType, final OptimizeFor hint, int blockSize) {
        if (dataType == PipelineConfig.DataType.DOUBLE) {
            return resolveDoubleCounter(hint, blockSize);
        }
        if (dataType == PipelineConfig.DataType.FLOAT) {
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
    private static PipelineConfig resolveForLogsDb(final FieldContext context) {
        final int blockSize = context.blockSize();
        if (context.dataType() == PipelineConfig.DataType.DOUBLE) {
            return PipelineConfig.forDoubles(blockSize).alpDoubleStage().offset().gcd().bitPack();
        }
        if (context.dataType() == PipelineConfig.DataType.FLOAT) {
            return PipelineConfig.forFloats(blockSize).alpFloatStage().offset().gcd().bitPack();
        }
        if (context.isDateField()) {
            return resolveDate(blockSize);
        }
        return null;
    }

    private static PipelineConfig resolveDate(int blockSize) {
        return PipelineConfig.forLongs(blockSize).deltaDelta().offset().gcd().bitPack();
    }
}
