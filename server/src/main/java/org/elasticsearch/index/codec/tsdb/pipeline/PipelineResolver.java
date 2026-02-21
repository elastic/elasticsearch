/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;

/**
 * Resolves a pipeline configuration based on field context and an optional data sample.
 */
public interface PipelineResolver {

    enum OptimizeFor {
        STORAGE,
        SPEED,
        BALANCED
    }

    record FieldContext(
        String fieldName,
        IndexMode indexMode,
        PipelineConfig.DataType dataType,
        @Nullable OptimizeFor hint,
        @Nullable MetricType metricType,
        boolean isDateField,
        int blockSize
    ) {}

    PipelineConfig resolve(FieldContext context, long[] sample, int sampleSize, IOContext ioContext);
}
