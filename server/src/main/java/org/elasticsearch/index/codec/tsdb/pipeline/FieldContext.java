/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.core.Nullable;

/**
 * Context about the field being encoded, passed to the {@link PipelineConfigResolver}
 * for pipeline selection.
 *
 * @param blockSize   the number of values per numeric block
 * @param fieldName   the name of the field being encoded
 * @param dataType    the underlying doc-values storage type of the field, or
 *                    {@code null} when unknown (e.g. construction sites without
 *                    mapper access). All integer-domain numeric mapper types
 *                    (long, integer, short, byte) collapse to
 *                    {@link PipelineDescriptor.DataType#LONG} because doc values
 *                    back-store them as long.
 * @param metricRole  the {@link MetricRole} of the field, or {@code null} when the
 *                    field is not a time-series metric
 */
public record FieldContext(
    int blockSize,
    String fieldName,
    @Nullable PipelineDescriptor.DataType dataType,
    @Nullable MetricRole metricRole
) {}
