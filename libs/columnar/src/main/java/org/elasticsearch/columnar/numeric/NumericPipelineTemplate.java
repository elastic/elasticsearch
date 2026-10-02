/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

/**
 * A factory that builds a {@link NumericPipeline} for a given block size. Returned by
 * {@link NumericPipelineSelector#select} so that the selector can express "which pipeline type"
 * without knowing the block size, while
 * {@link org.elasticsearch.columnar.ColumNARDocValuesFormat} remains the sole owner of the block
 * size decision.
 *
 * <p>Typical usage as a method reference:
 * <pre>{@code
 * (fieldName, type) -> NumericPipeline::monotonicLongPipeline
 * }</pre>
 */
@FunctionalInterface
public interface NumericPipelineTemplate {

    /**
     * Builds a pipeline configured for the given block size.
     */
    NumericPipeline build(int blockSize);
}
