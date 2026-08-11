/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.elasticsearch.columnar.ColumnarWriteProfile;

/**
 * A factory that builds a {@link NumericPipeline} for a target write profile and block size.
 * Returned by {@link NumericPipelineSelector#select} so that the selector can express "which
 * pipeline type" without knowing the block size or the profile, while
 * {@link org.elasticsearch.columnar.ColumNARDocValuesFormat} remains the sole owner of the block
 * size decision and {@link org.elasticsearch.columnar.ColumNARDocValuesConsumer} passes the
 * profile through.
 *
 * <p>Baseline factories ignore the profile:
 * <pre>{@code
 * (profile, bs) -> NumericPipeline.monotonicLongPipeline(bs)
 * }</pre>
 *
 * <p>Future factories that require a minimum version compare
 * {@link ColumnarWriteProfile#version()} against a {@code FormatVersion.VERSION_*} constant at
 * build time before constructing the pipeline.
 */
@FunctionalInterface
public interface NumericPipelineTemplate {

    /**
     * Builds a pipeline configured for the given write profile and block size. Baseline factories
     * ignore {@code profile}. Factories that require a minimum version compare
     * {@link ColumnarWriteProfile#version()} against a {@code VERSION_*} constant before
     * constructing the pipeline.
     */
    NumericPipeline build(ColumnarWriteProfile profile, int blockSize);
}
