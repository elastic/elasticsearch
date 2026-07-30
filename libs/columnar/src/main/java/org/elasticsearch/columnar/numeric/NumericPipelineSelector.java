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
 * Selects the {@link NumericPipeline} to use when writing a numeric column. The library calls
 * {@link #select} once per field at write time and uses whatever pipeline is returned.
 *
 * <p>Implementations live outside {@code libs/columnar}: the server module supplies a concrete
 * implementation that inspects field type, index mode, and metric role via the mapper. The
 * library never imports mapper types. A typical server-side implementation closes over mapper
 * context and routes by field semantics:
 *
 * <pre>{@code
 * new ColumNARDocValuesFormat((fieldName, blockSize) -> {
 *     if (isMonotonicLong(fieldName))
 *         return NumericPipeline.monotonicLongPipeline(blockSize);
 *     if (isDoubleGauge(fieldName))
 *         return NumericPipeline.doubleGaugePipeline(blockSize);
 *     if (isDoubleCounter(fieldName))
 *         return NumericPipeline.doubleCounterPipeline(blockSize);
 *     return NumericPipeline.defaultPipeline(blockSize);
 * });
 * }</pre>
 *
 * <p>The no-arg {@link org.elasticsearch.columnar.ColumNARDocValuesFormat} constructor wires a
 * default implementation that always returns {@link NumericPipeline#defaultPipeline}.
 */
@FunctionalInterface
public interface NumericPipelineSelector {

    /**
     * Returns the pipeline to use for the named field.
     *
     * @param fieldName the Lucene field name
     * @param blockSize the number of values per block; pass to the chosen
     *                  {@link NumericPipeline} factory so stateful stages are sized correctly
     */
    NumericPipeline select(String fieldName, int blockSize);
}
