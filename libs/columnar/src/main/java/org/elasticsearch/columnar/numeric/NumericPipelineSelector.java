/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.elasticsearch.columnar.ColumnarFieldType;

/**
 * Selects the {@link NumericPipelineTemplate} to use when writing a numeric column. The library
 * calls {@link #select} once per field at write time and then applies the format's block size and
 * write profile to the returned template to obtain the concrete {@link NumericPipeline}.
 *
 * <p>The selector only chooses the logical pipeline for a field; compatibility enforcement happens
 * when the returned template is built with the writer's
 * {@link org.elasticsearch.columnar.ColumnarWriteProfile}. Implementations must not inspect or
 * branch on the write profile; that responsibility belongs to the template factory.
 *
 * <p>Implementations live outside {@code libs/columnar}: the server module supplies a concrete
 * implementation that inspects field type, index mode, and metric role via the mapper. The library
 * never imports mapper types. A typical server-side implementation routes by field semantics:
 *
 * <pre>{@code
 * new ColumNARDocValuesFormat.Builder()
 *     .pipelineSelector((fieldName, type) -> switch (type) {
 *         case DOUBLE -> (profile, bs) -> NumericPipeline.doubleGaugePipeline(bs);
 *         default     -> (profile, bs) -> NumericPipeline.defaultPipeline(bs);
 *     })
 *     .blockSize(blockSize)
 *     .build()
 * }</pre>
 *
 * <p>The no-arg {@link org.elasticsearch.columnar.ColumNARDocValuesFormat} constructor wires a
 * default implementation that always returns {@link NumericPipeline#defaultPipeline}.
 */
@FunctionalInterface
public interface NumericPipelineSelector {

    /**
     * Returns a template for the pipeline to use for the named field. The template is called with
     * the format's block size to produce the concrete {@link NumericPipeline}.
     *
     * @param fieldName the Lucene field name
     * @param type      the columnar field type resolved from {@link org.apache.lucene.index.FieldInfo} attributes
     */
    NumericPipelineTemplate select(String fieldName, ColumnarFieldType type);
}
