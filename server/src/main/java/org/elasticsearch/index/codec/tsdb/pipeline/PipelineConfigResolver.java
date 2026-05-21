/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * Resolves a {@link PipelineConfig} for a given field. Implementations may
 * return different pipelines based on the {@link FieldContext}, enabling
 * per-field encoding strategies such as delta-of-delta for timestamp fields.
 */
@FunctionalInterface
public interface PipelineConfigResolver {

    /**
     * Resolves the pipeline configuration for the given field.
     *
     * @param context the field context for pipeline selection
     * @return the pipeline configuration
     */
    PipelineConfig resolve(FieldContext context);
}
