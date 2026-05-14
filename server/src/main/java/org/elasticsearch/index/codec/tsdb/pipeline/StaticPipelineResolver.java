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
 * A {@link PipelineResolver} that returns the same pipeline for all fields:
 * {@code delta > offset > gcd > bitpack}. This matches the ES819 baseline
 * encoding and is the default for ES95.
 */
public final class StaticPipelineResolver implements PipelineResolver {

    @Override
    public PipelineConfig resolve(final FieldContext context) {
        return PipelineConfig.forLongs(context.blockSize()).delta().offset().gcd().bitPack();
    }
}
