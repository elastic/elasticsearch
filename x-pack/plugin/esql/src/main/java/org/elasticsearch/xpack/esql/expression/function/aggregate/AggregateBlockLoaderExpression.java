/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;

/**
 * Aggregation functions that can benefit from pushing a per-document multi-value
 * reduction into the block loader. Unlike {@link BlockLoaderExpression} — which
 * replaces an entire expression with a fused field load — this interface keeps
 * the aggregate intact and only rewrites its <strong>field input</strong>.
 * <p>
 *     For example, {@code MIN(field)} is mathematically equivalent to
 *     {@code MIN(MV_MIN(field))}. By returning {@link BlockLoaderFunctionConfig.Function#MV_MIN}
 *     from {@link #fieldBlockLoaderFunction()}, the optimizer can instruct the
 *     block loader to materialize only the per-document minimum instead of all
 *     multi-values, reducing both memory and CPU.
 * </p>
 */
public interface AggregateBlockLoaderExpression {
    /**
     * Returns the {@link BlockLoaderFunctionConfig.Function} to apply to this
     * aggregate's field input during block loading, or {@code null} if no
     * optimization is applicable.
     */
    @Nullable
    BlockLoaderFunctionConfig.Function fieldBlockLoaderFunction();
}
