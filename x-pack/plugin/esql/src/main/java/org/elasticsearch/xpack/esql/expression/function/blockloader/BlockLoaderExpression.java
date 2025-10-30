/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.blockloader;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.stats.SearchStats;

/**
 * {@link Expression} that can be "fused" into value loading. Most of the time
 * we load values into {@link Block}s and then run the expressions on them, but
 * sometimes it's worth short-circuiting this process and running the expression
 * in the tight loop we use for loading:
 * <ul>
 *     <li>
 *         {@code V_COSINE(vector, [constant_vector])} - vector is ~512 floats
 *         and V_COSINE is one double.
 *     </li>
 *     <li>
 *         {@code ST_CENTROID(shape)} - shapes can be quite large. Centroids
 *         are just one point.
 *     </li>
 *     <li>
 *         {@code LENGTH(string)} - strings can be quite long, but string length
 *         is always an int. For more fun, {@code keyword}s are usually stored
 *         using a dictionary, and it's <strong>fairly</strong> easy to optimize
 *         running {@code LENGTH} once per dictionary entry.
 *     </li>
 *     <li>
 *         {@code MV_COUNT(anything)} - counts are always integers.
 *     </li>
 * </ul>
 */
public interface BlockLoaderExpression {
    /**
     * The field and loading configuration that replaces this expression, effectively
     * "fusing" the expression into the load. Or null if the fusion isn't possible.
     */
    @Nullable
    Fuse tryFuse(SearchStats stats);

    /**
     * Fused load configuration.
     * @param field the field whose load we're fusing into
     * @param config the fusion configuration
     */
    record Fuse(FieldAttribute field, BlockLoaderFunctionConfig config) {}
}
