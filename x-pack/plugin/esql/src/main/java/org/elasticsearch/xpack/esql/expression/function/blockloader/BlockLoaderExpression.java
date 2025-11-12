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
 * {@link Expression} that can be "pushed" into value loading. Most of the time
 * we load values into {@link Block}s and then run the expressions on them, but
 * sometimes it's worth short-circuiting this process and running the expression
 * in the tight loop we use for loading:
 * <ul>
 *     <li>
 *         {@code V_COSINE(vector, [constant_vector])} - vector is ~512 floats
 *         and V_COSINE is one double. We can find the similarity without any
 *         copies if we combine.
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
 *     <li>
 *         {@code MV_MIN} and {@code MV_MAX} - loads much fewer data for
 *         multivalued fields.
 *     </li>
 * </ul>
 */
public interface BlockLoaderExpression {
    /**
     * The field and loading configuration that replaces this expression, effectively
     * "fusing" the expression into the load. Or null if the fusion isn't possible.
     * <p>
     *     {@link SearchStats#supportsLoaderConfig} checks that the configuration is
     *     supported by all field mappers. Callers to this method <strong>must</strong>
     *     call that to confirm that configurations returned are supported. Implementations
     *     of this method do not need to call it, though they <strong>may</strong> use
     *     methods like {@link SearchStats#hasDocValues} and {@link SearchStats#isIndexed}
     *     as preflight checks. They <strong>should</strong> use those methods if it is
     *     expensive to build the {@link BlockLoaderFunctionConfig}.
     * </p>
     */
    @Nullable
    PushedBlockLoaderExpression tryPushToFieldLoading(SearchStats stats);

    /**
     * Expression "fused" to the block loader.
     * @param field the field whose load we're fusing into
     * @param config the expression's configuration
     */
    record PushedBlockLoaderExpression(FieldAttribute field, BlockLoaderFunctionConfig config) {}
}
