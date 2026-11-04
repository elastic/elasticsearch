/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.util.function.Predicate;

/**
 * Holds context used when merging mappings.
 * As the merge process also involves building merged {@link Mapper.Builder}s,
 * this also contains a {@link MapperBuilderContext}.
 */
public final class MapperMergeContext {

    private final MapperBuilderContext mapperBuilderContext;
    private final ParseFieldLimits fieldLimits;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    /**
     * The root context, to be used when merging a tree of mappers
     */
    static MapperMergeContext root(
        boolean isSourceSynthetic,
        boolean isDataStream,
        MergeReason mergeReason,
        ParseFieldLimits fieldLimits,
        boolean isStrictColumnar,
        boolean isSourceColumnarStored,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return new MapperMergeContext(
            MapperBuilderContext.root(isSourceSynthetic, isDataStream, mergeReason, isStrictColumnar, isSourceColumnarStored),
            fieldLimits,
            clusterSupportsFeature
        );
    }

    /**
     * Creates a new {@link MapperMergeContext} from a {@link MapperBuilderContext}.
     *
     * @param mapperBuilderContext the {@link MapperBuilderContext} for this {@link MapperMergeContext}
     * @param newFieldsBudget limits how many fields can be added during the merge process
     * @param clusterSupportsFeature the cluster feature check
     * @return a new {@link MapperMergeContext}, wrapping the provided {@link MapperBuilderContext}
     */
    static MapperMergeContext fromBuilderContext(
        MapperBuilderContext mapperBuilderContext,
        long newFieldsBudget,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return new MapperMergeContext(
            mapperBuilderContext,
            ParseFieldLimits.withBudget(NewFieldsBudget.dropping(newFieldsBudget)),
            clusterSupportsFeature
        );
    }

    private MapperMergeContext(
        MapperBuilderContext mapperBuilderContext,
        ParseFieldLimits fieldLimits,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        this.mapperBuilderContext = mapperBuilderContext;
        this.fieldLimits = fieldLimits;
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    /**
     * Creates a context over the same {@link MapperBuilderContext} and feature predicate with an unlimited, independent field budget.
     */
    public MapperMergeContext withUnlimitedParseFieldLimitsBudget() {
        return withUnlimitedParseFieldLimitsBudget(mapperBuilderContext);
    }

    /**
     * Creates a context over the given {@link MapperBuilderContext} with this context's feature predicate and an unlimited,
     * independent field budget.
     */
    public MapperMergeContext withUnlimitedParseFieldLimitsBudget(MapperBuilderContext mapperBuilderContext) {
        return new MapperMergeContext(
            mapperBuilderContext,
            ParseFieldLimits.withBudget(NewFieldsBudget.dropping(Long.MAX_VALUE)),
            clusterSupportsFeature
        );
    }

    /**
     * Creates a new {@link MapperMergeContext} with a child {@link MapperBuilderContext}.
     * The child {@link MapperMergeContext} context will share the same field limits.
     * @param name the name of the child context
     * @return a new {@link MapperMergeContext} with this context as its parent
     */
    public MapperMergeContext createChildContext(String name, ObjectMapper.Dynamic dynamic) {
        return createChildContext(mapperBuilderContext.createChildContext(name, dynamic));
    }

    /**
     * Creates a new {@link MapperMergeContext} with a given child {@link MapperBuilderContext}.
     * The child shares the same {@link ParseFieldLimits} so counts accumulate across the whole tree.
     * @param childContext the child {@link MapperBuilderContext}
     * @return a new {@link MapperMergeContext}, wrapping the provided {@link MapperBuilderContext}
     */
    MapperMergeContext createChildContext(MapperBuilderContext childContext) {
        return new MapperMergeContext(childContext, fieldLimits, clusterSupportsFeature);
    }

    /**
     * Returns {@code true} if all nodes in the cluster support {@code feature}.
     */
    public boolean clusterHasFeature(NodeFeature feature) {
        return clusterSupportsFeature.test(feature);
    }

    public MapperBuilderContext getMapperBuilderContext() {
        return mapperBuilderContext;
    }

    boolean decrementFieldBudgetIfPossible(int fieldSize) {
        return fieldLimits.decrementTotalFieldsIfPossible(fieldSize);
    }

    void checkNestedFieldCount() {
        fieldLimits.checkNestedFieldCount();
    }
}
