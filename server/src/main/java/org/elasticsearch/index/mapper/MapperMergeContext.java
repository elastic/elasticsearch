/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.MapperService.MergeReason;

/**
 * Holds context used when merging mappings.
 * As the merge process also involves building merged {@link Mapper.Builder}s,
 * this also contains a {@link MapperBuilderContext}.
 */
public final class MapperMergeContext {

    private final MapperBuilderContext mapperBuilderContext;
    private final ParseFieldLimits fieldLimits;

    private MapperMergeContext(MapperBuilderContext mapperBuilderContext, ParseFieldLimits fieldLimits) {
        this.mapperBuilderContext = mapperBuilderContext;
        this.fieldLimits = fieldLimits;
    }

    /**
     * The root context, to be used when merging a tree of mappers
     */
    public static MapperMergeContext root(boolean isSourceSynthetic, boolean isDataStream, MergeReason mergeReason, long newFieldsBudget) {
        return root(
            isSourceSynthetic,
            isDataStream,
            mergeReason,
            ParseFieldLimits.withBudget(NewFieldsBudget.dropping(newFieldsBudget)),
            false,
            false
        );
    }

    /**
     * The root context, to be used when merging a tree of mappers in a strict columnar index
     */
    public static MapperMergeContext root(
        boolean isSourceSynthetic,
        boolean isDataStream,
        MergeReason mergeReason,
        ParseFieldLimits fieldLimits,
        boolean isStrictColumnar,
        boolean isSourceColumnarStored
    ) {
        return new MapperMergeContext(
            MapperBuilderContext.root(isSourceSynthetic, isDataStream, mergeReason, isStrictColumnar, isSourceColumnarStored),
            fieldLimits
        );
    }

    /**
     * Creates a new {@link MapperMergeContext} from a {@link MapperBuilderContext}
     * @param mapperBuilderContext the {@link MapperBuilderContext} for this {@link MapperMergeContext}
     * @param newFieldsBudget limits how many fields can be added during the merge process
     * @return a new {@link MapperMergeContext}, wrapping the provided {@link MapperBuilderContext}
     */
    public static MapperMergeContext from(MapperBuilderContext mapperBuilderContext, long newFieldsBudget) {
        return new MapperMergeContext(mapperBuilderContext, ParseFieldLimits.withBudget(NewFieldsBudget.dropping(newFieldsBudget)));
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
        return new MapperMergeContext(childContext, fieldLimits);
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
