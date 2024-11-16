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
    private final NewFieldsBudget newFieldsBudget;

    private MapperMergeContext(MapperBuilderContext mapperBuilderContext, NewFieldsBudget newFieldsBudget) {
        this.mapperBuilderContext = mapperBuilderContext;
        this.newFieldsBudget = newFieldsBudget;
    }

    /**
     * The root context, to be used when merging a tree of mappers
     */
    public static MapperMergeContext root(boolean isSourceSynthetic, boolean isDataStream, MergeReason mergeReason, long newFieldsBudget) {
        return new MapperMergeContext(
            MapperBuilderContext.root(isSourceSynthetic, isDataStream, mergeReason),
            NewFieldsBudget.of(newFieldsBudget)
        );
    }

    /**
     * Creates a new {@link MapperMergeContext} from a {@link MapperBuilderContext}
     * @param mapperBuilderContext the {@link MapperBuilderContext} for this {@link MapperMergeContext}
     * @param newFieldsBudget limits how many fields can be added during the merge process
     * @return a new {@link MapperMergeContext}, wrapping the provided {@link MapperBuilderContext}
     */
    public static MapperMergeContext from(MapperBuilderContext mapperBuilderContext, long newFieldsBudget) {
        return new MapperMergeContext(mapperBuilderContext, NewFieldsBudget.of(newFieldsBudget));
    }

    /**
     * Creates a new {@link MapperMergeContext} with a child {@link MapperBuilderContext}.
     * The child {@link MapperMergeContext} context will share the same field limit.
     * @param name the name of the child context
     * @return a new {@link MapperMergeContext} with this context as its parent
     */
    public MapperMergeContext createChildContext(String name, ObjectMapper.Dynamic dynamic) {
        return createChildContext(mapperBuilderContext.createChildContext(name, dynamic));
    }

    /**
     * Creates a new {@link MapperMergeContext} with a given child {@link MapperBuilderContext}
     * The child {@link MapperMergeContext} context will share the same field limit.
     * @param childContext the child {@link MapperBuilderContext}
     * @return a new {@link MapperMergeContext}, wrapping the provided {@link MapperBuilderContext}
     */
    MapperMergeContext createChildContext(MapperBuilderContext childContext) {
        return new MapperMergeContext(childContext, newFieldsBudget);
    }

    public MapperBuilderContext getMapperBuilderContext() {
        return mapperBuilderContext;
    }

    boolean decrementFieldBudgetIfPossible(int fieldSize) {
        return newFieldsBudget.decrementIfPossible(fieldSize);
    }

    /**
     * Keeps track of how many new fields can be added during mapper merge.
     * The field budget is shared across instances of {@link MapperMergeContext} that are created via
     * {@link MapperMergeContext#createChildContext}.
     * This ensures that fields that are consumed by one child object mapper also decrement the budget for another child object.
     * Not thread safe.The same instance may not be modified by multiple threads.
     */
    private interface NewFieldsBudget {

        static NewFieldsBudget of(long fieldsBudget) {
            if (fieldsBudget == Long.MAX_VALUE) {
                return Unlimited.INSTANCE;
            }
            return new Limited(fieldsBudget);
        }

        boolean decrementIfPossible(long fieldSize);

        final class Unlimited implements NewFieldsBudget {

            private static final Unlimited INSTANCE = new Unlimited();

            private Unlimited() {}

            @Override
            public boolean decrementIfPossible(long fieldSize) {
                return true;
            }
        }

        final class Limited implements NewFieldsBudget {

            private long fieldsBudget;

            Limited(long fieldsBudget) {
                this.fieldsBudget = fieldsBudget;
            }

            @Override
            public boolean decrementIfPossible(long fieldSize) {
                if (fieldsBudget >= fieldSize) {
                    fieldsBudget -= fieldSize;
                    return true;
                }
                return false;
            }
        }
    }
}
