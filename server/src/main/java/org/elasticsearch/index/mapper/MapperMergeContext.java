/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Holds context used when merging mappings.
 * As the merge process also involves building merged {@link Mapper.Builder}s,
 * this also contains a {@link MapperBuilderContext}.
 */
public final class MapperMergeContext {

    private final MapperBuilderContext mapperBuilderContext;
    private final NewFieldsBudget remainingFieldsBudget;

    private MapperMergeContext(MapperBuilderContext mapperBuilderContext, NewFieldsBudget remainingFieldsBudget) {
        this.mapperBuilderContext = mapperBuilderContext;
        this.remainingFieldsBudget = remainingFieldsBudget;
    }

    /**
     * The root context, to be used when merging a tree of mappers
     */
    public static MapperMergeContext root(boolean isSourceSynthetic, boolean isDataStream, NewFieldsBudget newFieldsBudget) {
        return new MapperMergeContext(MapperBuilderContext.root(isSourceSynthetic, isDataStream), newFieldsBudget);
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
     * Creates a new {@link MapperMergeContext} that is a child of this context
     * @param name the name of the child context
     * @return a new {@link MapperMergeContext} with this context as its parent
     */
    public MapperMergeContext createChildContext(String name) {
        return createChildContext(mapperBuilderContext.createChildContext(name));
    }

    /**
     * Creates a new {@link MapperMergeContext} with a given {@link MapperBuilderContext}
     * @param childContext the child {@link MapperBuilderContext}
     * @return a new {@link MapperMergeContext}, wrapping the provided {@link MapperBuilderContext}
     */
    public MapperMergeContext createChildContext(MapperBuilderContext childContext) {
        return new MapperMergeContext(childContext, remainingFieldsBudget);
    }

    MapperBuilderContext getMapperBuilderContext() {
        return mapperBuilderContext;
    }

    void removeRuntimeField(Map<String, RuntimeField> runtimeFields, String name) {
        if (runtimeFields.containsKey(name)) {
            runtimeFields.remove(name);
            remainingFieldsBudget.increment(1);
        }
    }

    <M extends Mapper> boolean addFieldIfPossible(M mapper, Consumer<M> addField) {
        if (remainingFieldsBudget.decrementIfPossible(mapper.mapperSize())) {
            addField.accept(mapper);
            return true;
        }
        return false;
    }

    void addRuntimeFieldIfPossible(RuntimeField runtimeField, Consumer<RuntimeField> addField) {
        if (remainingFieldsBudget.decrementIfPossible(1)) {
            addField.accept(runtimeField);
        }
    }

    boolean hasRemainingBudget() {
        return remainingFieldsBudget.hasRemainingBudget();
    }

    /**
     * Keeps track of now many new fields can be added during mapper merge
     */
    public interface NewFieldsBudget {
        static NewFieldsBudget unlimited() {
            return Unlimited.INSTANCE;
        }

        static NewFieldsBudget of(long fieldsBudget) {
            return new Limited(fieldsBudget);
        }

        boolean decrementIfPossible(long fieldSize);

        void increment(long fieldSize);

        boolean hasRemainingBudget();

        class Unlimited implements NewFieldsBudget {

            private static final Unlimited INSTANCE = new Unlimited();

            private Unlimited() {}

            @Override
            public boolean decrementIfPossible(long fieldSize) {
                return true;
            }

            @Override
            public void increment(long fieldSize) {
                // noop
            }

            @Override
            public boolean hasRemainingBudget() {
                return true;
            }
        }

        class Limited implements NewFieldsBudget {

            private long fieldsBudget;

            public Limited(long fieldsBudget) {
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

            @Override
            public void increment(long fieldSize) {
                fieldsBudget++;
            }

            @Override
            public boolean hasRemainingBudget() {
                return fieldsBudget >= 1;
            }
        }
    }
}
