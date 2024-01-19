/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds context used when merging mappings.
 * As the merge process also involves building merged {@link Mapper.Builder}s,
 * this also contains a {@link MapperBuilderContext}.
 */
public final class MapperMergeContext {

    private final MapperBuilderContext mapperBuilderContext;
    private final AtomicLong remainingFieldsBudget;

    /**
     * The root context, to be used when merging a tree of mappers
     */
    public static MapperMergeContext root(boolean isSourceSynthetic, boolean isDataStream, long newFieldsBudget) {
        return new MapperMergeContext(MapperBuilderContext.root(isSourceSynthetic, isDataStream), new AtomicLong(newFieldsBudget));
    }

    /**
     * Creates a new {@link MapperMergeContext} from a {@link MapperBuilderContext}
     * @param mapperBuilderContext the {@link MapperBuilderContext} for this {@link MapperMergeContext}
     * @param newFieldsBudget limits how many fields can be added during the merge process
     * @return a new {@link MapperMergeContext}, wrapping the provided {@link MapperBuilderContext}
     */
    public static MapperMergeContext from(MapperBuilderContext mapperBuilderContext, long newFieldsBudget) {
        return new MapperMergeContext(mapperBuilderContext, new AtomicLong(newFieldsBudget));
    }

    private MapperMergeContext(MapperBuilderContext mapperBuilderContext, AtomicLong remainingFieldsBudget) {
        this.mapperBuilderContext = mapperBuilderContext;
        this.remainingFieldsBudget = remainingFieldsBudget;
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
            if (remainingFieldsBudget.get() != Long.MAX_VALUE) {
                remainingFieldsBudget.incrementAndGet();
            }
        }
    }

    void addRuntimeFieldIfPossible(Map<String, RuntimeField> runtimeFields, RuntimeField runtimeField) {
        if (runtimeFields.containsKey(runtimeField.name())) {
            runtimeFields.put(runtimeField.name(), runtimeField);
        } else if (canAddField(1)) {
            remainingFieldsBudget.decrementAndGet();
            runtimeFields.put(runtimeField.name(), runtimeField);
        }
    }

    boolean addFieldIfPossible(Map<String, Mapper> mappers, Mapper mapper) {
        if (canAddField(mapper.mapperSize())) {
            remainingFieldsBudget.getAndAdd(mapper.mapperSize() * -1);
            mappers.put(mapper.simpleName(), mapper);
            return true;
        }
        return false;
    }

    void addFieldIfPossible(Mapper mapper, Runnable addField) {
        if (canAddField(mapper.mapperSize())) {
            remainingFieldsBudget.getAndAdd(mapper.mapperSize() * -1);
            addField.run();
        }
    }

    boolean canAddField(int fieldSize) {
        return remainingFieldsBudget.get() >= fieldSize;
    }
}
