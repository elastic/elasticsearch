/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

/**
 * Holds context used when merging mappings.
 * As the merge process also involves building merged {@link Mapper.Builder}s,
 * this also contains a {@link MapperBuilderContext}.
 */
public final class MapperMergeContext {

    private final MapperBuilderContext mapperBuilderContext;

    /**
     * The root context, to be used when merging a tree of mappers
     */
    public static MapperMergeContext root(boolean isSourceSynthetic, boolean isDataStream) {
        return new MapperMergeContext(MapperBuilderContext.root(isSourceSynthetic, isDataStream));
    }

    /**
     * Creates a new {@link MapperMergeContext} from a {@link MapperBuilderContext}
     * @param mapperBuilderContext the {@link MapperBuilderContext} for this {@link MapperMergeContext}
     * @return a new {@link MapperMergeContext}, wrapping the provided {@link MapperBuilderContext}
     */
    public static MapperMergeContext from(MapperBuilderContext mapperBuilderContext) {
        return new MapperMergeContext(mapperBuilderContext);
    }

    private MapperMergeContext(MapperBuilderContext mapperBuilderContext) {
        this.mapperBuilderContext = mapperBuilderContext;
    }

    /**
     * Creates a new {@link MapperMergeContext} that is a child of this context
     * @param name the name of the child context
     * @return a new {@link MapperMergeContext} with this context as its parent
     */
    public MapperMergeContext createChildContext(String name) {
        return new MapperMergeContext(mapperBuilderContext.createChildContext(name));
    }

    MapperBuilderContext getMapperBuilderContext() {
        return mapperBuilderContext;
    }
}
