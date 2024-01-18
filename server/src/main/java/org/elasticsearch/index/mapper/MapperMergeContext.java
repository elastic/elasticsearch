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

    public static MapperMergeContext root(boolean isSourceSynthetic, boolean isDataStream) {
        return new MapperMergeContext(MapperBuilderContext.root(isSourceSynthetic, isDataStream));
    }

    public static MapperMergeContext from(MapperBuilderContext mapperBuilderContext) {
        return new MapperMergeContext(mapperBuilderContext);
    }

    private MapperMergeContext(MapperBuilderContext mapperBuilderContext) {
        this.mapperBuilderContext = mapperBuilderContext;
    }

    public MapperMergeContext createChildContext(String name) {
        return createChildContext(mapperBuilderContext.createChildContext(name));
    }

    public MapperMergeContext createChildContext(MapperBuilderContext childContext) {
        return new MapperMergeContext(childContext);
    }

    public MapperBuilderContext getMapperBuilderContext() {
        return mapperBuilderContext;
    }
}
