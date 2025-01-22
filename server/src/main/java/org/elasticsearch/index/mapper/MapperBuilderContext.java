/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.util.Objects;

/**
 * Holds context for building Mapper objects from their Builders
 */
public class MapperBuilderContext {

    /**
     * The root context, to be used when building a tree of mappers
     */
    public static MapperBuilderContext root(boolean isSourceSynthetic, boolean isDataStream) {
        return root(isSourceSynthetic, isDataStream, MergeReason.MAPPING_UPDATE);
    }

    public static MapperBuilderContext root(boolean isSourceSynthetic, boolean isDataStream, MergeReason mergeReason) {
        var params = new MapperBuilderContextParams(
            null,
            isSourceSynthetic,
            isDataStream,
            false,
            ObjectMapper.Defaults.DYNAMIC,
            mergeReason
        );
        return new MapperBuilderContext(params, false);
    }

    private final String path;
    private final boolean isSourceSynthetic;
    private final boolean isDataStream;
    private final boolean parentObjectContainsDimensions;
    private final ObjectMapper.Dynamic dynamic;
    private final MergeReason mergeReason;
    private final boolean inNestedContext;

    /**
     * @param path the full name of the field, taking into account parent objects
     * @param isSourceSynthetic is the {@code _source} field being reconstructed on the fly?
     * @param isDataStream are these mappings being built for a data stream index?
     * @param parentObjectContainsDimensions are these field mappings being built dimensions?
     * @param dynamic strategy for handling dynamic mappings in this context
     * @param mergeReason the merge reason to use when merging mappers while building the mapper
     */
    public record MapperBuilderContextParams(
        String path,
        boolean isSourceSynthetic,
        boolean isDataStream,
        boolean parentObjectContainsDimensions,
        ObjectMapper.Dynamic dynamic,
        MergeReason mergeReason
    ) {
        public MapperBuilderContextParams {
            Objects.requireNonNull(dynamic, "dynamic must not be null");
        }
    }

    MapperBuilderContext(MapperBuilderContextParams params, boolean inNestedContext) {
        Objects.requireNonNull(params, "params must not be null");
        this.path = params.path;
        this.isSourceSynthetic = params.isSourceSynthetic;
        this.isDataStream = params.isDataStream;
        this.parentObjectContainsDimensions = params.parentObjectContainsDimensions;
        this.dynamic = params.dynamic;
        this.mergeReason = params.mergeReason;
        this.inNestedContext = inNestedContext;
    }

    /**
     * Creates a new MapperBuilderContext that is a child of this context
     *
     * @param name    the name of the child context
     * @param dynamic strategy for handling dynamic mappings in this context
     * @return a new MapperBuilderContext with this context as its parent
     */
    public MapperBuilderContext createChildContext(String name, @Nullable ObjectMapper.Dynamic dynamic) {
        return createChildContext(name, this.parentObjectContainsDimensions, dynamic);
    }

    /**
     * Creates a new MapperBuilderContext that is a child of this context
     *
     * @param name    the name of the child context
     * @param dynamic strategy for handling dynamic mappings in this context
     * @param parentObjectContainsDimensions whether the parent object contains dimensions
     * @return a new MapperBuilderContext with this context as its parent
     */
    public MapperBuilderContext createChildContext(
        String name,
        boolean parentObjectContainsDimensions,
        @Nullable ObjectMapper.Dynamic dynamic
    ) {
        var params = new MapperBuilderContextParams(
            buildFullName(name),
            this.isSourceSynthetic,
            this.isDataStream,
            parentObjectContainsDimensions,
            getDynamic(dynamic),
            this.mergeReason
        );

        return new MapperBuilderContext(params, isInNestedContext());
    }

    protected ObjectMapper.Dynamic getDynamic(@Nullable ObjectMapper.Dynamic dynamic) {
        return dynamic == null ? this.dynamic : dynamic;
    }

    /**
     * Builds the full name of the field, taking into account parent objects
     */
    public String buildFullName(String name) {
        if (Strings.isEmpty(path)) {
            return name;
        }
        return path + "." + name;
    }

    /**
     * Is the {@code _source} field being reconstructed on the fly?
     */
    public boolean isSourceSynthetic() {
        return isSourceSynthetic;
    }

    /**
     * Are these mappings being built for a data stream index?
     */
    public boolean isDataStream() {
        return isDataStream;
    }

    /**
     * Are these field mappings being built dimensions?
     */
    public boolean parentObjectContainsDimensions() {
        return parentObjectContainsDimensions;
    }

    public ObjectMapper.Dynamic getDynamic() {
        return dynamic;
    }

    /**
     * The merge reason to use when merging mappers while building the mapper.
     * See also {@link ObjectMapper.Builder#buildMappers(MapperBuilderContext)}.
     */
    public MergeReason getMergeReason() {
        return mergeReason;
    }

    /**
     * Returns true if this context is included in a nested context, either directly or any of its ancestors.
     */
    public boolean isInNestedContext() {
        return inNestedContext;
    }
}
