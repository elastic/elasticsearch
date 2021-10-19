/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import com.fasterxml.jackson.core.filter.TokenFilter;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.support.filtering.FilterPathBasedFilter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class Mapper implements ToXContentFragment, Iterable<Mapper> {

    public abstract static class Builder {

        protected final String name;

        protected Builder(String name) {
            this.name = name;
        }

        public String name() {
            return this.name;
        }

        /** Returns a newly built mapper. */
        public abstract Mapper build(MapperBuilderContext context);
    }

    public interface TypeParser {
        Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext) throws MapperParsingException;
    }

    private final String simpleName;

    public Mapper(String simpleName) {
        Objects.requireNonNull(simpleName);
        this.simpleName = simpleName;
    }

    /** Returns the simple name, which identifies this mapper against other mappers at the same level in the mappers hierarchy
     * TODO: make this protected once Mapper and FieldMapper are merged together */
    public final String simpleName() {
        return simpleName;
    }

    /** Returns the canonical name which uniquely identifies the mapper against other mappers in a type. */
    public abstract String name();

    /**
     * Returns a name representing the type of this mapper.
     */
    public abstract String typeName();

    /** Return the merge of {@code mergeWith} into this.
     *  Both {@code this} and {@code mergeWith} will be left unmodified. */
    public abstract Mapper merge(Mapper mergeWith);

    /**
     * Validate any cross-field references made by this mapper
     * @param mappers a {@link MappingLookup} that can produce references to other mappers
     */
    public abstract void validate(MappingLookup mappers);

    /**
     * Validate a {@link TokenFilter} made from {@link IndexMetadata#INDEX_ROUTING_PATH}.
     */
    public final void validateRoutingPath(List<String> routingPaths) {
        validateRoutingPath(new FilterPathBasedFilter(Set.copyOf(routingPaths), true));
    }

    /**
     * Validate a {@link TokenFilter} made from {@link IndexMetadata#INDEX_ROUTING_PATH}.
     */
    private void validateRoutingPath(TokenFilter filter) {
        if (filter == TokenFilter.INCLUDE_ALL) {
            validateMatchedRoutingPath();
        }
        for (Mapper m : this) {
            TokenFilter next = filter.includeProperty(m.simpleName());
            if (next == null) {
                // null means "do not include"
                continue;
            }
            m.validateRoutingPath(next);
        }
    }

    /**
     * Validate that this field can be the target of {@link IndexMetadata#INDEX_ROUTING_PATH}.
     */
    protected void validateMatchedRoutingPath() {
        throw new IllegalArgumentException(
            "All fields that match routing_path must be keyword time_series_dimensions but [" + name() + "] was [" + typeName() + "]"
        );
    }
}
