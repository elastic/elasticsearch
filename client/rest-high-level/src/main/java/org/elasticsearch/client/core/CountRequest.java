/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Encapsulates a request to _count API against one, several or all indices.
 */
public final class CountRequest implements Validatable, ToXContentObject {

    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] types = Strings.EMPTY_ARRAY;
    private String routing;
    private String preference;
    private QueryBuilder query;
    private IndicesOptions indicesOptions;
    private int terminateAfter = SearchContext.DEFAULT_TERMINATE_AFTER;
    private Float minScore;

    public CountRequest() {}

    /**
     * Constructs a new count request against the indices. No indices provided here means that count will execute on all indices.
     */
    public CountRequest(String... indices) {
        indices(indices);
    }

    /**
     * Constructs a new search request against the provided indices with the given search source.
     *
     * @deprecated The count api only supports a query. Use {@link #CountRequest(String[], QueryBuilder)} instead.
     */
    @Deprecated
    public CountRequest(String[] indices, SearchSourceBuilder searchSourceBuilder) {
        indices(indices);
        this.query = Objects.requireNonNull(searchSourceBuilder, "source must not be null").query();
    }

    /**
     * Constructs a new search request against the provided indices with the given query.
     */
    public CountRequest(String[] indices, QueryBuilder query) {
        indices(indices);
        this.query = Objects.requireNonNull(query, "query must not be null");
        ;
    }

    /**
     * Sets the indices the count will be executed on.
     */
    public CountRequest indices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
        this.indices = indices;
        return this;
    }

    /**
     * The source of the count request.
     *
     * @deprecated The count api only supports a query. Use {@link #query(QueryBuilder)} instead.
     */
    @Deprecated
    public CountRequest source(SearchSourceBuilder searchSourceBuilder) {
        this.query = Objects.requireNonNull(searchSourceBuilder, "source must not be null").query();
        return this;
    }

    /**
     * Sets the query to execute for this count request.
     */
    public CountRequest query(QueryBuilder query) {
        this.query = Objects.requireNonNull(query, "query must not be null");
        return this;
    }

    /**
     * The document types to execute the count against. Defaults to be executed against all types.
     *
     * @deprecated Types are in the process of being removed. Instead of using a type, prefer to
     * filter on a field on the document.
     */
    @Deprecated
    public CountRequest types(String... types) {
        Objects.requireNonNull(types, "types must not be null");
        for (String type : types) {
            Objects.requireNonNull(type, "type must not be null");
        }
        this.types = types;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public CountRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the count will be executed on.
     */
    public CountRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * Returns the indices options used to resolve indices. They tell for instance whether a single index is accepted, whether an empty
     * array will be converted to _all, and how wildcards will be expanded if needed.
     *
     * @see org.elasticsearch.action.support.IndicesOptions
     */
    public CountRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
        return this;
    }

    /**
     * Sets the preference to execute the count. Defaults to randomize across shards. Can be set to {@code _local} to prefer local shards
     * or a custom value, which guarantees that the same order will be used across different requests.
     */
    public CountRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    public String routing() {
        return this.routing;
    }

    public String preference() {
        return this.preference;
    }

    public String[] indices() {
        return Arrays.copyOf(this.indices, this.indices.length);
    }

    public Float minScore() {
        return minScore;
    }

    public CountRequest minScore(Float minScore) {
        this.minScore = minScore;
        return this;
    }

    public int terminateAfter() {
        return this.terminateAfter;
    }

    public CountRequest terminateAfter(int terminateAfter) {
        if (terminateAfter < 0) {
            throw new IllegalArgumentException("terminateAfter must be > 0");
        }
        this.terminateAfter = terminateAfter;
        return this;
    }

    /**
     * @deprecated Types are in the process of being removed. Instead of using a type, prefer to
     * filter on a field on the document.
     */
    @Deprecated
    public String[] types() {
        return Arrays.copyOf(this.types, this.types.length);
    }

    /**
     * @return the source builder
     * @deprecated The count api only supports a query. Use {@link #query()} instead.
     */
    @Deprecated
    public SearchSourceBuilder source() {
        return new SearchSourceBuilder().query(query);
    }

    /**
     * @return The provided query to execute with the count request or
     * <code>null</code> if no query was provided.
     */
    public QueryBuilder query() {
        return query;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (query != null) {
            builder.field("query", query);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CountRequest that = (CountRequest) o;
        return Objects.equals(indicesOptions, that.indicesOptions)
            && Arrays.equals(indices, that.indices)
            && Arrays.equals(types, that.types)
            && Objects.equals(routing, that.routing)
            && Objects.equals(preference, that.preference)
            && Objects.equals(terminateAfter, that.terminateAfter)
            && Objects.equals(minScore, that.minScore)
            && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indicesOptions, routing, preference, terminateAfter, minScore, query);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(types);
        return result;
    }
}
