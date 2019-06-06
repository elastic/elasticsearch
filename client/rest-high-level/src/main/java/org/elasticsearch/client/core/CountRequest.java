/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;

/**
 * Encapsulates a request to _count API against one, several or all indices.
 */
public final class CountRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] types = Strings.EMPTY_ARRAY;
    private String routing;
    private String preference;
    private SearchSourceBuilder searchSourceBuilder;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public CountRequest() {
        this.searchSourceBuilder = new SearchSourceBuilder();
    }

    /**
     * Constructs a new count request against the indices. No indices provided here means that count will execute on all indices.
     */
    public CountRequest(String... indices) {
        this(indices, new SearchSourceBuilder());
    }

    /**
     * Constructs a new search request against the provided indices with the given search source.
     */
    public CountRequest(String[] indices, SearchSourceBuilder searchSourceBuilder) {
        indices(indices);
        this.searchSourceBuilder = searchSourceBuilder;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
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
     */
    public CountRequest source(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = Objects.requireNonNull(searchSourceBuilder, "source must not be null");
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
        return this.searchSourceBuilder.minScore();
    }

    public CountRequest minScore(Float minScore) {
        this.searchSourceBuilder.minScore(minScore);
        return this;
    }

    public int terminateAfter() {
        return this.searchSourceBuilder.terminateAfter();
    }

    public CountRequest terminateAfter(int terminateAfter) {
        this.searchSourceBuilder.terminateAfter(terminateAfter);
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

    public SearchSourceBuilder source() {
        return this.searchSourceBuilder;
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
        return Objects.equals(indicesOptions, that.indicesOptions) &&
            Arrays.equals(indices, that.indices) &&
            Arrays.equals(types, that.types) &&
            Objects.equals(routing, that.routing) &&
            Objects.equals(preference, that.preference);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indicesOptions, routing, preference);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(types);
        return result;
    }
}
