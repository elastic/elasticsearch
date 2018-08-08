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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Objects;


/**
 * Encapsulates a request to _count API against one, several or all indices.
 */
public final class CountRequest extends ActionRequest implements IndicesRequest.Replaceable {

    //CountRequest wraps SearchRequest and delegates appropriate methods
    private final SearchRequest searchRequest;

    public CountRequest() {
        this.searchRequest = new SearchRequest();
    }

    @Override
    public ActionRequestValidationException validate() {
        return searchRequest.validate();
    }

    /**
     * Constructs a new count request against the indices. No indices provided here means that count will execute on all indices.
     */
    public CountRequest(String... indices) {
        this.searchRequest = new SearchRequest(indices);
    }

    /**
     * Constructs a new search request against the provided indices with the given search source.
     */
    public CountRequest(String[] indices, SearchSourceBuilder source) {
        this.searchRequest = new SearchRequest(indices, source);
    }

    /**
     * Sets the indices the count will be executed on.
     */
    public CountRequest indices(String... indices) {
        searchRequest.indices(indices);
        return this;
    }

    /**
     * The source of the count request.
     */
    public CountRequest source(SearchSourceBuilder sourceBuilder) {
        searchRequest.source(sourceBuilder);
        return this;
    }

    /**
     * The document types to execute the count against. Defaults to be executed against all types.
     *
     * @deprecated Types are going away, prefer filtering on a type.
     */
    @Deprecated
    public CountRequest types(String... types) {
        searchRequest.types(types);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the count will be executed on.
     */
    public CountRequest routing(String routing) {
        searchRequest.routing(routing);
        return this;
    }

    /**
     * The routing values to control the count that the search will be executed on.
     */
    public CountRequest routing(String... routings) {
        searchRequest.routing(routings);
        return this;
    }

    /**
     * Returns the indices options used to resolve indices. They tell for instance whether a single index is accepted, whether an empty
     * array will be converted to _all, and how wildcards will be expanded if needed.
     *
     * @see org.elasticsearch.action.support.IndicesOptions
     */
    public CountRequest indicesOptions(IndicesOptions indicesOptions) {
        searchRequest.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Sets the preference to execute the count. Defaults to randomize across shards. Can be set to {@code _local} to prefer local shards
     * or a custom value, which guarantees that the same order will be used across different requests.
     */
    public CountRequest preference(String preference) {
        searchRequest.preference(preference);
        return this;
    }

    public IndicesOptions indicesOptions() {
        return searchRequest.indicesOptions();
    }

    public String routing() {
        return searchRequest.routing();
    }

    public String preference() {
        return searchRequest.preference();
    }

    public SearchSourceBuilder source() {
        return searchRequest.source();
    }

    public String[] indices() {
        return searchRequest.indices();
    }

    public String[] types() {
        return searchRequest.types();
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountRequest that = (CountRequest) o;
        return Objects.equals(searchRequest, that.searchRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchRequest);
    }

}
