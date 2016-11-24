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

package org.elasticsearch.client;

import org.elasticsearch.common.Strings;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Representation of a request to the "_search" endpoint.
 * Wraps a @link {@link SearchSourceBuilder} representing the request body. Setting indices, types and url parameters can be chained.
 */
public class SearchRequest {

    private SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] types = Strings.EMPTY_ARRAY;
    Map<String, String> urlParams = Collections.emptyMap();

    /**
     * Create a new SearchRequest
     * @param searchSource the builder for the request body
     */
    public SearchRequest(SearchSourceBuilder searchSource) {
        this.searchSourceBuilder = Objects.requireNonNull(searchSource);
    }

    /**
     * Set the indices this request runs on
     */
    public SearchRequest indices(String... indices) {
        this.indices = Objects.requireNonNull(indices);
        return this;
    }

    /**
     * Set the types this request runs on
     */
    public SearchRequest types(String... types) {
        this.types = Objects.requireNonNull(types);
        return this;
    }

    /**
     * Set the url parameters used by this request
     */
    public SearchRequest params(Map<String, String> urlParams) {
        this.urlParams = Objects.requireNonNull(urlParams);
        return this;
    }

    /**
     * Get the underlying {@link SearchSourceBuilder}
     */
    public SearchSourceBuilder searchSource() {
        return this.searchSourceBuilder;
    }

    /**
     * Get the indices this request runs on
     */
    public String[] indices() {
        return this.indices;
    }

    /**
     * Get the types this request runs on
     */
    public String[] types() {
        return this.types;
    }

    /**
     * Get the url parameters used in this request
     */
    public Map<String, String> params() {
        return this.urlParams;
    }
}
