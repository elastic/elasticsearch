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

package org.elasticsearch.action.count;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to count the number of documents matching a specific query. Best created with
 * {@link org.elasticsearch.client.Requests#countRequest(String...)}.
 *
 * @see CountResponse
 * @see org.elasticsearch.client.Client#count(CountRequest)
 * @see org.elasticsearch.client.Requests#countRequest(String...)
 */
public class CountRequest extends BroadcastRequest<CountRequest> {

    @Nullable
    protected String routing;

    @Nullable
    private String preference;

    private String[] types = Strings.EMPTY_ARRAY;

    private final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    /**
     * Constructs a new count request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public CountRequest(String... indices) {
        super(indices);
        searchSourceBuilder.size(0);
    }

    /**
     * The minimum score of the documents to include in the count.
     */
    public Float minScore() {
        return searchSourceBuilder.minScore();
    }

    /**
     * The minimum score of the documents to include in the count. Defaults to <tt>-1</tt> which means all
     * documents will be included in the count.
     */
    public CountRequest minScore(float minScore) {
        this.searchSourceBuilder.minScore(minScore);
        return this;
    }


    /**
     * The query to execute
     */
    public CountRequest query(QueryBuilder<?> queryBuilder) {
        this.searchSourceBuilder.query(queryBuilder);
        return this;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public String[] types() {
        return this.types;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public CountRequest types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public CountRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public CountRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    public CountRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * Upon reaching <code>terminateAfter</code> counts, the count request will early terminate
     */
    public CountRequest terminateAfter(int terminateAfterCount) {
        this.searchSourceBuilder.terminateAfter(terminateAfterCount);
        return this;
    }

    public int terminateAfter() {
        return this.searchSourceBuilder.terminateAfter();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("CountRequest doesn't support being sent over the wire, just a shortcut to the search api");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("CountRequest doesn't support being sent over the wire, just a shortcut to the search api");
    }

    @Override
    public String toString() {
        return "count request indices:" + Arrays.toString(indices) +
                ", types:" + Arrays.toString(types) +
                ", routing: " + routing +
                ", preference: " + preference +
                ", source:" + searchSourceBuilder.toString();
    }

    public SearchRequest toSearchRequest() {
        SearchRequest searchRequest = new SearchRequest(indices());
        searchRequest.source(searchSourceBuilder);
        searchRequest.indicesOptions(indicesOptions());
        searchRequest.types(types());
        searchRequest.routing(routing());
        searchRequest.preference(preference());
        return searchRequest;
    }
}
