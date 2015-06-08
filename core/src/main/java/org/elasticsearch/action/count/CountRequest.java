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

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.search.internal.SearchContext.DEFAULT_TERMINATE_AFTER;

/**
 * A request to count the number of documents matching a specific query. Best created with
 * {@link org.elasticsearch.client.Requests#countRequest(String...)}.
 * <p/>
 * <p>The request requires the query source to be set either using {@link #source(QuerySourceBuilder)},
 * or {@link #source(byte[])}.
 *
 * @see CountResponse
 * @see org.elasticsearch.client.Client#count(CountRequest)
 * @see org.elasticsearch.client.Requests#countRequest(String...)
 */
public class CountRequest extends BroadcastRequest<CountRequest> {

    public static final float DEFAULT_MIN_SCORE = -1f;

    private float minScore = DEFAULT_MIN_SCORE;

    @Nullable
    protected String routing;

    @Nullable
    private String preference;

    private BytesReference source;

    private String[] types = Strings.EMPTY_ARRAY;

    private int terminateAfter = DEFAULT_TERMINATE_AFTER;

    /**
     * Constructs a new count request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public CountRequest(String... indices) {
        super(indices);
    }

    /**
     * The minimum score of the documents to include in the count.
     */
    public float minScore() {
        return minScore;
    }

    /**
     * The minimum score of the documents to include in the count. Defaults to <tt>-1</tt> which means all
     * documents will be included in the count.
     */
    public CountRequest minScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    /**
     * The source to execute.
     */
    public BytesReference source() {
        return source;
    }

    /**
     * The source to execute.
     */
    public CountRequest source(QuerySourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        return this;
    }

    /**
     * The source to execute in the form of a map.
     */
    @SuppressWarnings("unchecked")
    public CountRequest source(Map querySource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(querySource);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + querySource + "]", e);
        }
    }

    public CountRequest source(XContentBuilder builder) {
        this.source = builder.bytes();
        return this;
    }

    /**
     * The source to execute. It is preferable to use either {@link #source(byte[])}
     * or {@link #source(QuerySourceBuilder)}.
     */
    public CountRequest source(String querySource) {
        this.source = new BytesArray(querySource);
        return this;
    }

    /**
     * The source to execute.
     */
    public CountRequest source(byte[] querySource) {
        return source(querySource, 0, querySource.length);
    }

    /**
     * The source to execute.
     */
    public CountRequest source(byte[] querySource, int offset, int length) {
        return source(new BytesArray(querySource, offset, length));
    }

    public CountRequest source(BytesReference querySource) {
        this.source = querySource;
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
        if (terminateAfterCount <= 0) {
            throw new IllegalArgumentException("terminateAfter must be > 0");
        }
        this.terminateAfter = terminateAfterCount;
        return this;
    }

    public int terminateAfter() {
        return this.terminateAfter;
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
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(types) + ", source[" + sSource + "]";
    }

    public SearchRequest toSearchRequest() {
        SearchRequest searchRequest = new SearchRequest(indices());
        searchRequest.indicesOptions(indicesOptions());
        searchRequest.types(types());
        searchRequest.routing(routing());
        searchRequest.preference(preference());
        searchRequest.source(source());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        if (minScore() != DEFAULT_MIN_SCORE) {
            searchSourceBuilder.minScore(minScore());
        }
        if (terminateAfter() != DEFAULT_TERMINATE_AFTER) {
            searchSourceBuilder.terminateAfter(terminateAfter());
        }
        searchRequest.extraSource(searchSourceBuilder);
        return searchRequest;
    }
}
