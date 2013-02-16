/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * A request to count the number of documents matching a specific query. Best created with
 * {@link org.elasticsearch.client.Requests#countRequest(String...)}.
 * <p/>
 * <p>The request requires the query source to be set either using {@link #setQuery(org.elasticsearch.index.query.QueryBuilder)},
 * or {@link #setQuery(byte[])}.
 *
 * @see CountResponse
 * @see org.elasticsearch.client.Client#count(CountRequest)
 * @see org.elasticsearch.client.Requests#countRequest(String...)
 */
public class CountRequest extends BroadcastOperationRequest<CountRequest> {

    private static final XContentType contentType = Requests.CONTENT_TYPE;

    public static final float DEFAULT_MIN_SCORE = -1f;

    private float minScore = DEFAULT_MIN_SCORE;

    @Nullable
    protected String routing;

    private BytesReference querySource;
    private boolean querySourceUnsafe;

    private String[] types = Strings.EMPTY_ARRAY;

    CountRequest() {
    }

    /**
     * Constructs a new count request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public CountRequest(String... indices) {
        super(indices);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        return validationException;
    }

    @Override
    protected void beforeStart() {
        if (querySourceUnsafe) {
            querySource = querySource.copyBytesArray();
            querySourceUnsafe = false;
        }
    }

    /**
     * The minimum score of the documents to include in the count.
     */
    float getMinScore() {
        return minScore;
    }

    /**
     * The minimum score of the documents to include in the count. Defaults to <tt>-1</tt> which means all
     * documents will be included in the count.
     */
    public CountRequest setMinScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    /**
     * The query source to execute.
     */
    BytesReference getQuerySource() {
        return querySource;
    }

    /**
     * The query source to execute.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    @Required
    public CountRequest setQuery(QueryBuilder queryBuilder) {
        this.querySource = queryBuilder.buildAsBytes();
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to execute in the form of a map.
     */
    @Required
    public CountRequest setQuery(Map querySource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(querySource);
            return setQuery(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + querySource + "]", e);
        }
    }

    @Required
    public CountRequest setQuery(XContentBuilder builder) {
        this.querySource = builder.bytes();
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to execute. It is preferable to use either {@link #setQuery(byte[])}
     * or {@link #setQuery(org.elasticsearch.index.query.QueryBuilder)}.
     */
    @Required
    public CountRequest setQuery(String querySource) {
        this.querySource = new BytesArray(querySource);
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to execute.
     */
    @Required
    public CountRequest setQuery(byte[] querySource) {
        return setQuery(querySource, 0, querySource.length, false);
    }

    /**
     * The query source to execute.
     */
    @Required
    public CountRequest setQuery(byte[] querySource, int offset, int length, boolean unsafe) {
        return setQuery(new BytesArray(querySource, offset, length), unsafe);
    }

    @Required
    public CountRequest setQuery(BytesReference querySource, boolean unsafe) {
        this.querySource = querySource;
        this.querySourceUnsafe = unsafe;
        return this;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    String[] getTypes() {
        return this.types;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public CountRequest setTypes(String... types) {
        this.types = types;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String getRouting() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public CountRequest setRouting(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public CountRequest setRouting(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        minScore = in.readFloat();
        routing = in.readOptionalString();
        querySourceUnsafe = false;
        querySource = in.readBytesReference();
        types = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeFloat(minScore);
        out.writeOptionalString(routing);
        out.writeBytesReference(querySource);
        out.writeStringArray(types);
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(querySource, false);
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(types) + ", querySource[" + sSource + "]";
    }
}
