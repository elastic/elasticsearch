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
 * <p>The request requires the query source to be set either using {@link #query(org.elasticsearch.index.query.QueryBuilder)},
 * or {@link #query(byte[])}.
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

    @Nullable
    private String preference;

    private BytesReference querySource;
    private boolean querySourceUnsafe;

    private String[] types = Strings.EMPTY_ARRAY;

    long nowInMillis;

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
    float minScore() {
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
     * The query source to execute.
     */
    BytesReference querySource() {
        return querySource;
    }

    /**
     * The query source to execute.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public CountRequest query(QueryBuilder queryBuilder) {
        this.querySource = queryBuilder.buildAsBytes();
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to execute in the form of a map.
     */
    public CountRequest query(Map querySource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(querySource);
            return query(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + querySource + "]", e);
        }
    }

    public CountRequest query(XContentBuilder builder) {
        this.querySource = builder.bytes();
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to execute. It is preferable to use either {@link #query(byte[])}
     * or {@link #query(org.elasticsearch.index.query.QueryBuilder)}.
     */
    public CountRequest query(String querySource) {
        this.querySource = new BytesArray(querySource);
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to execute.
     */
    public CountRequest query(byte[] querySource) {
        return query(querySource, 0, querySource.length, false);
    }

    /**
     * The query source to execute.
     */
    public CountRequest query(byte[] querySource, int offset, int length, boolean unsafe) {
        return query(new BytesArray(querySource, offset, length), unsafe);
    }

    public CountRequest query(BytesReference querySource, boolean unsafe) {
        this.querySource = querySource;
        this.querySourceUnsafe = unsafe;
        return this;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    String[] types() {
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        minScore = in.readFloat();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        querySourceUnsafe = false;
        querySource = in.readBytesReference();
        types = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeFloat(minScore);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
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
