/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.Required;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to count the number of documents matching a specific query. Best created with
 * {@link org.elasticsearch.client.Requests#countRequest(String...)}.
 *
 * <p>The request requires the query source to be set either using {@link #querySource(org.elasticsearch.index.query.QueryBuilder)},
 * or {@link #querySource(byte[])}.
 *
 * @author kimchy (shay.banon)
 * @see CountResponse
 * @see org.elasticsearch.client.Client#count(CountRequest)
 * @see org.elasticsearch.client.Requests#countRequest(String...)
 */
public class CountRequest extends BroadcastOperationRequest {

    public static final float DEFAULT_MIN_SCORE = -1f;

    private float minScore = DEFAULT_MIN_SCORE;
    @Required private byte[] querySource;
    private String[] types = Strings.EMPTY_ARRAY;
    @Nullable private String queryParserName;

    CountRequest() {
    }

    /**
     * Constructs a new count request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public CountRequest(String... indices) {
        super(indices, null);
    }

    /**
     * Controls the operation threading model.
     */
    @Override public CountRequest operationThreading(BroadcastOperationThreading operationThreading) {
        super.operationThreading(operationThreading);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override public CountRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * A query hint to optionally later be used when routing the request.
     */
    public CountRequest queryHint(String queryHint) {
        this.queryHint = queryHint;
        return this;
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
    byte[] querySource() {
        return querySource;
    }

    /**
     * The query source to execute.
     *
     * @see org.elasticsearch.index.query.json.JsonQueryBuilders
     */
    @Required public CountRequest querySource(QueryBuilder queryBuilder) {
        return querySource(queryBuilder.buildAsBytes());
    }

    /**
     * The query source to execute. It is preferable to use either {@link #querySource(byte[])}
     * or {@link #querySource(org.elasticsearch.index.query.QueryBuilder)}.
     */
    @Required public CountRequest querySource(String querySource) {
        return querySource(Unicode.fromStringAsBytes(querySource));
    }

    /**
     * The query source to execute.
     */
    @Required public CountRequest querySource(byte[] querySource) {
        this.querySource = querySource;
        return this;
    }

    /**
     * The query parse name to use. If not set, will use the default one.
     */
    String queryParserName() {
        return queryParserName;
    }

    /**
     * The query parse name to use. If not set, will use the default one.
     */
    public CountRequest queryParserName(String queryParserName) {
        this.queryParserName = queryParserName;
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

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        minScore = in.readFloat();
        querySource = new byte[in.readVInt()];
        in.readFully(querySource());
        if (in.readBoolean()) {
            queryParserName = in.readUTF();
        }
        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeFloat(minScore);
        out.writeVInt(querySource.length);
        out.writeBytes(querySource);
        if (queryParserName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryParserName);
        }
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }

    @Override public String toString() {
        return "[" + Arrays.toString(indices) + "][" + Arrays.toString(types) + "], querySource[" + Unicode.fromBytes(querySource) + "]";
    }
}
