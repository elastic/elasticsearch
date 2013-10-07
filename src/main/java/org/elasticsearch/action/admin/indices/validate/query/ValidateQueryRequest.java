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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.client.Requests;
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
 * A request to validate a specific query.
 * <p/>
 * <p>The request requires the query source to be set either using {@link #query(org.elasticsearch.index.query.QueryBuilder)},
 * or {@link #query(byte[])}.
 */
public class ValidateQueryRequest extends BroadcastOperationRequest<ValidateQueryRequest> {

    private static final XContentType contentType = Requests.CONTENT_TYPE;

    private BytesReference querySource;
    private boolean querySourceUnsafe;

    private boolean explain;

    private String[] types = Strings.EMPTY_ARRAY;

    long nowInMillis;

    ValidateQueryRequest() {
    }

    /**
     * Constructs a new validate request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public ValidateQueryRequest(String... indices) {
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
    public ValidateQueryRequest query(QueryBuilder queryBuilder) {
        this.querySource = queryBuilder.buildAsBytes();
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to execute in the form of a map.
     */
    public ValidateQueryRequest query(Map querySource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(querySource);
            return query(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + querySource + "]", e);
        }
    }

    public ValidateQueryRequest query(XContentBuilder builder) {
        this.querySource = builder.bytes();
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to validate. It is preferable to use either {@link #query(byte[])}
     * or {@link #query(org.elasticsearch.index.query.QueryBuilder)}.
     */
    public ValidateQueryRequest query(String querySource) {
        this.querySource = new BytesArray(querySource);
        this.querySourceUnsafe = false;
        return this;
    }

    /**
     * The query source to validate.
     */
    public ValidateQueryRequest query(byte[] querySource) {
        return query(querySource, 0, querySource.length, false);
    }

    /**
     * The query source to validate.
     */
    public ValidateQueryRequest query(byte[] querySource, int offset, int length, boolean unsafe) {
        return query(new BytesArray(querySource, offset, length), unsafe);
    }

    /**
     * The query source to validate.
     */
    public ValidateQueryRequest query(BytesReference querySource, boolean unsafe) {
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
    public ValidateQueryRequest types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * Indicate if detailed information about query is requested
     */
    public void explain(boolean explain) {
        this.explain = explain;
    }

    /**
     * Indicates if detailed information about query is requested
     */
    public boolean explain() {
        return explain;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        querySourceUnsafe = false;
        querySource = in.readBytesReference();

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }

        explain = in.readBoolean();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeBytesReference(querySource);

        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }

        out.writeBoolean(explain);
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(querySource, false);
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(types) + ", querySource[" + sSource + "], explain:" + explain;
    }
}
