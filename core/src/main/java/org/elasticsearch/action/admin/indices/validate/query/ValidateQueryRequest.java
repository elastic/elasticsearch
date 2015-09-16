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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * A request to validate a specific query.
 * <p/>
 * <p>The request requires the query source to be set either using {@link #source(QuerySourceBuilder)},
 * or {@link #source(byte[])}.
 */
public class ValidateQueryRequest extends BroadcastRequest<ValidateQueryRequest> {

    private BytesReference source;

    private boolean explain;
    private boolean rewrite;

    private String[] types = Strings.EMPTY_ARRAY;

    long nowInMillis;

    public ValidateQueryRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    /**
     * Constructs a new validate request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public ValidateQueryRequest(String... indices) {
        super(indices);
        indicesOptions(IndicesOptions.fromOptions(false, false, true, false));
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        return validationException;
    }

    /**
     * The source to execute.
     */
    public BytesReference source() {
        return source;
    }

    public ValidateQueryRequest source(QuerySourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        return this;
    }

    /**
     * The source to execute in the form of a map.
     */
    public ValidateQueryRequest source(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public ValidateQueryRequest source(XContentBuilder builder) {
        this.source = builder.bytes();
        return this;
    }

    /**
     * The query source to validate. It is preferable to use either {@link #source(byte[])}
     * or {@link #source(QuerySourceBuilder)}.
     */
    public ValidateQueryRequest source(String source) {
        this.source = new BytesArray(source);
        return this;
    }

    /**
     * The source to validate.
     */
    public ValidateQueryRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * The source to validate.
     */
    public ValidateQueryRequest source(byte[] source, int offset, int length) {
        return source(new BytesArray(source, offset, length));
    }

    /**
     * The source to validate.
     */
    public ValidateQueryRequest source(BytesReference source) {
        this.source = source;
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

    /**
     * Indicates whether the query should be rewritten into primitive queries
     */
    public void rewrite(boolean rewrite) {
        this.rewrite = rewrite;
    }

    /**
     * Indicates whether the query should be rewritten into primitive queries
     */
    public boolean rewrite() {
        return rewrite;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        source = in.readBytesReference();

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }

        explain = in.readBoolean();
        rewrite = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeBytesReference(source);

        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }

        out.writeBoolean(explain);
        out.writeBoolean(rewrite);
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(types) + ", source[" + sSource + "], explain:" + explain + 
                ", rewrite:" + rewrite;
    }
}
