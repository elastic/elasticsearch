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

package org.elasticsearch.search.builder;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * A explain source builder allowing to easily build explain source. Simple construction using
 * {@link org.elasticsearch.search.builder.ExplainSourceBuilder#explainSource()}.
 * 
 * @see org.elasticsearch.action.explain.ExplainRequest#source(ExplainSourceBuilder)
 */
public class ExplainSourceBuilder implements ToXContent {

    /**
     * A static factory method to construct a new explain source.
     */
    public static ExplainSourceBuilder explainSource() {
        return new ExplainSourceBuilder();
    }

    private QueryBuilder queryBuilder;

    private byte[] queryBinary;
    private int queryBinaryOffset;
    private int queryBinaryLength;

    private long timeoutInMillis = -1;

    /**
     * Constructs a new explain source builder.
     */
    public ExplainSourceBuilder() {
    }

    /**
     * Constructs a new explain source builder with a search query.
     * 
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public ExplainSourceBuilder query(QueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    /**
     * Constructs a new explain source builder with a raw search query.
     */
    public ExplainSourceBuilder query(byte[] queryBinary) {
        return query(queryBinary, 0, queryBinary.length);
    }

    /**
     * Constructs a new explain source builder with a raw search query.
     */
    public ExplainSourceBuilder query(byte[] queryBinary, int queryBinaryOffset, int queryBinaryLength) {
        this.queryBinary = queryBinary;
        this.queryBinaryOffset = queryBinaryOffset;
        this.queryBinaryLength = queryBinaryLength;
        return this;
    }

    /**
     * Constructs a new explain source builder with a raw search query.
     */
    public ExplainSourceBuilder query(String queryString) {
        return query(Unicode.fromStringAsBytes(queryString));
    }

    /**
     * Constructs a new explain source builder with a query from a builder.
     */
    public ExplainSourceBuilder query(XContentBuilder query) {
        try {
            return query(query.underlyingBytes(), 0, query.underlyingBytesLength());
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("failed to generate query from builder", e);
        }
    }

    /**
     * Constructs a new explain source builder with a query from a map.
     */
    public ExplainSourceBuilder query(Map query) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(query);
            return query(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + query + "]", e);
        }
    }

    /**
     * An optional timeout to control how long explain is allowed to take.
     */
    public ExplainSourceBuilder timeout(TimeValue timeout) {
        this.timeoutInMillis = timeout.millis();
        return this;
    }

    /**
     * An optional timeout to control how long explain is allowed to take.
     */
    public ExplainSourceBuilder timeout(String timeout) {
        this.timeoutInMillis = TimeValue.parseTimeValue(timeout, null).millis();
        return this;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.string();
        } catch (Exception e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    public BytesStream buildAsBytesStream(XContentType contentType) throws ExplainSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.underlyingStream();
        } catch (Exception e) {
            throw new ExplainSourceBuilderException("Failed to build explain source", e);
        }
    }

    public byte[] buildAsBytes() throws ExplainSourceBuilderException {
        return buildAsBytes(Requests.CONTENT_TYPE);
    }

    public byte[] buildAsBytes(XContentType contentType) throws ExplainSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, EMPTY_PARAMS);
            return builder.copiedBytes();
        } catch (Exception e) {
            throw new ExplainSourceBuilderException("Failed to build explain source", e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (timeoutInMillis != -1) {
            builder.field("timeout", timeoutInMillis);
        }

        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }

        if (queryBinary != null) {
            if (XContentFactory.xContentType(queryBinary, queryBinaryOffset, queryBinaryLength) == builder
                    .contentType()) {
                builder.rawField("query", queryBinary, queryBinaryOffset, queryBinaryLength);
            } else {
                builder.field("query_binary", queryBinary, queryBinaryOffset, queryBinaryLength);
            }
        }

        builder.endObject();
        return builder;
    }

}
