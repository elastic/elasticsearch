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

package org.elasticsearch.action.explain;

import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.BaseRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.ExplainSourceBuilder;

/**
 * An explain action request builder.
 */
public class ExplainRequestBuilder extends BaseRequestBuilder<ExplainRequest, ExplainResponse> {

    private ExplainSourceBuilder sourceBuilder;

    public ExplainRequestBuilder(Client client) {
        super(client, new ExplainRequest());
    }

    /**
     * Sets the indices the explain will be executed on.
     */
    public ExplainRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * Sets the id of the document to explain
     */
    public ExplainRequestBuilder setId(String id) {
        request.id = id;
        return this;
    }

    /**
     * The document type to execute the explain against
     */
    public ExplainRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * An optional timeout to control how long explain is allowed to take.
     */
    public ExplainRequestBuilder setTimeout(TimeValue timeout) {
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * An optional timeout to control how long explain is allowed to take.
     */
    public ExplainRequestBuilder setTimeout(String timeout) {
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * Sets the preference to execute the explain. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public ExplainRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public ExplainRequestBuilder setListenerThreaded(boolean listenerThreaded) {
        request.listenerThreaded(listenerThreaded);
        return this;
    }

    /**
     * Constructs a new explain source builder with a query.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public ExplainRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Constructs a new explain source builder with a raw query.
     */
    public ExplainRequestBuilder setQuery(String query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Constructs a new explain source builder with a raw query.
     */
    public ExplainRequestBuilder setQuery(byte[] queryBinary) {
        sourceBuilder().query(queryBinary);
        return this;
    }

    /**
     * Constructs a new explain source builder with a raw query.
     */
    public ExplainRequestBuilder setQuery(byte[] queryBinary, int queryBinaryOffset, int queryBinaryLength) {
        sourceBuilder().query(queryBinary, queryBinaryOffset, queryBinaryLength);
        return this;
    }

    /**
     * Constructs a new explain source builder with a raw query.
     */
    public ExplainRequestBuilder setQuery(XContentBuilder query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Constructs a new explain source builder with a raw query.
     */
    public ExplainRequestBuilder setQuery(Map query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(String)}.
     */
    public ExplainRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public ExplainRequestBuilder setExtraSource(String source) {
        request.extraSource(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public ExplainRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public ExplainRequestBuilder setExtraSource(byte[] source) {
        request.extraSource(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public ExplainRequestBuilder setSource(byte[] source, int offset, int length) {
        request.source(source, offset, length);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public ExplainRequestBuilder setExtraSource(byte[] source, int offset, int length) {
        request.extraSource(source, offset, length);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public ExplainRequestBuilder setSource(XContentBuilder builder) {
        request.source(builder);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public ExplainRequestBuilder setExtraSource(XContentBuilder builder) {
        request.extraSource(builder);
        return this;
    }

    /**
     * Sets the source of the request as a map. Note, setting anything other than the
     * search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(java.util.Map)}.
     */
    public ExplainRequestBuilder setSource(Map source) {
        request.source(source);
        return this;
    }

    public ExplainRequestBuilder setExtraSource(Map source) {
        request.extraSource(source);
        return this;
    }

    /**
     * Sets the source builder to be used with this request. Note, any operations done
     * on this require builder before are discarded as this internal builder replaces
     * what has been built up until this point.
     */
    public ExplainRequestBuilder internalBuilder(ExplainSourceBuilder sourceBuilder) {
        this.sourceBuilder = sourceBuilder;
        return this;
    }

    /**
     * Returns the internal explain source builder used to construct the request.
     */
    public ExplainSourceBuilder internalBuilder() {
        return sourceBuilder();
    }

    @Override
    public String toString() {
        return internalBuilder().toString();
    }

    @Override
    public ExplainRequest request() {
        if (sourceBuilder != null) {
            request.source(sourceBuilder());
        }
        return request;
    }

    @Override
    protected void doExecute(ActionListener<ExplainResponse> listener) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder());
        }
        client.explain(request, listener);
    }

    private ExplainSourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new ExplainSourceBuilder();
        }
        return sourceBuilder;
    }

}
