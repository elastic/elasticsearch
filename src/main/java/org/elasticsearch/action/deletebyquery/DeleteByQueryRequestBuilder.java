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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.replication.IndicesReplicationOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

/**
 *
 */
public class DeleteByQueryRequestBuilder extends IndicesReplicationOperationRequestBuilder<DeleteByQueryRequest, DeleteByQueryResponse, DeleteByQueryRequestBuilder> {

    private QuerySourceBuilder sourceBuilder;

    public DeleteByQueryRequestBuilder(Client client) {
        super(client, new DeleteByQueryRequest());
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public DeleteByQueryRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the action will be executed on.
     */
    public DeleteByQueryRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the action will be executed on.
     */
    public DeleteByQueryRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }


    /**
     * The query to delete documents for.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public DeleteByQueryRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().setQuery(queryBuilder);
        return this;
    }

    /**
     * The source to execute. It is preferable to use either {@link #setSource(byte[])}
     * or {@link #setQuery(QueryBuilder)}.
     */
    public DeleteByQueryRequestBuilder setSource(String source) {
        request().source(source);
        return this;
    }

    /**
     * The source to execute in the form of a map.
     */
    public DeleteByQueryRequestBuilder setSource(Map<String, Object> source) {
        request().source(source);
        return this;
    }

    /**
     * The source to execute in the form of a builder.
     */
    public DeleteByQueryRequestBuilder setSource(XContentBuilder builder) {
        request().source(builder);
        return this;
    }

    /**
     * The source to execute.
     */
    public DeleteByQueryRequestBuilder setSource(byte[] source) {
        request().source(source);
        return this;
    }

    /**
     * The source to execute.
     */
    public DeleteByQueryRequestBuilder setSource(BytesReference source) {
        request().source(source);
        return this;
    }

    /**
     * The source to execute.
     */
    public DeleteByQueryRequestBuilder setSource(byte[] source, int offset, int length) {
        request().source(source, offset, length);
        return this;
    }

    @Override
    public DeleteByQueryRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteByQueryResponse> listener) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }

        client.deleteByQuery(request, listener);
    }

    private QuerySourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new QuerySourceBuilder();
        }
        return sourceBuilder;
    }
}
