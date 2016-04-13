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

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Creates a new {@link DeleteByQueryRequestBuilder}
 * @see DeleteByQueryRequest
 */
public class DeleteByQueryRequestBuilder extends ActionRequestBuilder<DeleteByQueryRequest, DeleteByQueryResponse, DeleteByQueryRequestBuilder> {

    public DeleteByQueryRequestBuilder(ElasticsearchClient client, DeleteByQueryAction action) {
        super(client, action, new DeleteByQueryRequest());
    }

    public DeleteByQueryRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public DeleteByQueryRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * The query used to delete documents.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public DeleteByQueryRequestBuilder setQuery(QueryBuilder<?> queryBuilder) {
        request.query(queryBuilder);
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
     * An optional timeout to control how long the delete by query is allowed to take.
     */
    public DeleteByQueryRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * An optional timeout to control how long the delete by query is allowed to take.
     */
    public DeleteByQueryRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public DeleteByQueryRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

}
