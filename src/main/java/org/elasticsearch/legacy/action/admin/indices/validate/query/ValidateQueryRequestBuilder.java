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

package org.elasticsearch.legacy.action.admin.indices.validate.query;

import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.QuerySourceBuilder;
import org.elasticsearch.legacy.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.legacy.client.IndicesAdminClient;
import org.elasticsearch.legacy.common.bytes.BytesReference;
import org.elasticsearch.legacy.index.query.QueryBuilder;

/**
 *
 */
public class ValidateQueryRequestBuilder extends BroadcastOperationRequestBuilder<ValidateQueryRequest, ValidateQueryResponse, ValidateQueryRequestBuilder, IndicesAdminClient> {

    private QuerySourceBuilder sourceBuilder;

    public ValidateQueryRequestBuilder(IndicesAdminClient client) {
        super(client, new ValidateQueryRequest());
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public ValidateQueryRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * The query source to validate.
     *
     * @see org.elasticsearch.legacy.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().setQuery(queryBuilder);
        return this;
    }

    /**
     * The source to validate.
     *
     * @see org.elasticsearch.legacy.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setSource(BytesReference source) {
        request().source(source, false);
        return this;
    }

    /**
     * The source to validate.
     *
     * @see org.elasticsearch.legacy.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setSource(BytesReference source, boolean unsafe) {
        request().source(source, unsafe);
        return this;
    }

    /**
     * The source to validate.
     *
     * @see org.elasticsearch.legacy.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    /**
     * Indicates if detailed information about the query should be returned.
     *
     * @see org.elasticsearch.legacy.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setExplain(boolean explain) {
        request.explain(explain);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<ValidateQueryResponse> listener) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }

        client.validateQuery(request, listener);
    }

    private QuerySourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new QuerySourceBuilder();
        }
        return sourceBuilder;
    }
}
