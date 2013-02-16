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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * A count action request builder.
 */
public class CountRequestBuilder extends BroadcastOperationRequestBuilder<CountRequest, CountResponse, CountRequestBuilder> {

    public CountRequestBuilder(Client client) {
        super((InternalClient) client, new CountRequest());
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public CountRequestBuilder setTypes(String... types) {
        request.setTypes(types);
        return this;
    }

    /**
     * The minimum score of the documents to include in the count. Defaults to <tt>-1</tt> which means all
     * documents will be included in the count.
     */
    public CountRequestBuilder setMinScore(float minScore) {
        request.setMinScore(minScore);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public CountRequestBuilder setRouting(String routing) {
        request.setRouting(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public CountRequestBuilder setRouting(String... routing) {
        request.setRouting(routing);
        return this;
    }

    /**
     * The query source to execute.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public CountRequestBuilder setQuery(QueryBuilder queryBuilder) {
        request.setQuery(queryBuilder);
        return this;
    }

    /**
     * The query source to execute.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public CountRequestBuilder setQuery(BytesReference querySource) {
        request.setQuery(querySource, false);
        return this;
    }

    /**
     * The query source to execute.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public CountRequestBuilder setQuery(BytesReference querySource, boolean unsafe) {
        request.setQuery(querySource, unsafe);
        return this;
    }

    /**
     * The query source to execute.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public CountRequestBuilder setQuery(byte[] querySource) {
        request.setQuery(querySource);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<CountResponse> listener) {
        ((InternalClient) client).count(request, listener);
    }
}
