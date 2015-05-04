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

package org.elasticsearch.action.count;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * A count action request builder.
 */
public class CountRequestBuilder extends BroadcastOperationRequestBuilder<CountRequest, CountResponse, CountRequestBuilder, Client> {

    private QuerySourceBuilder sourceBuilder;

    public CountRequestBuilder(Client client) {
        super(client, new CountRequest());
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public CountRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * The minimum score of the documents to include in the count. Defaults to <tt>-1</tt> which means all
     * documents will be included in the count.
     */
    public CountRequestBuilder setMinScore(float minScore) {
        request.minScore(minScore);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public CountRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards,
     * _shards:x,y to operate on shards x & y, or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public CountRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public CountRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The query source to execute.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public CountRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().setQuery(queryBuilder);
        return this;
    }

    /**
     * The query binary to execute
     */
    public CountRequestBuilder setQuery(BytesReference queryBinary) {
        sourceBuilder().setQuery(queryBinary);
        return this;
    }
    
    /**
     * Constructs a new builder with a raw search query.
     */
    public CountRequestBuilder setQuery(XContentBuilder query) {
        return setQuery(query.bytes());
    }


    /**
     * The source to execute.
     */
    public CountRequestBuilder setSource(BytesReference source) {
        request().source(source);
        return this;
    }

    /**
     * The query source to execute.
     */
    public CountRequestBuilder setSource(byte[] querySource) {
        request.source(querySource);
        return this;
    }

    public CountRequestBuilder setTerminateAfter(int terminateAfter) {
        request().terminateAfter(terminateAfter);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<CountResponse> listener) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }

        client.count(request, listener);
    }

    private QuerySourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new QuerySourceBuilder();
        }
        return sourceBuilder;
    }

    @Override
    public String toString() {
        if (sourceBuilder != null) {
            return sourceBuilder.toString();
        }
        if (request.source() != null) {
            try {
                return XContentHelper.convertToJson(request.source().toBytesArray(), false, true);
            } catch(Exception e) {
                return "{ \"error\" : \"" + ExceptionsHelper.detailedMessage(e) + "\"}";
            }
        }
        return new QuerySourceBuilder().toString();
    }
}
