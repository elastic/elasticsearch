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

package org.elasticsearch.action.terms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.FilterBuilder;

/**
 * A terms by query action request builder.  This is an internal api.
 */
public class TermsByQueryRequestBuilder extends BroadcastOperationRequestBuilder<TermsByQueryRequest, TermsByQueryResponse, TermsByQueryRequestBuilder> {

    public TermsByQueryRequestBuilder(Client client) {
        super((InternalClient) client, new TermsByQueryRequest());
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public TermsByQueryRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * The minimum score of the documents to include in the terms by query. Defaults to <tt>-1</tt> which means all documents will be
     * included in the terms by query.
     */
    public TermsByQueryRequestBuilder setMinScore(float minScore) {
        request.minScore(minScore);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public TermsByQueryRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to <tt>_local</tt> to prefer local
     * shards, <tt>_primary</tt> to execute only on primary shards, _shards:x,y to operate on shards x & y, or a custom value, which
     * guarantees that the same order will be used across different requests.
     */
    public TermsByQueryRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public TermsByQueryRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The field to extract terms from.
     */
    public TermsByQueryRequestBuilder setField(String field) {
        request.field(field);
        return this;
    }

    /**
     * The filter source to execute.
     *
     * @see org.elasticsearch.index.query.FilterBuilders
     */
    public TermsByQueryRequestBuilder setFilter(FilterBuilder filterBuilder) {
        request.filter(filterBuilder);
        return this;
    }

    /**
     * The filter source to execute.
     */
    public TermsByQueryRequestBuilder setFilter(BytesReference filterSource) {
        request.filter(filterSource, false);
        return this;
    }

    /**
     * The filter source to execute.
     */
    public TermsByQueryRequestBuilder setFilter(BytesReference filterSource, boolean unsafe) {
        request.filter(filterSource, unsafe);
        return this;
    }

    /**
     * The filter source to execute.
     */
    public TermsByQueryRequestBuilder setFilter(byte[] filterSource) {
        request.filter(filterSource);
        return this;
    }

    /**
     * If we should use a bloom filter to gather terms
     */
    public TermsByQueryRequestBuilder setUseBloomFilter(boolean useBloomFilter) {
        request.useBloomFilter(useBloomFilter);
        return this;
    }

    /**
     * The BloomFilter false positive probability
     */
    public TermsByQueryRequestBuilder setBloomFpp(double bloomFpp) {
        request.bloomFpp(bloomFpp);
        return this;
    }

    /**
     * The expected insertions for the BloomFilter
     */
    public TermsByQueryRequestBuilder setExpectedInsertions(int bloomExpectedInsertions) {
        request.bloomExpectedInsertions(bloomExpectedInsertions);
        return this;
    }

    /**
     * The number of hashes to use in the BloomFilter
     */
    public TermsByQueryRequestBuilder setBloomHashFunctions(int bloomHashFunctions) {
        request.bloomHashFunctions(bloomHashFunctions);
        return this;
    }

    /**
     * The max number of terms collected per shard
     */
    public TermsByQueryRequestBuilder setMaxTermsPerShard(long maxTermsPerShard) {
        request.maxTermsPerShard(maxTermsPerShard);
        return this;
    }

    /**
     * Executes the the request
     */
    @Override
    protected void doExecute(ActionListener<TermsByQueryResponse> listener) {
        ((InternalClient) client).execute(TermsByQueryAction.INSTANCE, request, listener);
    }
}
