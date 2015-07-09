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

package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Request builder for {@link IndicesShardsStoresRequest}
 */
public class IndicesShardsStoresRequestBuilder extends MasterNodeReadOperationRequestBuilder<IndicesShardsStoresRequest, IndicesShardsStoresResponse, IndicesShardsStoresRequestBuilder> {

    public IndicesShardsStoresRequestBuilder(ElasticsearchClient client, Action<IndicesShardsStoresRequest, IndicesShardsStoresResponse, IndicesShardsStoresRequestBuilder> action, String... indices) {
        super(client, action, new IndicesShardsStoresRequest(indices));
    }

    /**
     * Sets the indices for the shard stores request
     */
    public IndicesShardsStoresRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * By default, expands wildcards to both open and closed indices
     */
    public IndicesShardsStoresRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Set statuses to filter shards to get stores info on.
     * @param shardStatuses acceptable values are "green", "yellow", "red" and "all"
     * see {@link IndicesShardsStoresRequest.Status} for details
     */
    public IndicesShardsStoresRequestBuilder setShardStatuses(String... shardStatuses) {
        request.shardStatuses(shardStatuses);
        return this;
    }
}
