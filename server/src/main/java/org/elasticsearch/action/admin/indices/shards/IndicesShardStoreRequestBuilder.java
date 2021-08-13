/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;

/**
 * Request builder for {@link IndicesShardStoresRequest}
 */
public class IndicesShardStoreRequestBuilder extends MasterNodeReadOperationRequestBuilder<
        IndicesShardStoresRequest,
        IndicesShardStoresResponse,
        IndicesShardStoreRequestBuilder> {

    public IndicesShardStoreRequestBuilder(ElasticsearchClient client, ActionType<IndicesShardStoresResponse> action, String... indices) {
        super(client, action, new IndicesShardStoresRequest(indices));
    }

    /**
     * Sets the indices for the shard stores request
     */
    public IndicesShardStoreRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * By default, expands wildcards to both open and closed indices
     */
    public IndicesShardStoreRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Set statuses to filter shards to get stores info on.
     * @param shardStatuses acceptable values are "green", "yellow", "red" and "all"
     * see {@link ClusterHealthStatus} for details
     */
    public IndicesShardStoreRequestBuilder setShardStatuses(String... shardStatuses) {
        request.shardStatuses(shardStatuses);
        return this;
    }
}
