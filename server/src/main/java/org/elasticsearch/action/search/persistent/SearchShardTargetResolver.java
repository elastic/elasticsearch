/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collections;

public interface SearchShardTargetResolver {
    void resolve(SearchShard shardSearchTarget, OriginalIndices originalIndices, ActionListener<SearchShardIterator> listener);

    class DefaultSearchShardTargetResolver implements SearchShardTargetResolver {
        private final ClusterService clusterService;

        public DefaultSearchShardTargetResolver(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public void resolve(SearchShard shardSearchTarget, OriginalIndices originalIndices, ActionListener<SearchShardIterator> listener) {
            final ShardId shardId = shardSearchTarget.getShardId();

            final IndexRoutingTable indexShardRoutingTable = clusterService.state().getRoutingTable().index(shardId.getIndex());
            if (indexShardRoutingTable == null) {
                // It will be retried
                listener.onResponse(new SearchShardIterator(null, shardId, Collections.emptyList(), originalIndices));
                return;
            }

            final IndexShardRoutingTable shardRoutingTable = indexShardRoutingTable.shard(shardId.getId());
            listener.onResponse(new SearchShardIterator(null, shardId, shardRoutingTable.activeShards(), originalIndices));
        }
    }
}
