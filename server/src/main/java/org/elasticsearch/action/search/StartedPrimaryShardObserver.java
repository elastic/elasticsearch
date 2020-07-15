/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;

class StartedPrimaryShardObserver {
    private final Logger logger = LogManager.getLogger(StartedPrimaryShardObserver.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    StartedPrimaryShardObserver(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    void waitUntilPrimaryShardIsStarted(SearchShardIterator searchShardIterator,
                                        TimeValue timeout,
                                        ActionListener<SearchShardIterator> listener) {
        final ShardId shardId = searchShardIterator.shardId();
        ClusterState state = clusterService.state();

        if (isPrimaryShardStarted(state, shardId)) {
            listener.onResponse(createUpdatedShardIterator(searchShardIterator, state));
            return;
        }

        final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());

        clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState newClusterState) {
                SearchShardIterator updatedShardIterator = createUpdatedShardIterator(searchShardIterator, newClusterState);
                listener.onResponse(updatedShardIterator);
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onFailure(new PrimaryShardStartTimeout());
            }
        }, clusterState -> isPrimaryShardStarted(clusterState, shardId), timeout);
    }

    private SearchShardIterator createUpdatedShardIterator(SearchShardIterator searchShardIterator, ClusterState clusterState) {
        ShardId shardId = searchShardIterator.shardId();

        ShardRouting primaryShard = clusterState.getRoutingTable().shardRoutingTable(shardId).primaryShard();
        assert primaryShard != null && primaryShard.started();

        return new SearchShardIterator(searchShardIterator.getClusterAlias(),
            shardId,
            Collections.singletonList(primaryShard),
            searchShardIterator.getOriginalIndices());
    }

    private boolean isPrimaryShardStarted(ClusterState clusterState, ShardId shardId) {
        IndexRoutingTable indexRouting = clusterState.getRoutingTable().index(shardId.getIndex());

        if (indexRouting == null) {
            return false;
        }

        IndexShardRoutingTable shardRoutingTable = indexRouting.shard(shardId.getId());

        return shardRoutingTable != null && shardRoutingTable.primaryShard() != null && shardRoutingTable.primaryShard().started();
    }

    private static final class PrimaryShardStartTimeout extends RuntimeException {}
}
