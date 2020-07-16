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

class StartedPrimaryShardObserver {
    private final Logger logger = LogManager.getLogger(StartedPrimaryShardObserver.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    StartedPrimaryShardObserver(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    void waitUntilPrimaryShardIsStarted(ShardId shardId,
                                        TimeValue timeout,
                                        ActionListener<ShardRouting> listener) {
        ClusterState state = clusterService.state();

        if (primaryShardIsStarted(shardId, state)) {
            listener.onResponse(getPrimaryShard(shardId, state));
            return;
        }

        if (timeout.equals(TimeValue.ZERO)) {
            listener.onFailure(new PrimaryShardStartTimeout());
        }

        final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());

        clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState newClusterState) {
                listener.onResponse(getPrimaryShard(shardId, newClusterState));
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onFailure(new PrimaryShardStartTimeout());
            }

        }, clusterState -> primaryShardIsStarted(shardId, clusterState), timeout);
    }

    private ShardRouting getPrimaryShard(ShardId shardId, ClusterState clusterState) {
        assert primaryShardIsStarted(shardId, clusterState);

        return clusterState.getRoutingTable().shardRoutingTable(shardId).primaryShard();
    }

    private boolean primaryShardIsStarted(ShardId shardId, ClusterState clusterState) {
        IndexRoutingTable indexRouting = clusterState.getRoutingTable().index(shardId.getIndex());

        if (indexRouting == null) {
            return false;
        }

        IndexShardRoutingTable shardRoutingTable = indexRouting.shard(shardId.getId());

        return shardRoutingTable != null && shardRoutingTable.primaryShard() != null && shardRoutingTable.primaryShard().started();
    }

    static final class PrimaryShardStartTimeout extends RuntimeException {}
}
