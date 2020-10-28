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
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

final class ClusterStateSearchShardRefresher implements SearchShardIteratorRefresher {
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ThreadContext threadContext;
    private final Logger logger = LogManager.getLogger(ClusterStateSearchShardRefresher.class);

    ClusterStateSearchShardRefresher(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.threadContext = threadPool.getThreadContext();
    }

    @Override
    public void refreshSearchTargets(RetryableSearchShardIterator searchShardIterator,
                                     TimeValue timeout,
                                     ActionListener<RetryableSearchShardIterator> listener) {
        ShardId shardId = searchShardIterator.shardId();
        RoutingTable routingTable = getRoutingTable();

        if (routingTable.hasIndex(shardId.getIndex()) == false) {
            listener.onFailure(new IndexNotFoundException(shardId.getIndex()));
            return;
        }

        if (isIteratorStillValid(routingTable, searchShardIterator)) {
            searchShardIterator.reset();
            // Fork to avoid stack overflows
            threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> listener.onResponse(searchShardIterator));
            return;
        }

        ClusterState state = clusterService.state();
        if (isPrimaryActive(state, shardId)) {
            doRefresh(searchShardIterator, listener);
        } else {
            ClusterStateObserver clusterStateObserver = new ClusterStateObserver(state, clusterService, timeout, logger, threadContext);
            clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState clusterState) {
                    doRefresh(searchShardIterator, listener);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new RuntimeException("Unable to get a primary after timeout"));
                }
            }, clusterState -> isPrimaryActive(clusterState, shardId), TimeValue.timeValueSeconds(2));
        }
    }

    private boolean isPrimaryActive(ClusterState clusterState, ShardId shardId) {
        RoutingTable routingTable = clusterState.routingTable();
        IndexRoutingTable index = routingTable.index(shardId.getIndex());
        if (index == null) {
            return false;
        }
        IndexShardRoutingTable shardRoutingTable = routingTable.shardRoutingTable(shardId);
        return shardRoutingTable.primaryShard() != null && shardRoutingTable.primaryShard().started();
    }

    private void doRefresh(RetryableSearchShardIterator searchShardIterator, ActionListener<RetryableSearchShardIterator> listener) {
        ShardId shardId = searchShardIterator.shardId();
        RoutingTable routingTable = clusterService.state().routingTable();
        if (routingTable.hasIndex(shardId.getIndex()) == false) {
            listener.onFailure(new IndexNotFoundException(shardId.getIndex()));
            return;
        }

        IndexRoutingTable indexRoutingTable = routingTable.index(shardId.getIndex());
        List<ShardRouting> shardRoutings = indexRoutingTable.shard(shardId.getId()).activeShards();
        listener.onResponse(searchShardIterator.withUpdatedRoutings(shardRoutings));
    }

    private boolean isIteratorStillValid(RoutingTable routingTable, RetryableSearchShardIterator searchShardIterator) {
        searchShardIterator.reset();
        SearchShardTarget searchShardTarget;
        if (routingTable.hasIndex(searchShardIterator.shardId().getIndex()) == false || searchShardIterator.size() == 0) {
            return false;
        }

        while ((searchShardTarget = searchShardIterator.nextOrNull()) != null) {
            if (isShardTargetStale(searchShardTarget, routingTable)) {
                return false;
            }
        }
        return true;
    }

    private boolean isShardTargetStale(SearchShardTarget searchShardTarget, RoutingTable routingTable) {
        ShardRouting shardRouting = routingTable.getByAllocationId(searchShardTarget.getShardId(),
                                                                   searchShardTarget.getShardRouting().allocationId().getId());
        return shardRouting == null ||
            shardRouting.currentNodeId().equals(searchShardTarget.getNodeId()) == false ||
            shardRouting.relocating();
    }

    private RoutingTable getRoutingTable() {
        return clusterService.state().getRoutingTable();
    }
}
