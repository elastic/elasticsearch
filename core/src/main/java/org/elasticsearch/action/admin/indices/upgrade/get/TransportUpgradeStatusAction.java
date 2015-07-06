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

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class TransportUpgradeStatusAction extends TransportBroadcastAction<UpgradeStatusRequest, UpgradeStatusResponse, TransportUpgradeStatusAction.IndexShardUpgradeStatusRequest, ShardUpgradeStatus> {

    private final IndicesService indicesService;

    @Inject
    public TransportUpgradeStatusAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                        IndicesService indicesService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, UpgradeStatusAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                UpgradeStatusRequest.class, IndexShardUpgradeStatusRequest.class, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    /**
     * Getting upgrade stats from *all* active shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, UpgradeStatusRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allActiveShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, UpgradeStatusRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, UpgradeStatusRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected UpgradeStatusResponse newResponse(UpgradeStatusRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        final List<ShardUpgradeStatus> shards = newArrayList();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                shards.add((ShardUpgradeStatus) shardResponse);
                successfulShards++;
            }
        }
        return new UpgradeStatusResponse(shards.toArray(new ShardUpgradeStatus[shards.size()]), shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected IndexShardUpgradeStatusRequest newShardRequest(int numShards, ShardRouting shard, UpgradeStatusRequest request) {
        return new IndexShardUpgradeStatusRequest(shard.shardId(), request);
    }

    @Override
    protected ShardUpgradeStatus newShardResponse() {
        return new ShardUpgradeStatus();
    }

    @Override
    protected ShardUpgradeStatus shardOperation(IndexShardUpgradeStatusRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.shardSafe(request.shardId().id());
        List<Segment> segments = indexShard.engine().segments(false);
        long total_bytes = 0;
        long to_upgrade_bytes = 0;
        long to_upgrade_bytes_ancient = 0;
        for (Segment seg : segments) {
            total_bytes += seg.sizeInBytes;
            if (seg.version.major != Version.CURRENT.luceneVersion.major) {
                to_upgrade_bytes_ancient += seg.sizeInBytes;
                to_upgrade_bytes += seg.sizeInBytes;
            } else if (seg.version.minor != Version.CURRENT.luceneVersion.minor) {
                // TODO: this comparison is bogus! it would cause us to upgrade even with the same format
                // instead, we should check if the codec has changed
                to_upgrade_bytes += seg.sizeInBytes;
            }
        }

        return new ShardUpgradeStatus(indexShard.routingEntry(), total_bytes, to_upgrade_bytes, to_upgrade_bytes_ancient);
    }

    static class IndexShardUpgradeStatusRequest extends BroadcastShardRequest {

        IndexShardUpgradeStatusRequest() {

        }

        IndexShardUpgradeStatusRequest(ShardId shardId, UpgradeStatusRequest request) {
            super(shardId, request);
        }

    }
}
