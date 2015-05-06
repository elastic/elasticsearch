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

package org.elasticsearch.action.admin.indices.syncedflush;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SyncedFlushService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Synced flush action
 */
public class TransportSyncedFlushAction extends HandledTransportAction<SyncedFlushRequest, SyncedFlushResponse> {


    final private SyncedFlushService syncedFlushService;
    final private ClusterService clusterService;

    @Inject
    public TransportSyncedFlushAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters, SyncedFlushService syncedFlushService, ClusterService clusterService) {
        super(settings, SyncedFlushAction.NAME, threadPool, transportService, actionFilters, SyncedFlushRequest.class);
        this.syncedFlushService = syncedFlushService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(SyncedFlushRequest request, ActionListener<SyncedFlushResponse> listener) {

        int successfulShards = 0;
        int failedShards = 0;
        int totalShards = 0;
        ClusterState state = clusterService.state();
        List<ShardOperationFailedException> failures = new ArrayList<>();
        for (String index : request.indices()) {
            int numberOfShards = state.metaData().index(index).getSettings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0);
            totalShards += numberOfShards * state.metaData().index(index).getSettings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0);
            for (int i = 0; i < numberOfShards; i++) {
                SyncedFlushService.SyncedFlushResult syncedFlushResult = syncedFlushService.attemptSyncedFlush(new ShardId(index, i));
                successfulShards += syncedFlushResult.successfulShards();
                failedShards += syncedFlushResult.totalShards() - syncedFlushResult.successfulShards();
                if (syncedFlushResult.success() == false) {
                    failures.add(new SyncFlushFailedException(new ShardId(index, i), syncedFlushResult.failureReason(), "all nodes"));
                } else {
                    for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponse : syncedFlushResult.shardResponses().entrySet()) {
                        if (shardResponse.getValue().success() == false) {
                            failures.add(new SyncFlushFailedException(shardResponse.getKey().shardId(),
                                    shardResponse.getValue().failureReason(),
                                    state.getRoutingNodes().node(shardResponse.getKey().currentNodeId()).node().name()));
                        }
                    }
                }
            }
        }
        listener.onResponse(new SyncedFlushResponse(totalShards, successfulShards, failedShards, failures));
    }

    public static class SyncFlushFailedException implements ShardOperationFailedException {

        ShardId shardId;
        String reason;
        String nodeName;

        public SyncFlushFailedException(ShardId shardId, String reason, String nodeName) {
            this.shardId = shardId;
            this.reason = reason;
            this.nodeName = nodeName;
        }

        @Override
        public String index() {
            return shardId.index().name();
        }

        @Override
        public int shardId() {
            return shardId.id();
        }

        @Override
        public String reason() {
            return reason;
        }

        @Override
        public RestStatus status() {
            return RestStatus.NOT_ACCEPTABLE;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.shardId = ShardId.readShardId(in);
            this.reason = in.readString();
            this.nodeName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeString(reason);
            out.writeString(nodeName);
        }
    }
}
