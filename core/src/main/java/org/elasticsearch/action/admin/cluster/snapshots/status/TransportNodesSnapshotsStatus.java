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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Transport client that collects snapshot shard statuses from data nodes
 */
public class TransportNodesSnapshotsStatus extends TransportNodesAction<TransportNodesSnapshotsStatus.Request, TransportNodesSnapshotsStatus.NodesSnapshotStatus, TransportNodesSnapshotsStatus.NodeRequest, TransportNodesSnapshotsStatus.NodeSnapshotStatus> {

    public static final String ACTION_NAME = SnapshotsStatusAction.NAME + "[nodes]";

    private final SnapshotShardsService snapshotShardsService;

    @Inject
    public TransportNodesSnapshotsStatus(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                         ClusterService clusterService, TransportService transportService,
                                         SnapshotShardsService snapshotShardsService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, clusterName, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                Request::new, NodeRequest::new, ThreadPool.Names.GENERIC);
        this.snapshotShardsService = snapshotShardsService;
    }

    @Override
    protected boolean transportCompress() {
        return true; // compress since the metadata can become large
    }

    @Override
    protected NodeRequest newNodeRequest(String nodeId, Request request) {
        return new NodeRequest(nodeId, request);
    }

    @Override
    protected NodeSnapshotStatus newNodeResponse() {
        return new NodeSnapshotStatus();
    }

    @Override
    protected NodesSnapshotStatus newResponse(Request request, AtomicReferenceArray responses) {
        final List<NodeSnapshotStatus> nodesList = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeSnapshotStatus) { // will also filter out null response for unallocated ones
                nodesList.add((NodeSnapshotStatus) resp);
            } else if (resp instanceof FailedNodeException) {
                failures.add((FailedNodeException) resp);
            } else {
                logger.warn("unknown response type [{}], expected NodeSnapshotStatus or FailedNodeException", resp);
            }
        }
        return new NodesSnapshotStatus(clusterName, nodesList.toArray(new NodeSnapshotStatus[nodesList.size()]),
                failures.toArray(new FailedNodeException[failures.size()]));
    }

    @Override
    protected NodeSnapshotStatus nodeOperation(NodeRequest request) {
        ImmutableMap.Builder<SnapshotId, ImmutableMap<ShardId, SnapshotIndexShardStatus>> snapshotMapBuilder = ImmutableMap.builder();
        try {
            String nodeId = clusterService.localNode().id();
            for (SnapshotId snapshotId : request.snapshotIds) {
                Map<ShardId, IndexShardSnapshotStatus> shardsStatus = snapshotShardsService.currentSnapshotShards(snapshotId);
                if (shardsStatus == null) {
                    continue;
                }
                ImmutableMap.Builder<ShardId, SnapshotIndexShardStatus> shardMapBuilder = ImmutableMap.builder();
                for (Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : shardsStatus.entrySet()) {
                    SnapshotIndexShardStatus shardStatus;
                    IndexShardSnapshotStatus.Stage stage = shardEntry.getValue().stage();
                    if (stage != IndexShardSnapshotStatus.Stage.DONE && stage != IndexShardSnapshotStatus.Stage.FAILURE) {
                        // Store node id for the snapshots that are currently running.
                        shardStatus = new SnapshotIndexShardStatus(shardEntry.getKey(), shardEntry.getValue(), nodeId);
                    } else {
                        shardStatus = new SnapshotIndexShardStatus(shardEntry.getKey(), shardEntry.getValue());
                    }
                    shardMapBuilder.put(shardEntry.getKey(), shardStatus);
                }
                snapshotMapBuilder.put(snapshotId, shardMapBuilder.build());
            }
            return new NodeSnapshotStatus(clusterService.localNode(), snapshotMapBuilder.build());
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load metadata", e);
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }

    public static class Request extends BaseNodesRequest<Request> {

        private SnapshotId[] snapshotIds;

        public Request() {
        }

        public Request(ActionRequest request, String[] nodesIds) {
            super(request, nodesIds);
        }

        public Request snapshotIds(SnapshotId[] snapshotIds) {
            this.snapshotIds = snapshotIds;
            return this;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            // This operation is never executed remotely
            throw new UnsupportedOperationException("shouldn't be here");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // This operation is never executed remotely
            throw new UnsupportedOperationException("shouldn't be here");
        }
    }

    public static class NodesSnapshotStatus extends BaseNodesResponse<NodeSnapshotStatus> {

        private FailedNodeException[] failures;

        NodesSnapshotStatus() {
        }

        public NodesSnapshotStatus(ClusterName clusterName, NodeSnapshotStatus[] nodes, FailedNodeException[] failures) {
            super(clusterName, nodes);
            this.failures = failures;
        }

        public FailedNodeException[] failures() {
            return failures;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            nodes = new NodeSnapshotStatus[in.readVInt()];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = new NodeSnapshotStatus();
                nodes[i].readFrom(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(nodes.length);
            for (NodeSnapshotStatus response : nodes) {
                response.writeTo(out);
            }
        }
    }


    public static class NodeRequest extends BaseNodeRequest {

        private SnapshotId[] snapshotIds;

        public NodeRequest() {
        }

        NodeRequest(String nodeId, TransportNodesSnapshotsStatus.Request request) {
            super(request, nodeId);
            snapshotIds = request.snapshotIds;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            int n = in.readVInt();
            snapshotIds = new SnapshotId[n];
            for (int i = 0; i < n; i++) {
                snapshotIds[i] = SnapshotId.readSnapshotId(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (snapshotIds != null) {
                out.writeVInt(snapshotIds.length);
                for (int i = 0; i < snapshotIds.length; i++) {
                    snapshotIds[i].writeTo(out);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }

    public static class NodeSnapshotStatus extends BaseNodeResponse {

        private ImmutableMap<SnapshotId, ImmutableMap<ShardId, SnapshotIndexShardStatus>> status;

        NodeSnapshotStatus() {
        }

        public NodeSnapshotStatus(DiscoveryNode node, ImmutableMap<SnapshotId, ImmutableMap<ShardId, SnapshotIndexShardStatus>> status) {
            super(node);
            this.status = status;
        }

        public ImmutableMap<SnapshotId, ImmutableMap<ShardId, SnapshotIndexShardStatus>> status() {
            return status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            int numberOfSnapshots = in.readVInt();
            ImmutableMap.Builder<SnapshotId, ImmutableMap<ShardId, SnapshotIndexShardStatus>> snapshotMapBuilder = ImmutableMap.builder();
            for (int i = 0; i < numberOfSnapshots; i++) {
                SnapshotId snapshotId = SnapshotId.readSnapshotId(in);
                ImmutableMap.Builder<ShardId, SnapshotIndexShardStatus> shardMapBuilder = ImmutableMap.builder();
                int numberOfShards = in.readVInt();
                for (int j = 0; j < numberOfShards; j++) {
                    ShardId shardId =  ShardId.readShardId(in);
                    SnapshotIndexShardStatus status = SnapshotIndexShardStatus.readShardSnapshotStatus(in);
                    shardMapBuilder.put(shardId, status);
                }
                snapshotMapBuilder.put(snapshotId, shardMapBuilder.build());
            }
            status = snapshotMapBuilder.build();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (status != null) {
                out.writeVInt(status.size());
                for (ImmutableMap.Entry<SnapshotId, ImmutableMap<ShardId, SnapshotIndexShardStatus>> entry : status.entrySet()) {
                    entry.getKey().writeTo(out);
                    out.writeVInt(entry.getValue().size());
                    for (ImmutableMap.Entry<ShardId, SnapshotIndexShardStatus> shardEntry : entry.getValue().entrySet()) {
                        shardEntry.getKey().writeTo(out);
                        shardEntry.getValue().writeTo(out);
                    }
                }
            } else {
                out.writeVInt(0);
            }
        }
    }
}
