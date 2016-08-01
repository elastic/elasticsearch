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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Transport client that collects snapshot shard statuses from data nodes
 */
public class TransportNodesSnapshotsStatus extends TransportNodesAction<TransportNodesSnapshotsStatus.Request,
                                                                        TransportNodesSnapshotsStatus.NodesSnapshotStatus,
                                                                        TransportNodesSnapshotsStatus.NodeRequest,
                                                                        TransportNodesSnapshotsStatus.NodeSnapshotStatus> {

    public static final String ACTION_NAME = SnapshotsStatusAction.NAME + "[nodes]";

    private final SnapshotShardsService snapshotShardsService;

    @Inject
    public TransportNodesSnapshotsStatus(Settings settings, ThreadPool threadPool,
                                         ClusterService clusterService, TransportService transportService,
                                         SnapshotShardsService snapshotShardsService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
              Request::new, NodeRequest::new, ThreadPool.Names.GENERIC, NodeSnapshotStatus.class);
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
    protected NodesSnapshotStatus newResponse(Request request, List<NodeSnapshotStatus> responses, List<FailedNodeException> failures) {
        return new NodesSnapshotStatus(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeSnapshotStatus nodeOperation(NodeRequest request) {
        Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> snapshotMapBuilder = new HashMap<>();
        try {
            String nodeId = clusterService.localNode().getId();
            for (Snapshot snapshot : request.snapshots) {
                Map<ShardId, IndexShardSnapshotStatus> shardsStatus = snapshotShardsService.currentSnapshotShards(snapshot);
                if (shardsStatus == null) {
                    continue;
                }
                Map<ShardId, SnapshotIndexShardStatus> shardMapBuilder = new HashMap<>();
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
                snapshotMapBuilder.put(snapshot, unmodifiableMap(shardMapBuilder));
            }
            return new NodeSnapshotStatus(clusterService.localNode(), unmodifiableMap(snapshotMapBuilder));
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load metadata", e);
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }

    public static class Request extends BaseNodesRequest<Request> {

        private Snapshot[] snapshots;

        public Request() {
        }

        public Request(String[] nodesIds) {
            super(nodesIds);
        }

        public Request snapshots(Snapshot[] snapshots) {
            this.snapshots = snapshots;
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

        public NodesSnapshotStatus(ClusterName clusterName, List<NodeSnapshotStatus> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeSnapshotStatus> readNodesFrom(StreamInput in) throws IOException {
            return in.readStreamableList(NodeSnapshotStatus::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeSnapshotStatus> nodes) throws IOException {
            out.writeStreamableList(nodes);
        }
    }


    public static class NodeRequest extends BaseNodeRequest {

        private List<Snapshot> snapshots;

        public NodeRequest() {
        }

        NodeRequest(String nodeId, TransportNodesSnapshotsStatus.Request request) {
            super(nodeId);
            snapshots = Arrays.asList(request.snapshots);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshots = in.readList(Snapshot::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(snapshots);
        }
    }

    public static class NodeSnapshotStatus extends BaseNodeResponse {

        private Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status;

        NodeSnapshotStatus() {
        }

        public NodeSnapshotStatus(DiscoveryNode node, Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status) {
            super(node);
            this.status = status;
        }

        public Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status() {
            return status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            int numberOfSnapshots = in.readVInt();
            Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> snapshotMapBuilder = new HashMap<>(numberOfSnapshots);
            for (int i = 0; i < numberOfSnapshots; i++) {
                Snapshot snapshot = new Snapshot(in);
                int numberOfShards = in.readVInt();
                Map<ShardId, SnapshotIndexShardStatus> shardMapBuilder = new HashMap<>(numberOfShards);
                for (int j = 0; j < numberOfShards; j++) {
                    ShardId shardId =  ShardId.readShardId(in);
                    SnapshotIndexShardStatus status = SnapshotIndexShardStatus.readShardSnapshotStatus(in);
                    shardMapBuilder.put(shardId, status);
                }
                snapshotMapBuilder.put(snapshot, unmodifiableMap(shardMapBuilder));
            }
            status = unmodifiableMap(snapshotMapBuilder);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (status != null) {
                out.writeVInt(status.size());
                for (Map.Entry<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> entry : status.entrySet()) {
                    entry.getKey().writeTo(out);
                    out.writeVInt(entry.getValue().size());
                    for (Map.Entry<ShardId, SnapshotIndexShardStatus> shardEntry : entry.getValue().entrySet()) {
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
