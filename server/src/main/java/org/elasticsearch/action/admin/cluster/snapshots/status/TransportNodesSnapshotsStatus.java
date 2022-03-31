/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Transport action that collects snapshot shard statuses from data nodes
 */
public class TransportNodesSnapshotsStatus extends TransportNodesAction<
    TransportNodesSnapshotsStatus.Request,
    TransportNodesSnapshotsStatus.NodesSnapshotStatus,
    TransportNodesSnapshotsStatus.NodeRequest,
    TransportNodesSnapshotsStatus.NodeSnapshotStatus> {

    public static final String ACTION_NAME = SnapshotsStatusAction.NAME + "[nodes]";
    public static final ActionType<NodesSnapshotStatus> TYPE = new ActionType<>(ACTION_NAME, NodesSnapshotStatus::new);

    private final SnapshotShardsService snapshotShardsService;

    @Inject
    public TransportNodesSnapshotsStatus(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        SnapshotShardsService snapshotShardsService,
        ActionFilters actionFilters
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            NodeSnapshotStatus.class
        );
        this.snapshotShardsService = snapshotShardsService;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeSnapshotStatus newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeSnapshotStatus(in);
    }

    @Override
    protected NodesSnapshotStatus newResponse(Request request, List<NodeSnapshotStatus> responses, List<FailedNodeException> failures) {
        return new NodesSnapshotStatus(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeSnapshotStatus nodeOperation(NodeRequest request, Task task) {
        Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> snapshotMapBuilder = new HashMap<>();
        try {
            final String nodeId = clusterService.localNode().getId();
            for (Snapshot snapshot : request.snapshots) {
                Map<ShardId, IndexShardSnapshotStatus> shardsStatus = snapshotShardsService.currentSnapshotShards(snapshot);
                if (shardsStatus == null) {
                    continue;
                }
                Map<ShardId, SnapshotIndexShardStatus> shardMapBuilder = new HashMap<>();
                for (Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : shardsStatus.entrySet()) {
                    final ShardId shardId = shardEntry.getKey();

                    final IndexShardSnapshotStatus.Copy lastSnapshotStatus = shardEntry.getValue().asCopy();
                    final IndexShardSnapshotStatus.Stage stage = lastSnapshotStatus.getStage();

                    String shardNodeId = null;
                    if (stage != IndexShardSnapshotStatus.Stage.DONE && stage != IndexShardSnapshotStatus.Stage.FAILURE) {
                        // Store node id for the snapshots that are currently running.
                        shardNodeId = nodeId;
                    }
                    shardMapBuilder.put(shardEntry.getKey(), new SnapshotIndexShardStatus(shardId, lastSnapshotStatus, shardNodeId));
                }
                snapshotMapBuilder.put(snapshot, unmodifiableMap(shardMapBuilder));
            }
            return new NodeSnapshotStatus(clusterService.localNode(), unmodifiableMap(snapshotMapBuilder));
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load metadata", e);
        }
    }

    public static class Request extends BaseNodesRequest<Request> {

        private Snapshot[] snapshots;

        public Request(StreamInput in) throws IOException {
            super(in);
            // This operation is never executed remotely
            throw new UnsupportedOperationException("shouldn't be here");
        }

        public Request(String[] nodesIds) {
            super(nodesIds);
        }

        public Request snapshots(Snapshot[] snapshots) {
            this.snapshots = snapshots;
            return this;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // This operation is never executed remotely
            throw new UnsupportedOperationException("shouldn't be here");
        }
    }

    public static class NodesSnapshotStatus extends BaseNodesResponse<NodeSnapshotStatus> {

        public NodesSnapshotStatus(StreamInput in) throws IOException {
            super(in);
        }

        public NodesSnapshotStatus(ClusterName clusterName, List<NodeSnapshotStatus> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeSnapshotStatus> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeSnapshotStatus::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeSnapshotStatus> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class NodeRequest extends TransportRequest {

        private final List<Snapshot> snapshots;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            snapshots = in.readList(Snapshot::new);
        }

        NodeRequest(TransportNodesSnapshotsStatus.Request request) {
            snapshots = Arrays.asList(request.snapshots);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(snapshots);
        }
    }

    public static class NodeSnapshotStatus extends BaseNodeResponse {

        private final Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status;

        public NodeSnapshotStatus(StreamInput in) throws IOException {
            super(in);
            status = unmodifiableMap(
                in.readMap(Snapshot::new, input -> unmodifiableMap(input.readMap(ShardId::new, SnapshotIndexShardStatus::new)))
            );
        }

        public NodeSnapshotStatus(DiscoveryNode node, Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status) {
            super(node);
            this.status = status;
        }

        public Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status() {
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (status != null) {
                out.writeMap(
                    status,
                    (o, s) -> s.writeTo(o),
                    (output, v) -> output.writeMap(v, (o, shardId) -> shardId.writeTo(o), (o, sis) -> sis.writeTo(o))
                );
            } else {
                out.writeVInt(0);
            }
        }
    }
}
