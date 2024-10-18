/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.action.cache;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING;

public class TransportSearchableSnapshotCacheStoresAction extends TransportNodesAction<
    TransportSearchableSnapshotCacheStoresAction.Request,
    TransportSearchableSnapshotCacheStoresAction.NodesCacheFilesMetadata,
    TransportSearchableSnapshotCacheStoresAction.NodeRequest,
    TransportSearchableSnapshotCacheStoresAction.NodeCacheFilesMetadata,
    Void> {

    public static final String ACTION_NAME = "internal:admin/xpack/searchable_snapshots/cache/store";

    public static final ActionType<NodesCacheFilesMetadata> TYPE = new ActionType<>(ACTION_NAME);

    private final CacheService cacheService;

    @Inject
    public TransportSearchableSnapshotCacheStoresAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        SearchableSnapshots.CacheServiceSupplier cacheService,
        ActionFilters actionFilters
    ) {
        super(
            ACTION_NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.cacheService = cacheService.get();
    }

    @Override
    protected NodesCacheFilesMetadata newResponse(
        Request request,
        List<NodeCacheFilesMetadata> nodesCacheFilesMetadata,
        List<FailedNodeException> failures
    ) {
        return new NodesCacheFilesMetadata(clusterService.getClusterName(), nodesCacheFilesMetadata, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeCacheFilesMetadata newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeCacheFilesMetadata(in);
    }

    @Override
    protected NodeCacheFilesMetadata nodeOperation(NodeRequest request, Task task) {
        assert cacheService != null;
        assert Optional.ofNullable(clusterService.state().metadata().index(request.shardId.getIndex()))
            .map(indexMetadata -> SNAPSHOT_PARTIAL_SETTING.get(indexMetadata.getSettings()))
            .orElse(false) == false : request.shardId + " is partial, should not be fetching its cached size";
        return new NodeCacheFilesMetadata(clusterService.localNode(), cacheService.getCachedSize(request.shardId, request.snapshotId));
    }

    public static final class Request extends BaseNodesRequest {

        private final SnapshotId snapshotId;
        private final ShardId shardId;

        public Request(SnapshotId snapshotId, ShardId shardId, DiscoveryNode[] nodes) {
            super(nodes);
            this.snapshotId = snapshotId;
            this.shardId = shardId;
        }
    }

    public static final class NodeRequest extends TransportRequest {

        private final SnapshotId snapshotId;
        private final ShardId shardId;

        public NodeRequest(Request request) {
            this.snapshotId = request.snapshotId;
            this.shardId = request.shardId;
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.snapshotId = new SnapshotId(in);
            this.shardId = new ShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshotId.writeTo(out);
            shardId.writeTo(out);
        }
    }

    public static class NodeCacheFilesMetadata extends BaseNodeResponse {

        private final long bytesCached;

        public NodeCacheFilesMetadata(StreamInput in) throws IOException {
            super(in);
            bytesCached = in.readLong();
        }

        public NodeCacheFilesMetadata(DiscoveryNode node, long bytesCached) {
            super(node);
            this.bytesCached = bytesCached;
        }

        public long bytesCached() {
            return bytesCached;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(bytesCached);
        }
    }

    public static class NodesCacheFilesMetadata extends BaseNodesResponse<NodeCacheFilesMetadata> {
        public NodesCacheFilesMetadata(ClusterName clusterName, List<NodeCacheFilesMetadata> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeCacheFilesMetadata> readNodesFrom(StreamInput in) {
            return TransportAction.localOnly();
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeCacheFilesMetadata> nodes) {
            TransportAction.localOnly();
        }
    }
}
