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
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Node level stats about searchable snapshots caches.
 */
public class TransportSearchableSnapshotsNodeCachesStatsAction extends TransportNodesAction<
    TransportSearchableSnapshotsNodeCachesStatsAction.NodesRequest,
    TransportSearchableSnapshotsNodeCachesStatsAction.NodesCachesStatsResponse,
    TransportSearchableSnapshotsNodeCachesStatsAction.NodeRequest,
    TransportSearchableSnapshotsNodeCachesStatsAction.NodeCachesStatsResponse> {

    public static final String ACTION_NAME = "cluster:admin/xpack/searchable_snapshots/cache/stats";

    public static final ActionType<NodesCachesStatsResponse> TYPE = new ActionType<>(ACTION_NAME, NodesCachesStatsResponse::new);

    private final Supplier<CacheService> cacheService;
    private final Supplier<FrozenCacheService> frozenCacheService;

    @Inject
    public TransportSearchableSnapshotsNodeCachesStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        SearchableSnapshots.CacheServiceSupplier cacheService,
        SearchableSnapshots.FrozenCacheServiceSupplier frozenCacheService
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ThreadPool.Names.SAME,
            NodeCachesStatsResponse.class
        );
        this.cacheService = cacheService;
        this.frozenCacheService = frozenCacheService;
    }

    @Override
    protected NodesCachesStatsResponse newResponse(
        NodesRequest request,
        List<NodeCachesStatsResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesCachesStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(NodesRequest request) {
        return new NodeRequest();
    }

    @Override
    protected NodeCachesStatsResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeCachesStatsResponse(in);
    }

    @Override
    protected void resolveRequest(NodesRequest request, ClusterState clusterState) {
        final ImmutableOpenMap<String, DiscoveryNode> dataNodes = clusterState.getNodes().getDataNodes();

        final DiscoveryNode[] resolvedNodes;
        if (request.nodesIds() == null || request.nodesIds().length == 0) {
            resolvedNodes = dataNodes.values().toArray(DiscoveryNode.class);
        } else {
            resolvedNodes = Arrays.stream(request.nodesIds())
                .filter(dataNodes::containsKey)
                .map(dataNodes::get)
                .collect(Collectors.toList())
                .toArray(DiscoveryNode[]::new);
        }
        request.setConcreteNodes(resolvedNodes);
    }

    @Override
    protected NodeCachesStatsResponse nodeOperation(NodeRequest request, Task task) {
        final CacheService.Stats cacheStats = cacheService.get() != null ? cacheService.get().getStats() : null;
        final FrozenCacheService.Stats frozenCacheStats = frozenCacheService.get() != null ? frozenCacheService.get().getStats() : null;
        return new NodeCachesStatsResponse(
            clusterService.localNode(),
            cacheStats != null ? cacheStats.getHits() : 0L,
            cacheStats != null ? cacheStats.getMisses() : 0L,
            cacheStats != null ? cacheStats.getEvictions() : 0L,
            cacheStats != null ? cacheStats.getPersistentCacheDocs() : 0L,
            cacheStats != null ? cacheStats.getPersistentCacheDeletedDocs() : 0L,
            cacheStats != null ? cacheStats.getPersistentCacheIndexSizeInBytes() : 0L,
            frozenCacheStats != null ? frozenCacheStats.getRegionSize() : 0L,
            frozenCacheStats != null ? frozenCacheStats.getTotalRegions() : 0,
            frozenCacheStats != null ? frozenCacheStats.getFreeRegions() : 0
        );
    }

    public static final class NodeRequest extends TransportRequest {

        public NodeRequest() {}

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static final class NodesRequest extends BaseNodesRequest<NodesRequest> {

        public NodesRequest(String[] nodes) {
            super(nodes);
        }

        public NodesRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodeCachesStatsResponse extends BaseNodeResponse implements ToXContentFragment {

        private final long cacheHits;
        private final long cacheMisses;
        private final long cacheEvictions;
        private final long persistentCacheDocs;
        private final long persistentCacheDeletedDocs;
        private final long persistentCacheIndexSizeInBytes;
        private final long frozenCacheRegionSizeInBytes;
        private final int frozenCacheTotalRegions;
        private final int frozenCacheFreeRegions;

        public NodeCachesStatsResponse(
            DiscoveryNode node,
            long cacheHits,
            long cacheMisses,
            long cacheEvictions,
            long persistentCacheDocs,
            long persistentCacheDeletedDocs,
            long persistentCacheIndexSizeInBytes,
            long frozenCacheRegionSizeInBytes,
            int frozenCacheTotalRegions,
            int frozenCacheFreeRegions
        ) {
            super(node);
            this.cacheHits = cacheHits;
            this.cacheMisses = cacheMisses;
            this.cacheEvictions = cacheEvictions;
            this.persistentCacheDocs = persistentCacheDocs;
            this.persistentCacheDeletedDocs = persistentCacheDeletedDocs;
            this.persistentCacheIndexSizeInBytes = persistentCacheIndexSizeInBytes;
            this.frozenCacheRegionSizeInBytes = frozenCacheRegionSizeInBytes;
            this.frozenCacheTotalRegions = frozenCacheTotalRegions;
            this.frozenCacheFreeRegions = frozenCacheFreeRegions;
        }

        public NodeCachesStatsResponse(StreamInput in) throws IOException {
            super(in);
            this.cacheHits = in.readVLong();
            this.cacheMisses = in.readVLong();
            this.cacheEvictions = in.readVLong();
            this.persistentCacheDocs = in.readVLong();
            this.persistentCacheDeletedDocs = in.readVLong();
            this.persistentCacheIndexSizeInBytes = in.readVLong();
            this.frozenCacheRegionSizeInBytes = in.readVLong();
            this.frozenCacheTotalRegions = in.readVInt();
            this.frozenCacheFreeRegions = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(cacheHits);
            out.writeVLong(cacheMisses);
            out.writeVLong(cacheEvictions);
            out.writeVLong(persistentCacheDocs);
            out.writeVLong(persistentCacheDeletedDocs);
            out.writeVLong(persistentCacheIndexSizeInBytes);
            out.writeVLong(frozenCacheRegionSizeInBytes);
            out.writeVInt(frozenCacheTotalRegions);
            out.writeVInt(frozenCacheFreeRegions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(getNode().getId());
            {
                builder.startObject("caches");
                {
                    builder.startObject("full_cache");
                    builder.field("hits", cacheHits);
                    builder.field("misses", cacheMisses);
                    builder.field("evictions", cacheEvictions);
                    builder.field("persistent_cache_docs", persistentCacheDocs);
                    builder.field("persistent_cache_deleted_docs", persistentCacheDeletedDocs);
                    builder.humanReadableField(
                        "persistent_cache_size_in_bytes",
                        "persistent_cache_size",
                        ByteSizeValue.ofBytes(persistentCacheIndexSizeInBytes)
                    );
                    builder.endObject();
                }
                {
                    builder.startObject("partial_cache");

                    builder.field("total_regions", frozenCacheFreeRegions);
                    final long totalBytes = frozenCacheTotalRegions * frozenCacheRegionSizeInBytes;
                    builder.humanReadableField("total_regions_size_in_bytes", "total_regions_size", ByteSizeValue.ofBytes(totalBytes));

                    builder.field("free_regions", frozenCacheFreeRegions);
                    final long freeBytes = frozenCacheFreeRegions * frozenCacheRegionSizeInBytes;
                    builder.humanReadableField("free_regions_size_in_bytes", "free_regions_size", ByteSizeValue.ofBytes(freeBytes));
                    final double freePercent = totalBytes > 0L ? Math.round(100.0 * ((double) freeBytes / totalBytes) * 10.0) / 10.0 : 0.0;
                    builder.field("free_regions_percent", freePercent);

                    builder.field("used_regions", frozenCacheTotalRegions - frozenCacheFreeRegions);
                    final long usedBytes = totalBytes - freeBytes;
                    builder.humanReadableField("used_regions_size_in_bytes", "used_regions_size", ByteSizeValue.ofBytes(usedBytes));
                    final double usedPercent = totalBytes > 0L ? Math.round(100.0 * ((double) usedBytes / totalBytes) * 10.0) / 10.0 : 0.0;
                    builder.field("used_regions_percent", usedPercent);

                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }

    public static class NodesCachesStatsResponse extends BaseNodesResponse<NodeCachesStatsResponse> implements ToXContentObject {

        public NodesCachesStatsResponse(StreamInput in) throws IOException {
            super(in);
        }

        public NodesCachesStatsResponse(ClusterName clusterName, List<NodeCachesStatsResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeCachesStatsResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeCachesStatsResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeCachesStatsResponse> nodes) throws IOException {
            out.writeList(nodes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.startObject("nodes");
                for (NodeCachesStatsResponse node : getNodes()) {
                    node.toXContent(builder, params);
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

    }
}
