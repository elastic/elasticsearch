/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexNotFoundException;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

public class TransportNodesListShardStoreMetadata extends TransportNodesAction<
    TransportNodesListShardStoreMetadata.Request,
    TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata,
    TransportNodesListShardStoreMetadata.NodeRequest,
    TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata,
    Void> {

    private static final Logger logger = LogManager.getLogger(TransportNodesListShardStoreMetadata.class);

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/shard/store";
    public static final ActionType<NodesStoreFilesMetadata> TYPE = new ActionType<>(ACTION_NAME);

    private final Settings settings;
    private final IndicesService indicesService;
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportNodesListShardStoreMetadata(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        NodeEnvironment nodeEnv,
        ActionFilters actionFilters
    ) {
        super(
            ACTION_NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.FETCH_SHARD_STORE)
        );
        this.settings = settings;
        this.indicesService = indicesService;
        this.nodeEnv = nodeEnv;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeStoreFilesMetadata newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        final NodeStoreFilesMetadata nodeStoreFilesMetadata = new NodeStoreFilesMetadata(in, node);
        assert nodeStoreFilesMetadata.getNode() == node;
        return nodeStoreFilesMetadata;
    }

    @Override
    protected NodesStoreFilesMetadata newResponse(
        Request request,
        List<NodeStoreFilesMetadata> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesStoreFilesMetadata(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStoreFilesMetadata nodeOperation(NodeRequest request, Task task) {
        try {
            return new NodeStoreFilesMetadata(clusterService.localNode(), listStoreMetadata(request));
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to list store metadata for shard [" + request.shardId + "]", e);
        }
    }

    private StoreFilesMetadata listStoreMetadata(NodeRequest request) throws IOException {
        final ShardId shardId = request.getShardId();
        logger.trace("listing store meta data for {}", shardId);
        long startTimeNS = System.nanoTime();
        boolean exists = false;
        try {
            IndexService indexService = indicesService.indexService(shardId.getIndex());
            if (indexService != null) {
                IndexShard indexShard = indexService.getShardOrNull(shardId.id());
                if (indexShard != null) {
                    try {
                        final StoreFilesMetadata storeFilesMetadata = new StoreFilesMetadata(
                            indexShard.snapshotStoreMetadata(),
                            indexShard.getPeerRecoveryRetentionLeases()
                        );
                        exists = true;
                        return storeFilesMetadata;
                    } catch (IndexNotFoundException e) {
                        logger.trace(() -> "[" + shardId + "] node is missing index, responding with empty", e);
                        return StoreFilesMetadata.EMPTY;
                    } catch (IOException e) {
                        logger.warn(() -> "[" + shardId + "] can't read metadata from store, responding with empty", e);
                        return StoreFilesMetadata.EMPTY;
                    }
                }
            }
            final String customDataPath;
            if (request.getCustomDataPath() != null) {
                customDataPath = request.getCustomDataPath();
            } else {
                // TODO: Fallback for BWC with older ES versions. Remove this once request.getCustomDataPath() always returns non-null
                if (indexService != null) {
                    customDataPath = indexService.getIndexSettings().customDataPath();
                } else {
                    IndexMetadata metadata = clusterService.state().metadata().findIndex(shardId.getIndex()).orElse(null);
                    if (metadata != null) {
                        customDataPath = new IndexSettings(metadata, settings).customDataPath();
                    } else {
                        logger.trace("{} node doesn't have meta data for the requests index", shardId);
                        throw new ElasticsearchException("node doesn't have meta data for index " + shardId.getIndex());
                    }
                }
            }
            final ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
            if (shardPath == null) {
                return StoreFilesMetadata.EMPTY;
            }
            // note that this may fail if it can't get access to the shard lock. Since we check above there is an active shard, this means:
            // 1) a shard is being constructed, which means the master will not use a copy of this replica
            // 2) A shard is shutting down and has not cleared it's content within lock timeout. In this case the master may not
            // reuse local resources.
            final Store.MetadataSnapshot metadataSnapshot = Store.readMetadataSnapshot(
                shardPath.resolveIndex(),
                shardId,
                nodeEnv::shardLock,
                logger
            );
            // We use peer recovery retention leases from the primary for allocating replicas. We should always have retention leases when
            // we refresh shard info after the primary has started. Hence, we can ignore retention leases if there is no active shard.
            return new StoreFilesMetadata(metadataSnapshot, emptyList());
        } finally {
            TimeValue took = new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS);
            if (exists) {
                logger.debug("{} loaded store meta data (took [{}])", shardId, took);
            } else {
                logger.trace("{} didn't find any store meta data to load (took [{}])", shardId, took);
            }
        }
    }

    public record StoreFilesMetadata(Store.MetadataSnapshot metadataSnapshot, List<RetentionLease> peerRecoveryRetentionLeases)
        implements
            Iterable<StoreFileMetadata>,
            Writeable {

        private static final ShardId FAKE_SHARD_ID = new ShardId("_na_", "_na_", 0);
        public static final StoreFilesMetadata EMPTY = new StoreFilesMetadata(Store.MetadataSnapshot.EMPTY, emptyList());

        public static StoreFilesMetadata readFrom(StreamInput in) throws IOException {
            if (in.getTransportVersion().before(TransportVersions.V_8_2_0)) {
                new ShardId(in);
            }
            final var metadataSnapshot = Store.MetadataSnapshot.readFrom(in);
            final var peerRecoveryRetentionLeases = in.readCollectionAsImmutableList(RetentionLease::new);
            if (metadataSnapshot == Store.MetadataSnapshot.EMPTY && peerRecoveryRetentionLeases.isEmpty()) {
                return EMPTY;
            } else {
                return new StoreFilesMetadata(metadataSnapshot, peerRecoveryRetentionLeases);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().before(TransportVersions.V_8_2_0)) {
                // no compatible version cares about the shard ID, we can just make one up
                FAKE_SHARD_ID.writeTo(out);
            }
            metadataSnapshot.writeTo(out);
            out.writeCollection(peerRecoveryRetentionLeases);
        }

        public boolean isEmpty() {
            return metadataSnapshot.size() == 0;
        }

        @Override
        public Iterator<StoreFileMetadata> iterator() {
            return metadataSnapshot.iterator();
        }

        public boolean fileExists(String name) {
            return metadataSnapshot.fileMetadataMap().containsKey(name);
        }

        public StoreFileMetadata file(String name) {
            return metadataSnapshot.fileMetadataMap().get(name);
        }

        /**
         * Returns the retaining sequence number of the peer recovery retention lease for a given node if exists; otherwise, returns -1.
         */
        public long getPeerRecoveryRetentionLeaseRetainingSeqNo(DiscoveryNode node) {
            assert node != null;
            final String retentionLeaseId = ReplicationTracker.getPeerRecoveryRetentionLeaseId(node.getId());
            return peerRecoveryRetentionLeases.stream()
                .filter(lease -> lease.id().equals(retentionLeaseId))
                .mapToLong(RetentionLease::retainingSequenceNumber)
                .findFirst()
                .orElse(-1L);
        }

        @Override
        public String toString() {
            return "StoreFilesMetadata{" + ", metadataSnapshot{size=" + metadataSnapshot.size() + "}" + '}';
        }
    }

    public static class Request extends BaseNodesRequest {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public Request(ShardId shardId, String customDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
        }

        public ShardId shardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }
    }

    public static class NodesStoreFilesMetadata extends BaseNodesResponse<NodeStoreFilesMetadata> {

        public NodesStoreFilesMetadata(ClusterName clusterName, List<NodeStoreFilesMetadata> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeStoreFilesMetadata> readNodesFrom(StreamInput in) throws IOException {
            return TransportAction.localOnly();
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeStoreFilesMetadata> nodes) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static class NodeRequest extends AbstractTransportRequest {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            customDataPath = in.readString();
        }

        public NodeRequest(Request request) {
            this.shardId = Objects.requireNonNull(request.shardId());
            this.customDataPath = Objects.requireNonNull(request.getCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            assert customDataPath != null;
            out.writeString(customDataPath);
        }

        public ShardId getShardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }
    }

    public static class NodeStoreFilesMetadata extends BaseNodeResponse {

        private final StoreFilesMetadata storeFilesMetadata;

        public NodeStoreFilesMetadata(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
            storeFilesMetadata = StoreFilesMetadata.readFrom(in);
        }

        public NodeStoreFilesMetadata(DiscoveryNode node, StoreFilesMetadata storeFilesMetadata) {
            super(node);
            this.storeFilesMetadata = storeFilesMetadata;
        }

        public StoreFilesMetadata storeFilesMetadata() {
            return storeFilesMetadata;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            storeFilesMetadata.writeTo(out);
        }

        @Override
        public String toString() {
            return "[[" + getNode() + "][" + storeFilesMetadata + "]]";
        }
    }
}
