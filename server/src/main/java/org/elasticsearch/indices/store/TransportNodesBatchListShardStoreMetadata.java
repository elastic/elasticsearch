/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.store;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.StoreFilesMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransportNodesBatchListShardStoreMetadata extends TransportNodesAction<
    TransportNodesBatchListShardStoreMetadata.Request,
    TransportNodesBatchListShardStoreMetadata.NodesBatchStoreFilesMetadata,
    TransportNodesBatchListShardStoreMetadata.NodeRequest,
    TransportNodesBatchListShardStoreMetadata.NodeBatchStoreFilesMetadata> {

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/batch_shard/store";
    public static final ActionType<NodesBatchStoreFilesMetadata> TYPE = new ActionType<>(ACTION_NAME, NodesBatchStoreFilesMetadata::new);

    private final Settings settings;
    private final IndicesService indicesService;
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportNodesBatchListShardStoreMetadata(
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
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STORE,
            NodeBatchStoreFilesMetadata.class
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
    protected NodeBatchStoreFilesMetadata newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        final NodeBatchStoreFilesMetadata nodeBatchStoreFilesMetadata = new NodeBatchStoreFilesMetadata(in, node);
        assert nodeBatchStoreFilesMetadata.getNode() == node;
        return nodeBatchStoreFilesMetadata;
    }

    @Override
    protected NodesBatchStoreFilesMetadata newResponse(
        Request request,
        List<NodeBatchStoreFilesMetadata> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesBatchStoreFilesMetadata(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeBatchStoreFilesMetadata nodeOperation(NodeRequest request, Task task) {
        NodeBatchStoreFilesMetadata batchStoreFiles = new NodeBatchStoreFilesMetadata(clusterService.localNode());
        try {
            final CountDownLatch latch = new CountDownLatch(request.getShards().size());
            for (Map.Entry<ShardId, String> entry : request.getShards().entrySet()) {
                threadPool.executor(ThreadPool.Names.FETCH_SHARD_STORE).execute(() -> {
                    try {
                        batchStoreFiles.addStoreFilesMetadata(listStoreMetadata(entry.getKey(), entry.getValue()));
                    } catch (IOException e) {
                        throw new ElasticsearchException("Failed to list store metadata for shard [" + entry.getKey() + "]", e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return batchStoreFiles;
    }

    private StoreFilesMetadata listStoreMetadata(final ShardId shardId, String customDataPath) throws IOException {
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
                            shardId,
                            indexShard.snapshotStoreMetadata(),
                            indexShard.getPeerRecoveryRetentionLeases()
                        );
                        exists = true;
                        return storeFilesMetadata;
                    } catch (org.apache.lucene.index.IndexNotFoundException e) {
                        logger.trace(new ParameterizedMessage("[{}] node is missing index, responding with empty", shardId), e);
                        return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
                    } catch (IOException e) {
                        logger.warn(new ParameterizedMessage("[{}] can't read metadata from store, responding with empty", shardId), e);
                        return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
                    }
                }
            }

            if (customDataPath == null) {
                // TODO: Fallback for BWC with older ES versions. Remove this once request.getCustomDataPath() always returns non-null
                if (indexService != null) {
                    customDataPath = indexService.getIndexSettings().customDataPath();
                } else {
                    IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
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
                return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
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
            return new StoreFilesMetadata(shardId, metadataSnapshot, Collections.emptyList());
        } finally {
            TimeValue took = new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS);
            if (exists) {
                logger.debug("{} loaded store meta data (took [{}])", shardId, took);
            } else {
                logger.trace("{} didn't find any store meta data to load (took [{}])", shardId, took);
            }
        }
    }

    public static class Request extends BaseNodesRequest<TransportNodesBatchListShardStoreMetadata.Request> {

        private final Map<ShardId, String> shards;

        public Request(StreamInput in) throws IOException {
            super(in);
            shards = new HashMap<>();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                shards.put(new ShardId(in), in.readString());
            }
        }

        public Request(Map<ShardId, String> shards, DiscoveryNode[] nodes) {
            super(nodes);
            this.shards = shards;
        }

        public Map<ShardId, String> getShards() {
            return shards;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(shards.size());
            for (Map.Entry<ShardId, String> entry : shards.entrySet()) {
                entry.getKey().writeTo(out);
                out.writeString(entry.getValue());
            }
        }
    }

    public static class NodeRequest extends TransportRequest {

        private final Map<ShardId, String> shards;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shards = new HashMap<>();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                shards.put(new ShardId(in), in.readString());
            }
        }

        public NodeRequest(Request request) {
            this.shards = request.shards;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(shards.size());
            for (Map.Entry<ShardId, String> entry : shards.entrySet()) {
                entry.getKey().writeTo(out);
                out.writeString(entry.getValue());
            }
        }

        public Map<ShardId, String> getShards() {
            return shards;
        }
    }

    public static class NodesBatchStoreFilesMetadata extends BaseNodesResponse<NodeBatchStoreFilesMetadata> {

        public NodesBatchStoreFilesMetadata(StreamInput in) throws IOException {
            super(in);
        }

        public NodesBatchStoreFilesMetadata(
            ClusterName clusterName,
            List<NodeBatchStoreFilesMetadata> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeBatchStoreFilesMetadata> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeBatchStoreFilesMetadata::readListShardStoreNodeOperationResponse);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeBatchStoreFilesMetadata> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class NodeBatchStoreFilesMetadata extends BaseNodeResponse {

        private final List<TransportNodesListShardStoreMetadata.StoreFilesMetadata> storeFilesMetadataList = Collections.synchronizedList(
            new ArrayList<>()
        );

        public NodeBatchStoreFilesMetadata(DiscoveryNode node) {
            super(node);
        }

        public NodeBatchStoreFilesMetadata(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                storeFilesMetadataList.add(StoreFilesMetadata.readFrom(in));
            }
        }

        public List<TransportNodesListShardStoreMetadata.StoreFilesMetadata> storeFilesMetadataList() {
            return storeFilesMetadataList;
        }

        public void addStoreFilesMetadata(TransportNodesListShardStoreMetadata.StoreFilesMetadata nodeStoreFilesMetadata) {
            this.storeFilesMetadataList.add(nodeStoreFilesMetadata);
        }

        public static NodeBatchStoreFilesMetadata readListShardStoreNodeOperationResponse(StreamInput in) throws IOException {
            return new NodeBatchStoreFilesMetadata(in, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(storeFilesMetadataList.size());
            for (TransportNodesListShardStoreMetadata.StoreFilesMetadata shard : storeFilesMetadataList) {
                shard.writeTo(out);
            }
        }
    }
}
