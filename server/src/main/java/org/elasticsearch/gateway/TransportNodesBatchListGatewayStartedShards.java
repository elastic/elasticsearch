/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetadata;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 */
public class TransportNodesBatchListGatewayStartedShards extends TransportNodesAction<
    TransportNodesBatchListGatewayStartedShards.Request,
    TransportNodesBatchListGatewayStartedShards.NodesGatewayBatchStartedShards,
    TransportNodesBatchListGatewayStartedShards.NodeRequest,
    TransportNodesBatchListGatewayStartedShards.NodeGatewayBatchStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/batch_started_shards";
    public static final ActionType<NodesGatewayBatchStartedShards> TYPE = new ActionType<>(
        ACTION_NAME,
        NodesGatewayBatchStartedShards::new
    );

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesBatchListGatewayStartedShards(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeEnvironment env,
        IndicesService indicesService,
        NamedXContentRegistry namedXContentRegistry
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STARTED,
            NodeGatewayBatchStartedShards.class
        );
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeGatewayBatchStartedShards newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        final NodeGatewayBatchStartedShards response = new NodeGatewayBatchStartedShards(in, node);
        assert response.getNode() == node;
        return response;
    }

    @Override
    protected NodesGatewayBatchStartedShards newResponse(
        Request request,
        List<NodeGatewayBatchStartedShards> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayBatchStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayBatchStartedShards nodeOperation(NodeRequest request, Task task) {
        NodeGatewayBatchStartedShards batchStartedShards = new NodeGatewayBatchStartedShards(clusterService.localNode());
        try {
            final CountDownLatch latch = new CountDownLatch(request.getShards().size());
            for (Map.Entry<ShardId, String> entry : request.getShards().entrySet()) {
                threadPool.executor(ThreadPool.Names.FETCH_SHARD_STARTED).execute(() -> {
                    try {
                        batchStartedShards.addStartedShard(listStartedShard(entry.getKey(), entry.getValue()));
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return batchStartedShards;
    }

    private NodeGatewayBatchStartedShard listStartedShard(final ShardId shardId, String customDataPath) {
        try {
            logger.trace("{} loading local shard state info", shardId);
            ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(
                logger,
                namedXContentRegistry,
                nodeEnv.availableShardPaths(shardId)
            );
            if (shardStateMetadata != null) {
                if (indicesService.getShardOrNull(shardId) == null) {
                    if (customDataPath == null) {
                        // TODO: Fallback for BWC with older ES versions. Remove once request.getCustomDataPath() always returns non-null
                        final IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
                        if (metadata != null) {
                            customDataPath = new IndexSettings(metadata, settings).customDataPath();
                        } else {
                            logger.trace("{} node doesn't have meta data for the requests index", shardId);
                            throw new ElasticsearchException("node doesn't have meta data for index " + shardId.getIndex());
                        }
                    }
                    // we don't have an open shard on the store, validate the files on disk are openable
                    ShardPath shardPath = null;
                    try {
                        shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
                        if (shardPath == null) {
                            throw new IllegalStateException(shardId + " no shard path found");
                        }
                        Store.tryOpenIndex(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
                    } catch (Exception exception) {
                        final ShardPath finalShardPath = shardPath;
                        logger.trace(
                            () -> new ParameterizedMessage(
                                "{} can't open index for shard [{}] in path [{}]",
                                shardId,
                                shardStateMetadata,
                                (finalShardPath != null) ? finalShardPath.resolveIndex() : ""
                            ),
                            exception
                        );
                        String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                        return new NodeGatewayBatchStartedShard(
                            shardId,
                            clusterService.localNode(),
                            allocationId,
                            shardStateMetadata.primary,
                            exception
                        );
                    }
                }

                logger.debug("{} shard state info found: [{}]", shardId, shardStateMetadata);
                String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                return new NodeGatewayBatchStartedShard(shardId, clusterService.localNode(), allocationId, shardStateMetadata.primary);
            }
            logger.trace("{} no local shard info found", shardId);
            return new NodeGatewayBatchStartedShard(shardId, clusterService.localNode(), null, false);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load started shards", e);
        }
    }

    public static class ShardRequestInfo<T extends BaseNodeResponse> {

        private final ShardId shardId;
        private final ActionListener<BaseNodesResponse<T>> listener;
        private final String customDataPath;

        public ShardRequestInfo(ShardId shardId, String customDataPath, ActionListener<BaseNodesResponse<T>> listener) {
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
            this.listener = Objects.requireNonNull(listener);
        }

        public ShardId shardId() {
            return shardId;
        }

        public String getCustomDataPath() {
            return customDataPath;
        }

        public ActionListener<BaseNodesResponse<T>> getListener() {
            return listener;
        }
    }

    public static class Request extends BaseNodesRequest<Request> {

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

    public static class NodesGatewayBatchStartedShards extends BaseNodesResponse<NodeGatewayBatchStartedShards> {

        public NodesGatewayBatchStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayBatchStartedShards(
            ClusterName clusterName,
            List<NodeGatewayBatchStartedShards> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayBatchStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayBatchStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayBatchStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class NodeGatewayBatchStartedShards extends BaseNodeResponse {

        private final List<NodeGatewayBatchStartedShard> startedShards = Collections.synchronizedList(new ArrayList<>());

        public NodeGatewayBatchStartedShards(DiscoveryNode node) {
            super(node);
        }

        public NodeGatewayBatchStartedShards(StreamInput in) throws IOException {
            this(in, null);
        }

        public NodeGatewayBatchStartedShards(StreamInput in, DiscoveryNode node) throws IOException {
            super(node); // we skip node serialization here, instead do it in NodeGatewayBatchStartedShard.
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                startedShards.add(new NodeGatewayBatchStartedShard(in, node));
            }
        }

        public void addStartedShard(NodeGatewayBatchStartedShard startedShard) {
            startedShards.add(startedShard);
        }

        public List<NodeGatewayBatchStartedShard> getStartedShards() {
            return startedShards;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // we skip node serialization here, instead do it in NodeGatewayBatchStartedShard.
            out.writeVInt(startedShards.size());
            for (NodeGatewayBatchStartedShard shard : startedShards) {
                shard.writeTo(out);
            }
        }
    }

    public static class NodeGatewayBatchStartedShard extends NodeGatewayStartedShards {

        private final ShardId shardId;

        public NodeGatewayBatchStartedShard(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
            shardId = new ShardId(in);
        }

        public NodeGatewayBatchStartedShard(ShardId shardId, DiscoveryNode node, String allocationId, boolean primary) {
            super(node, allocationId, primary, null);
            this.shardId = shardId;
        }

        public NodeGatewayBatchStartedShard(
            ShardId shardId,
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            Exception storeException
        ) {
            super(node, allocationId, primary, storeException);
            this.shardId = shardId;
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }
    }
}
