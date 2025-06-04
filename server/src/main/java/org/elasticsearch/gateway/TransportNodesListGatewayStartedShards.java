/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetadata;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 */
public class TransportNodesListGatewayStartedShards extends TransportNodesAction<
    TransportNodesListGatewayStartedShards.Request,
    TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
    TransportNodesListGatewayStartedShards.NodeRequest,
    TransportNodesListGatewayStartedShards.NodeGatewayStartedShards,
    Void> {

    private static final Logger logger = LogManager.getLogger(TransportNodesListGatewayStartedShards.class);

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    public static final ActionType<NodesGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesListGatewayStartedShards(
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
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.FETCH_SHARD_STARTED)
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
    protected NodeGatewayStartedShards newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        final NodeGatewayStartedShards response = new NodeGatewayStartedShards(in, node);
        assert response.getNode() == node;
        return response;
    }

    @Override
    protected NodesGatewayStartedShards newResponse(
        Request request,
        List<NodeGatewayStartedShards> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayStartedShards nodeOperation(NodeRequest request, Task task) {
        try {
            final ShardId shardId = request.getShardId();
            logger.trace("{} loading local shard state info", shardId);
            ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(
                logger,
                namedXContentRegistry,
                nodeEnv.availableShardPaths(request.shardId)
            );
            if (shardStateMetadata != null) {
                if (indicesService.getShardOrNull(shardId) == null) {
                    final String customDataPath;
                    if (request.getCustomDataPath() != null) {
                        customDataPath = request.getCustomDataPath();
                    } else {
                        // TODO: Fallback for BWC with older ES versions. Remove once request.getCustomDataPath() always returns non-null
                        final IndexMetadata metadata = clusterService.state().metadata().findIndex(shardId.getIndex()).orElse(null);
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
                            () -> format(
                                "%s can't open index for shard [%s] in path [%s]",
                                shardId,
                                shardStateMetadata,
                                (finalShardPath != null) ? finalShardPath.resolveIndex() : ""
                            ),
                            exception
                        );
                        String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                        return new NodeGatewayStartedShards(
                            clusterService.localNode(),
                            allocationId,
                            shardStateMetadata.primary,
                            exception
                        );
                    }
                }

                logger.debug("{} shard state info found: [{}]", shardId, shardStateMetadata);
                String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                return new NodeGatewayStartedShards(clusterService.localNode(), allocationId, shardStateMetadata.primary);
            }
            logger.trace("{} no local shard info found", shardId);
            return new NodeGatewayStartedShards(clusterService.localNode(), null, false);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load started shards", e);
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

    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        public NodesGatewayStartedShards(
            ClusterName clusterName,
            List<NodeGatewayStartedShards> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return TransportAction.localOnly();
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShards> nodes) throws IOException {
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

    public static class NodeGatewayStartedShards extends BaseNodeResponse {

        private final String allocationId;
        private final boolean primary;
        private final Exception storeException;

        public NodeGatewayStartedShards(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
            allocationId = in.readOptionalString();
            primary = in.readBoolean();
            if (in.readBoolean()) {
                storeException = in.readException();
            } else {
                storeException = null;
            }
        }

        public NodeGatewayStartedShards(DiscoveryNode node, String allocationId, boolean primary) {
            this(node, allocationId, primary, null);
        }

        public NodeGatewayStartedShards(DiscoveryNode node, String allocationId, boolean primary, Exception storeException) {
            super(node);
            this.allocationId = allocationId;
            this.primary = primary;
            this.storeException = storeException;
        }

        public String allocationId() {
            return this.allocationId;
        }

        public boolean primary() {
            return this.primary;
        }

        public Exception storeException() {
            return this.storeException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(allocationId);
            out.writeBoolean(primary);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeException(storeException);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NodeGatewayStartedShards that = (NodeGatewayStartedShards) o;

            return primary == that.primary
                && Objects.equals(allocationId, that.allocationId)
                && Objects.equals(storeException, that.storeException);
        }

        @Override
        public int hashCode() {
            int result = (allocationId != null ? allocationId.hashCode() : 0);
            result = 31 * result + (primary ? 1 : 0);
            result = 31 * result + (storeException != null ? storeException.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("NodeGatewayStartedShards[").append("allocationId=").append(allocationId).append(",primary=").append(primary);
            if (storeException != null) {
                buf.append(",storeException=").append(storeException);
            }
            buf.append("]");
            return buf.toString();
        }
    }
}
