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
import org.elasticsearch.Version;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.NodeEnvironment;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 */
public class TransportNodesListGatewayStartedShards extends TransportNodesAction<
    TransportNodesListGatewayStartedShards.Request,
    TransportNodesListGatewayStartedShards.NodesGroupedGatewayStartedShards,
    TransportNodesListGatewayStartedShards.NodeRequest,
    TransportNodesListGatewayStartedShards.NodeGroupedGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    public static final ActionType<NodesGroupedGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME, NodesGroupedGatewayStartedShards::new);

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
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STARTED,
            NodeGroupedGatewayStartedShards.class
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
    protected NodeGroupedGatewayStartedShards newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        final NodeGroupedGatewayStartedShards response = new NodeGroupedGatewayStartedShards(in, node);
        assert response.getNode() == node;
        return response;
    }

    @Override
    protected NodesGroupedGatewayStartedShards newResponse(Request request,
                                                           List<NodeGroupedGatewayStartedShards> responses,
                                                           List<FailedNodeException> failures) {
        return new NodesGroupedGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGroupedGatewayStartedShards nodeOperation(NodeRequest request, Task task) {
        NodeGroupedGatewayStartedShards groupedStartedShards = new NodeGroupedGatewayStartedShards(clusterService.localNode());
        for (Map.Entry<ShardId, String> entry : request.getShards().entrySet()) {
            NodeGatewayStartedShards startedShard = handleFetch(entry.getKey(), entry.getValue());
            groupedStartedShards.addStartedShard(startedShard);
        }

        return groupedStartedShards;
    }

    private NodeGatewayStartedShards handleFetch(final ShardId shardId, String customDataPath) {
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
                        return new NodeGatewayStartedShards(shardId,
                            clusterService.localNode(),
                            allocationId,
                            shardStateMetadata.primary,
                            exception
                        );
                    }
                }

                logger.debug("{} shard state info found: [{}]", shardId, shardStateMetadata);
                String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                return new NodeGatewayStartedShards(shardId, clusterService.localNode(), allocationId, shardStateMetadata.primary);
            }
            logger.trace("{} no local shard info found", shardId);
            return new NodeGatewayStartedShards(shardId, clusterService.localNode(), null, false);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load started shards", e);
        }
    }

    public static class Request extends BaseNodesRequest<Request> {

        private final Map<ShardId, String> shards;

        public Request(StreamInput in) throws IOException {
            super(in);
            shards = new HashMap<>();
            if (in.getVersion().before(Version.V_7_16_0)) {
                ShardId shardId = new ShardId(in);
                String customDataPath = null;
                if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
                    customDataPath = in.readString();
                }
                shards.put(shardId, customDataPath);
            } else {
                int size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    ShardId shardId = new ShardId(in);
                    String customDataPath = null;
                    if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
                        customDataPath = in.readString();
                    }
                    shards.put(shardId, customDataPath);
                }
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
            if (out.getVersion().before(Version.V_7_16_0)) {
                assert shards.size() == 1;
                Map.Entry<ShardId, String> entry = shards.entrySet().iterator().next();
                entry.getKey().writeTo(out);
                if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
                    String customDataPath = entry.getValue();
                    assert customDataPath != null;
                    out.writeString(customDataPath);
                }
            } else {
                out.writeVInt(shards.size());
                for (Map.Entry<ShardId, String> entry : shards.entrySet()) {
                    entry.getKey().writeTo(out);
                    if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
                        out.writeString(entry.getValue());
                    }
                }
            }
        }
    }

    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        public NodesGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShards(
            ClusterName clusterName,
            List<NodeGatewayStartedShards> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class NodeRequest extends TransportRequest {

        private final Map<ShardId, String> shards;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shards = new HashMap<>();
            if (in.getVersion().before(Version.V_7_16_0)) {
                ShardId shardId = new ShardId(in);
                String customDataPath = null;
                if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
                    customDataPath = in.readString();
                }
                shards.put(shardId, customDataPath);
            } else {
                int size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    ShardId shardId = new ShardId(in);
                    String customDataPath = null;
                    if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
                        customDataPath = in.readString();
                    }
                    shards.put(shardId, customDataPath);
                }
            }
        }

        public NodeRequest(Request request) {
            this.shards = request.shards;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().before(Version.V_7_16_0)) {
                assert shards.size() == 1;
                Map.Entry<ShardId, String> entry = shards.entrySet().iterator().next();
                entry.getKey().writeTo(out);
                if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
                    String customDataPath = entry.getValue();
                    assert customDataPath != null;
                    out.writeString(customDataPath);
                }
            } else {
                out.writeVInt(shards.size());
                for (Map.Entry<ShardId, String> entry : shards.entrySet()) {
                    entry.getKey().writeTo(out);
                    if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
                        out.writeString(entry.getValue());
                    }
                }
            }
        }

        public Map<ShardId, String> getShards() {
            return shards;
        }
    }

    public static class NodeGatewayStartedShards extends BaseNodeResponse {

        private final ShardId shardId;
        private final String allocationId;
        private final boolean primary;
        private final Exception storeException;

        public NodeGatewayStartedShards(StreamInput in) throws IOException {
            this(in, null);
        }

        public NodeGatewayStartedShards(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
            allocationId = in.readOptionalString();
            primary = in.readBoolean();
            if (in.readBoolean()) {
                storeException = in.readException();
            } else {
                storeException = null;
            }
            shardId = new ShardId(in);
        }

        public NodeGatewayStartedShards(ShardId shardId, DiscoveryNode node, String allocationId, boolean primary) {
            this(shardId, node, allocationId, primary, null);
        }

        public NodeGatewayStartedShards(ShardId shardId, DiscoveryNode node, String allocationId,
                                        boolean primary, Exception storeException) {
            super(node);
            this.allocationId = allocationId;
            this.primary = primary;
            this.shardId = shardId;
            this.storeException = storeException;
        }

        public String allocationId() {
            return this.allocationId;
        }

        public boolean primary() {
            return this.primary;
        }

        public ShardId getShardId() {
            return shardId;
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
            shardId.writeTo(out);
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

    public static class ShardRequestInfo<T extends BaseNodeResponse> {

        private final ShardId shardId;
        private final ActionListener<BaseNodesResponse<T>> listener;
        @Nullable
        private final String customDataPath;

        public ShardRequestInfo(ShardId shardId, String customDataPath, ActionListener<BaseNodesResponse<T>> listener) {
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
            this.listener = Objects.requireNonNull(listener);
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

        public ActionListener<BaseNodesResponse<T>> getListener() {
            return listener;
        }
    }

    public static class NodesGroupedGatewayStartedShards extends BaseNodesResponse<NodeGroupedGatewayStartedShards> {

        public NodesGroupedGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGroupedGatewayStartedShards(ClusterName clusterName, List<NodeGroupedGatewayStartedShards> nodes,
                                                List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGroupedGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGroupedGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGroupedGatewayStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class NodeGroupedGatewayStartedShards extends BaseNodeResponse {

        private final List<NodeGatewayStartedShards> startedShards = new ArrayList<>();

        public NodeGroupedGatewayStartedShards(DiscoveryNode node) {
            super(node);
        }

        public NodeGroupedGatewayStartedShards(StreamInput in) throws IOException {
            this(in, null);
        }

        public NodeGroupedGatewayStartedShards(StreamInput in, DiscoveryNode node) throws IOException {
            super(node); // we skip node serialization here, instead do it in NodeGatewayStartedShards.
            if (in.getVersion().before(Version.V_7_14_2)) {
                startedShards.add(new NodeGatewayStartedShards(in, node));
            } else {
                int size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    startedShards.add(new NodeGatewayStartedShards(in, node));
                }
            }
        }

        public void addStartedShard(NodeGatewayStartedShards startedShard) {
            startedShards.add(startedShard);
        }

        public List<NodeGatewayStartedShards> getStartedShards() {
            return startedShards;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // we skip node serialization here, instead do it in NodeGatewayStartedShards.
            if (out.getVersion().before(Version.V_7_14_2)) {
                assert startedShards.size() == 1;
                startedShards.get(0).writeTo(out);
            } else {
                out.writeVInt(startedShards.size());
                for (NodeGatewayStartedShards shard : startedShards) {
                    shard.writeTo(out);
                }
            }
        }
    }
}
