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

package org.elasticsearch.gateway;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetaData;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 */
public class TransportNodesListGatewayStartedShards extends
    TransportNodesAction<TransportNodesListGatewayStartedShards.Request,
        TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
        TransportNodesListGatewayStartedShards.NodeRequest,
        TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    public static final ActionType<NodesGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayStartedShards::new);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesListGatewayStartedShards(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                  TransportService transportService, ActionFilters actionFilters,
                                                  NodeEnvironment env, IndicesService indicesService,
                                                  NamedXContentRegistry namedXContentRegistry) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
            Request::new, NodeRequest::new, ThreadPool.Names.FETCH_SHARD_STARTED, NodeGatewayStartedShards.class);
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
    protected NodeGatewayStartedShards newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayStartedShards(in);
    }

    @Override
    protected NodesGatewayStartedShards newResponse(Request request,
                                                    List<NodeGatewayStartedShards> responses, List<FailedNodeException> failures) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayStartedShards nodeOperation(NodeRequest request, Task task) {
        try {
            final ShardId shardId = request.getShardId();
            logger.trace("{} loading local shard state info", shardId);
            ShardStateMetaData shardStateMetaData = ShardStateMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry,
                nodeEnv.availableShardPaths(request.shardId));
            if (shardStateMetaData != null) {
                IndexMetaData metaData = clusterService.state().metaData().index(shardId.getIndex());
                if (metaData == null) {
                    // we may send this requests while processing the cluster state that recovered the index
                    // sometimes the request comes in before the local node processed that cluster state
                    // in such cases we can load it from disk
                    metaData = IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry,
                        nodeEnv.indexPaths(shardId.getIndex()));
                }
                if (metaData == null) {
                    ElasticsearchException e = new ElasticsearchException("failed to find local IndexMetaData");
                    e.setShard(request.shardId);
                    throw e;
                }

                if (indicesService.getShardOrNull(shardId) == null) {
                    // we don't have an open shard on the store, validate the files on disk are openable
                    ShardPath shardPath = null;
                    try {
                        IndexSettings indexSettings = new IndexSettings(metaData, settings);
                        shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, indexSettings);
                        if (shardPath == null) {
                            throw new IllegalStateException(shardId + " no shard path found");
                        }
                        Store.tryOpenIndex(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
                    } catch (Exception exception) {
                        final ShardPath finalShardPath = shardPath;
                        logger.trace(() -> new ParameterizedMessage(
                                "{} can't open index for shard [{}] in path [{}]",
                                shardId,
                                shardStateMetaData,
                                (finalShardPath != null) ? finalShardPath.resolveIndex() : ""),
                            exception);
                        String allocationId = shardStateMetaData.allocationId != null ?
                            shardStateMetaData.allocationId.getId() : null;
                        return new NodeGatewayStartedShards(clusterService.localNode(), allocationId, shardStateMetaData.primary,
                            exception);
                    }
                }

                logger.debug("{} shard state info found: [{}]", shardId, shardStateMetaData);
                String allocationId = shardStateMetaData.allocationId != null ?
                    shardStateMetaData.allocationId.getId() : null;
                return new NodeGatewayStartedShards(clusterService.localNode(), allocationId, shardStateMetaData.primary);
            }
            logger.trace("{} no local shard info found", shardId);
            return new NodeGatewayStartedShards(clusterService.localNode(), null, false);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load started shards", e);
        }
    }

    public static class Request extends BaseNodesRequest<Request> {

        private ShardId shardId;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
        }

        public Request(ShardId shardId, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = shardId;
        }


        public ShardId shardId() {
            return this.shardId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }
    }

    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        public NodesGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShards(ClusterName clusterName, List<NodeGatewayStartedShards> nodes,
                                         List<FailedNodeException> failures) {
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


    public static class NodeRequest extends BaseNodeRequest {

        private ShardId shardId;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
        }

        public NodeRequest(Request request) {
            this.shardId = request.shardId();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }

        public ShardId getShardId() {
            return shardId;
        }
    }

    public static class NodeGatewayStartedShards extends BaseNodeResponse {

        private String allocationId = null;
        private boolean primary = false;
        private Exception storeException = null;

        public NodeGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
            allocationId = in.readOptionalString();
            primary = in.readBoolean();
            if (in.readBoolean()) {
                storeException = in.readException();
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

            return primary == that.primary && Objects.equals(allocationId, that.allocationId)
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
            buf.append("NodeGatewayStartedShards[")
               .append("allocationId=").append(allocationId)
               .append(",primary=").append(primary);
            if (storeException != null) {
                buf.append(",storeException=").append(storeException);
            }
            buf.append("]");
            return buf.toString();
        }
    }
}
