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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetaData;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 */
public class TransportNodesListGatewayStartedShards extends
    TransportNodesAction<TransportNodesListGatewayStartedShards.Request,
        TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
        TransportNodesListGatewayStartedShards.NodeRequest,
        TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>
    implements
    AsyncShardFetch.Lister<TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
        TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    private final NodeEnvironment nodeEnv;


    @Inject
    public TransportNodesListGatewayStartedShards(Settings settings, ThreadPool threadPool,
                                                  ClusterService clusterService, TransportService transportService,
                                                  ActionFilters actionFilters,
                                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                                  NodeEnvironment env) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
              indexNameExpressionResolver, Request::new, NodeRequest::new, ThreadPool.Names.FETCH_SHARD_STARTED,
              NodeGatewayStartedShards.class);
        this.nodeEnv = env;
    }

    @Override
    public void list(ShardId shardId, String[] nodesIds,
                     ActionListener<NodesGatewayStartedShards> listener) {
        execute(new Request(shardId, nodesIds), listener);
    }

    @Override
    protected String[] resolveNodes(Request request, ClusterState clusterState) {
        // default implementation may filter out non existent nodes. it's important to keep exactly the ids
        // we were given for accounting on the caller
        return request.nodesIds();
    }

    @Override
    protected boolean transportCompress() {
        return true; // this can become big...
    }

    @Override
    protected NodeRequest newNodeRequest(String nodeId, Request request) {
        return new NodeRequest(nodeId, request);
    }

    @Override
    protected NodeGatewayStartedShards newNodeResponse() {
        return new NodeGatewayStartedShards();
    }

    @Override
    protected NodesGatewayStartedShards newResponse(Request request,
                                                    List<NodeGatewayStartedShards> responses, List<FailedNodeException> failures) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayStartedShards nodeOperation(NodeRequest request) {
        try {
            final ShardId shardId = request.getShardId();
            logger.trace("{} loading local shard state info", shardId);
            ShardStateMetaData shardStateMetaData = ShardStateMetaData.FORMAT.loadLatestState(logger,
                nodeEnv.availableShardPaths(request.shardId));
            if (shardStateMetaData != null) {
                IndexMetaData metaData = clusterService.state().metaData().index(shardId.getIndex());
                if (metaData == null) {
                    // we may send this requests while processing the cluster state that recovered the index
                    // sometimes the request comes in before the local node processed that cluster state
                    // in such cases we can load it from disk
                    metaData = IndexMetaData.FORMAT.loadLatestState(logger, nodeEnv.indexPaths(shardId.getIndex()));
                }
                if (metaData == null) {
                    ElasticsearchException e = new ElasticsearchException("failed to find local IndexMetaData");
                    e.setShard(request.shardId);
                    throw e;
                }

                ShardPath shardPath = null;
                try {
                    IndexSettings indexSettings = new IndexSettings(metaData, settings);
                    shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, indexSettings);
                    if (shardPath == null) {
                        throw new IllegalStateException(shardId + " no shard path found");
                    }
                    Store.tryOpenIndex(shardPath.resolveIndex(), shardId, logger);
                } catch (Exception exception) {
                    logger.trace("{} can't open index for shard [{}] in path [{}]", exception, shardId,
                        shardStateMetaData, (shardPath != null) ? shardPath.resolveIndex() : "");
                    String allocationId = shardStateMetaData.allocationId != null ?
                        shardStateMetaData.allocationId.getId() : null;
                    return new NodeGatewayStartedShards(clusterService.localNode(), shardStateMetaData.legacyVersion,
                        allocationId, shardStateMetaData.primary, exception);
                }

                logger.debug("{} shard state info found: [{}]", shardId, shardStateMetaData);
                String allocationId = shardStateMetaData.allocationId != null ?
                    shardStateMetaData.allocationId.getId() : null;
                return new NodeGatewayStartedShards(clusterService.localNode(), shardStateMetaData.legacyVersion,
                    allocationId, shardStateMetaData.primary);
            }
            logger.trace("{} no local shard info found", shardId);
            return new NodeGatewayStartedShards(clusterService.localNode(), ShardStateMetaData.NO_VERSION, null, false);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load started shards", e);
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }

    public static class Request extends BaseNodesRequest<Request> {

        private ShardId shardId;

        public Request() {
        }

        public Request(ShardId shardId, String[] nodesIds) {
            super(nodesIds);
            this.shardId = shardId;
        }


        public ShardId shardId() {
            return this.shardId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }
    }

    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        public NodesGatewayStartedShards(ClusterName clusterName, List<NodeGatewayStartedShards> nodes,
                                         List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readStreamableList(NodeGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShards> nodes) throws IOException {
            out.writeStreamableList(nodes);
        }
    }


    public static class NodeRequest extends BaseNodeRequest {

        private ShardId shardId;

        public NodeRequest() {
        }

        public NodeRequest(String nodeId, Request request) {
            super(nodeId);
            this.shardId = request.shardId();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
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

        private long legacyVersion = ShardStateMetaData.NO_VERSION; // for pre-3.0 shards that have not yet been active
        private String allocationId = null;
        private boolean primary = false;
        private Throwable storeException = null;

        public NodeGatewayStartedShards() {
        }

        public NodeGatewayStartedShards(DiscoveryNode node, long legacyVersion, String allocationId, boolean primary) {
            this(node, legacyVersion, allocationId, primary, null);
        }

        public NodeGatewayStartedShards(DiscoveryNode node, long legacyVersion, String allocationId, boolean primary,
                                        Throwable storeException) {
            super(node);
            this.legacyVersion = legacyVersion;
            this.allocationId = allocationId;
            this.primary = primary;
            this.storeException = storeException;
        }

        public long legacyVersion() {
            return this.legacyVersion;
        }

        public String allocationId() {
            return this.allocationId;
        }

        public boolean primary() {
            return this.primary;
        }

        public Throwable storeException() {
            return this.storeException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            legacyVersion = in.readLong();
            allocationId = in.readOptionalString();
            primary = in.readBoolean();
            if (in.readBoolean()) {
                storeException = in.readThrowable();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(legacyVersion);
            out.writeOptionalString(allocationId);
            out.writeBoolean(primary);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeThrowable(storeException);
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

            if (legacyVersion != that.legacyVersion) {
                return false;
            }
            if (primary != that.primary) {
                return false;
            }
            if (allocationId != null ? !allocationId.equals(that.allocationId) : that.allocationId != null) {
                return false;
            }
            return storeException != null ? storeException.equals(that.storeException) : that.storeException == null;

        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(legacyVersion);
            result = 31 * result + (allocationId != null ? allocationId.hashCode() : 0);
            result = 31 * result + (primary ? 1 : 0);
            result = 31 * result + (storeException != null ? storeException.hashCode() : 0);
            return result;
        }
    }
}
