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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetaData;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 */
public class TransportNodesListGatewayStartedShards extends TransportNodesAction<TransportNodesListGatewayStartedShards.Request, TransportNodesListGatewayStartedShards.NodesGatewayStartedShards, TransportNodesListGatewayStartedShards.NodeRequest, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>
        implements AsyncShardFetch.List<TransportNodesListGatewayStartedShards.NodesGatewayStartedShards, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportNodesListGatewayStartedShards(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                                  ClusterService clusterService, TransportService transportService,
                                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, NodeEnvironment env) {
        super(settings, ACTION_NAME, clusterName, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                Request::new, NodeRequest::new, ThreadPool.Names.FETCH_SHARD_STARTED);
        this.nodeEnv = env;
    }

    @Override
    public void list(ShardId shardId, IndexMetaData indexMetaData, String[] nodesIds, ActionListener<NodesGatewayStartedShards> listener) {
        execute(new Request(shardId, indexMetaData.getIndexUUID(), nodesIds), listener);
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
    protected NodesGatewayStartedShards newResponse(Request request, AtomicReferenceArray responses) {
        final List<NodeGatewayStartedShards> nodesList = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeGatewayStartedShards) { // will also filter out null response for unallocated ones
                nodesList.add((NodeGatewayStartedShards) resp);
            } else if (resp instanceof FailedNodeException) {
                failures.add((FailedNodeException) resp);
            } else {
                logger.warn("unknown response type [{}], expected NodeLocalGatewayStartedShards or FailedNodeException", resp);
            }
        }
        return new NodesGatewayStartedShards(clusterName, nodesList.toArray(new NodeGatewayStartedShards[nodesList.size()]),
                failures.toArray(new FailedNodeException[failures.size()]));
    }

    @Override
    protected NodeGatewayStartedShards nodeOperation(NodeRequest request) {
        try {
            final ShardId shardId = request.getShardId();
            final String indexUUID = request.getIndexUUID();
            logger.trace("{} loading local shard state info", shardId);
            ShardStateMetaData shardStateMetaData = ShardStateMetaData.FORMAT.loadLatestState(logger, nodeEnv.availableShardPaths(request.shardId));
            if (shardStateMetaData != null) {
                final IndexMetaData metaData = clusterService.state().metaData().index(shardId.index().name()); // it's a mystery why this is sometimes null
                if (metaData != null) {
                    ShardPath shardPath = null;
                    try {
                        shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, metaData.settings());
                        if (shardPath == null) {
                            throw new IllegalStateException(shardId + " no shard path found");
                        }
                        Store.tryOpenIndex(shardPath.resolveIndex());
                    } catch (Exception exception) {
                        logger.trace("{} can't open index for shard [{}] in path [{}]", exception, shardId, shardStateMetaData, (shardPath != null) ? shardPath.resolveIndex() : "");
                        return new NodeGatewayStartedShards(clusterService.localNode(), shardStateMetaData.version, exception);
                    }
                }
                // old shard metadata doesn't have the actual index UUID so we need to check if the actual uuid in the metadata
                // is equal to IndexMetaData.INDEX_UUID_NA_VALUE otherwise this shard doesn't belong to the requested index.
                if (indexUUID.equals(shardStateMetaData.indexUUID) == false
                        && IndexMetaData.INDEX_UUID_NA_VALUE.equals(shardStateMetaData.indexUUID) == false) {
                    logger.warn("{} shard state info found but indexUUID didn't match expected [{}] actual [{}]", shardId, indexUUID, shardStateMetaData.indexUUID);
                } else {
                    logger.debug("{} shard state info found: [{}]", shardId, shardStateMetaData);
                    return new NodeGatewayStartedShards(clusterService.localNode(), shardStateMetaData.version);
                }
            }
            logger.trace("{} no local shard info found", shardId);
            return new NodeGatewayStartedShards(clusterService.localNode(), -1);
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
        private String indexUUID;

        public Request() {
        }

        public Request(ShardId shardId, String indexUUID, String[] nodesIds) {
            super(nodesIds);
            this.shardId = shardId;
            this.indexUUID = indexUUID;
        }


        public ShardId shardId() {
            return this.shardId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            indexUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(indexUUID);
        }

        public String getIndexUUID() {
            return indexUUID;
        }
    }

    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        private FailedNodeException[] failures;

        public NodesGatewayStartedShards(ClusterName clusterName, NodeGatewayStartedShards[] nodes, FailedNodeException[] failures) {
            super(clusterName, nodes);
            this.failures = failures;
        }

        @Override
        public FailedNodeException[] failures() {
            return failures;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            nodes = new NodeGatewayStartedShards[in.readVInt()];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = new NodeGatewayStartedShards();
                nodes[i].readFrom(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(nodes.length);
            for (NodeGatewayStartedShards response : nodes) {
                response.writeTo(out);
            }
        }
    }


    public static class NodeRequest extends BaseNodeRequest {

        private ShardId shardId;
        private String indexUUID;

        public NodeRequest() {
        }

        NodeRequest(String nodeId, TransportNodesListGatewayStartedShards.Request request) {
            super(request, nodeId);
            this.shardId = request.shardId();
            this.indexUUID = request.getIndexUUID();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            indexUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(indexUUID);
        }

        public ShardId getShardId() {
            return shardId;
        }

        public String getIndexUUID() {
            return indexUUID;
        }
    }

    public static class NodeGatewayStartedShards extends BaseNodeResponse {

        private long version = -1;
        private Throwable storeException = null;

        public NodeGatewayStartedShards() {
        }
        public NodeGatewayStartedShards(DiscoveryNode node, long version) {
            this(node, version, null);
        }

        public NodeGatewayStartedShards(DiscoveryNode node, long version, Throwable storeException) {
            super(node);
            this.version = version;
            this.storeException = storeException;
        }

        public long version() {
            return this.version;
        }

        public Throwable storeException() {
            return this.storeException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            version = in.readLong();
            if (in.readBoolean()) {
                storeException = in.readThrowable();
            }

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(version);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeThrowable(storeException);
            } else {
                out.writeBoolean(false);
            }
        }
    }
}
