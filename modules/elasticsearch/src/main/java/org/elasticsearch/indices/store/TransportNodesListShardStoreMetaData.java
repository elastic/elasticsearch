/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indices.store;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.*;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (shay.banon)
 */
public class TransportNodesListShardStoreMetaData extends TransportNodesOperationAction<TransportNodesListShardStoreMetaData.Request, TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData, TransportNodesListShardStoreMetaData.NodeRequest, TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> {

    private final IndicesService indicesService;

    @Inject public TransportNodesListShardStoreMetaData(Settings settings, ClusterName clusterName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                                        IndicesService indicesService) {
        super(settings, clusterName, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    public ActionFuture<NodesStoreFilesMetaData> list(ShardId shardId, boolean onlyUnallocated, Set<String> nodesIds, @Nullable TimeValue timeout) {
        return execute(new Request(shardId, onlyUnallocated, nodesIds).timeout(timeout));
    }

    @Override protected String transportAction() {
        return "/cluster/nodes/indices/shard/store";
    }

    @Override protected String transportNodeAction() {
        return "/cluster/nodes/indices/shard/store/node";
    }

    @Override protected Request newRequest() {
        return new Request();
    }

    @Override protected NodeRequest newNodeRequest() {
        return new NodeRequest();
    }

    @Override protected NodeRequest newNodeRequest(String nodeId, Request request) {
        return new NodeRequest(nodeId, request.shardId, request.unallocated);
    }

    @Override protected NodeStoreFilesMetaData newNodeResponse() {
        return new NodeStoreFilesMetaData();
    }

    /**
     * We only need to ask data nodes for shard allocation information.
     */
    @Override protected String[] filterNodeIds(DiscoveryNodes nodes, String[] nodesIds) {
        List<String> dataNodeIds = Lists.newArrayList();
        for (String nodeId : nodesIds) {
            if (nodes.get(nodeId).dataNode()) {
                dataNodeIds.add(nodeId);
            }
        }
        return dataNodeIds.toArray(new String[dataNodeIds.size()]);
    }

    @Override protected NodesStoreFilesMetaData newResponse(Request request, AtomicReferenceArray responses) {
        final List<NodeStoreFilesMetaData> nodeStoreFilesMetaDatas = Lists.newArrayList();
        final List<FailedNodeException> failures = Lists.newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeStoreFilesMetaData) { // will also filter out null response for unallocated ones
                nodeStoreFilesMetaDatas.add((NodeStoreFilesMetaData) resp);
            } else if (resp instanceof FailedNodeException) {
                failures.add((FailedNodeException) resp);
            }
        }
        return new NodesStoreFilesMetaData(clusterName, nodeStoreFilesMetaDatas.toArray(new NodeStoreFilesMetaData[nodeStoreFilesMetaDatas.size()]),
                failures.toArray(new FailedNodeException[failures.size()]));
    }

    @Override protected NodeStoreFilesMetaData nodeOperation(NodeRequest request) throws ElasticSearchException {
        InternalIndexService indexService = (InternalIndexService) indicesService.indexServiceSafe(request.shardId.index().name());
        if (request.unallocated && indexService.hasShard(request.shardId.id())) {
            return new NodeStoreFilesMetaData(clusterService.state().nodes().localNode(), null);
        }
        try {
            return new NodeStoreFilesMetaData(clusterService.state().nodes().localNode(), indexService.store().listStoreMetaData(request.shardId));
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to list store metadata for shard [" + request.shardId + "]", e);
        }
    }

    @Override protected boolean accumulateExceptions() {
        return true;
    }

    static class Request extends NodesOperationRequest {

        private ShardId shardId;

        private boolean unallocated;

        public Request() {
        }

        public Request(ShardId shardId, boolean unallocated, Set<String> nodesIds) {
            super(nodesIds.toArray(new String[nodesIds.size()]));
            this.shardId = shardId;
            this.unallocated = unallocated;
        }

        public Request(ShardId shardId, boolean unallocated, String... nodesIds) {
            super(nodesIds);
            this.shardId = shardId;
            this.unallocated = unallocated;
        }

        @Override public Request timeout(TimeValue timeout) {
            super.timeout(timeout);
            return this;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            unallocated = in.readBoolean();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeBoolean(unallocated);
        }
    }

    public static class NodesStoreFilesMetaData extends NodesOperationResponse<NodeStoreFilesMetaData> {

        private FailedNodeException[] failures;

        NodesStoreFilesMetaData() {
        }

        public NodesStoreFilesMetaData(ClusterName clusterName, NodeStoreFilesMetaData[] nodes, FailedNodeException[] failures) {
            super(clusterName, nodes);
            this.failures = failures;
        }

        public FailedNodeException[] failures() {
            return failures;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            nodes = new NodeStoreFilesMetaData[in.readVInt()];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = NodeStoreFilesMetaData.readListShardStoreNodeOperationResponse(in);
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(nodes.length);
            for (NodeStoreFilesMetaData response : nodes) {
                response.writeTo(out);
            }
        }
    }


    static class NodeRequest extends NodeOperationRequest {

        private ShardId shardId;

        private boolean unallocated;

        NodeRequest() {
        }

        NodeRequest(String nodeId, ShardId shardId, boolean unallocated) {
            super(nodeId);
            this.shardId = shardId;
            this.unallocated = unallocated;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            unallocated = in.readBoolean();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeBoolean(unallocated);
        }
    }

    public static class NodeStoreFilesMetaData extends NodeOperationResponse {

        private IndexStore.StoreFilesMetaData storeFilesMetaData;

        NodeStoreFilesMetaData() {
        }

        public NodeStoreFilesMetaData(DiscoveryNode node, IndexStore.StoreFilesMetaData storeFilesMetaData) {
            super(node);
            this.storeFilesMetaData = storeFilesMetaData;
        }

        public IndexStore.StoreFilesMetaData storeFilesMetaData() {
            return storeFilesMetaData;
        }

        public static NodeStoreFilesMetaData readListShardStoreNodeOperationResponse(StreamInput in) throws IOException {
            NodeStoreFilesMetaData resp = new NodeStoreFilesMetaData();
            resp.readFrom(in);
            return resp;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.readBoolean()) {
                storeFilesMetaData = IndexStore.StoreFilesMetaData.readStoreFilesMetaData(in);
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (storeFilesMetaData == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                storeFilesMetaData.writeTo(out);
            }
        }
    }
}
