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
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportNodesListGatewayMetaState extends TransportNodesAction<TransportNodesListGatewayMetaState.Request, TransportNodesListGatewayMetaState.NodesGatewayMetaState, TransportNodesListGatewayMetaState.NodeRequest, TransportNodesListGatewayMetaState.NodeGatewayMetaState> {

    public static final String ACTION_NAME = "internal:gateway/local/meta_state";

    private GatewayMetaState metaState;

    @Inject
    public TransportNodesListGatewayMetaState(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                              ClusterService clusterService, TransportService transportService,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, clusterName, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, Request::new, NodeRequest::new, ThreadPool.Names.GENERIC);
    }

    TransportNodesListGatewayMetaState init(GatewayMetaState metaState) {
        this.metaState = metaState;
        return this;
    }

    public ActionFuture<NodesGatewayMetaState> list(String[] nodesIds, @Nullable TimeValue timeout) {
        return execute(new Request(nodesIds).timeout(timeout));
    }

    @Override
    protected boolean transportCompress() {
        return true; // compress since the metadata can become large
    }

    @Override
    protected NodeRequest newNodeRequest(String nodeId, Request request) {
        return new NodeRequest(nodeId, request);
    }

    @Override
    protected NodeGatewayMetaState newNodeResponse() {
        return new NodeGatewayMetaState();
    }

    @Override
    protected NodesGatewayMetaState newResponse(Request request, AtomicReferenceArray responses) {
        final List<NodeGatewayMetaState> nodesList = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeGatewayMetaState) { // will also filter out null response for unallocated ones
                nodesList.add((NodeGatewayMetaState) resp);
            } else if (resp instanceof FailedNodeException) {
                failures.add((FailedNodeException) resp);
            } else {
                logger.warn("unknown response type [{}], expected NodeLocalGatewayMetaState or FailedNodeException", resp);
            }
        }
        return new NodesGatewayMetaState(clusterName, nodesList.toArray(new NodeGatewayMetaState[nodesList.size()]),
                failures.toArray(new FailedNodeException[failures.size()]));
    }

    @Override
    protected NodeGatewayMetaState nodeOperation(NodeRequest request) {
        try {
            return new NodeGatewayMetaState(clusterService.localNode(), metaState.loadMetaState());
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load metadata", e);
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }

    public static class Request extends BaseNodesRequest<Request> {

        public Request() {
        }

        public Request(String... nodesIds) {
            super(nodesIds);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodesGatewayMetaState extends BaseNodesResponse<NodeGatewayMetaState> {

        private FailedNodeException[] failures;

        NodesGatewayMetaState() {
        }

        public NodesGatewayMetaState(ClusterName clusterName, NodeGatewayMetaState[] nodes, FailedNodeException[] failures) {
            super(clusterName, nodes);
            this.failures = failures;
        }

        public FailedNodeException[] failures() {
            return failures;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            nodes = new NodeGatewayMetaState[in.readVInt()];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = new NodeGatewayMetaState();
                nodes[i].readFrom(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(nodes.length);
            for (NodeGatewayMetaState response : nodes) {
                response.writeTo(out);
            }
        }
    }


    public static class NodeRequest extends BaseNodeRequest {

        public NodeRequest() {
        }

        NodeRequest(String nodeId, TransportNodesListGatewayMetaState.Request request) {
            super(request, nodeId);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodeGatewayMetaState extends BaseNodeResponse {

        private MetaData metaData;

        NodeGatewayMetaState() {
        }

        public NodeGatewayMetaState(DiscoveryNode node, MetaData metaData) {
            super(node);
            this.metaData = metaData;
        }

        public MetaData metaData() {
            return metaData;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.readBoolean()) {
                metaData = MetaData.Builder.readFrom(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (metaData == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                metaData.writeTo(out);
            }
        }
    }
}
