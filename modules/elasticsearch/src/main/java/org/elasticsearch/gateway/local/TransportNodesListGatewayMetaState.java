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

package org.elasticsearch.gateway.local;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.*;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (shay.banon)
 */
public class TransportNodesListGatewayMetaState extends TransportNodesOperationAction<TransportNodesListGatewayMetaState.Request, TransportNodesListGatewayMetaState.NodesLocalGatewayMetaState, TransportNodesListGatewayMetaState.NodeRequest, TransportNodesListGatewayMetaState.NodeLocalGatewayMetaState> {

    private LocalGateway gateway;

    @Inject public TransportNodesListGatewayMetaState(Settings settings, ClusterName clusterName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
        super(settings, clusterName, threadPool, clusterService, transportService);
    }

    TransportNodesListGatewayMetaState initGateway(LocalGateway gateway) {
        this.gateway = gateway;
        return this;
    }

    public ActionFuture<NodesLocalGatewayMetaState> list(Set<String> nodesIds, @Nullable TimeValue timeout) {
        return execute(new Request(nodesIds).timeout(timeout));
    }

    @Override protected String executor() {
        return ThreadPool.Names.CACHED;
    }

    @Override protected String transportAction() {
        return "/gateway/local/meta-state";
    }

    @Override protected String transportNodeAction() {
        return "/gateway/local/meta-state/node";
    }

    @Override protected boolean transportCompress() {
        return true; // compress since the metadata can become large
    }

    @Override protected Request newRequest() {
        return new Request();
    }

    @Override protected NodeRequest newNodeRequest() {
        return new NodeRequest();
    }

    @Override protected NodeRequest newNodeRequest(String nodeId, Request request) {
        return new NodeRequest(nodeId);
    }

    @Override protected NodeLocalGatewayMetaState newNodeResponse() {
        return new NodeLocalGatewayMetaState();
    }

    @Override protected NodesLocalGatewayMetaState newResponse(Request request, AtomicReferenceArray responses) {
        final List<NodeLocalGatewayMetaState> nodesList = Lists.newArrayList();
        final List<FailedNodeException> failures = Lists.newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeLocalGatewayMetaState) { // will also filter out null response for unallocated ones
                nodesList.add((NodeLocalGatewayMetaState) resp);
            } else if (resp instanceof FailedNodeException) {
                failures.add((FailedNodeException) resp);
            }
        }
        return new NodesLocalGatewayMetaState(clusterName, nodesList.toArray(new NodeLocalGatewayMetaState[nodesList.size()]),
                failures.toArray(new FailedNodeException[failures.size()]));
    }

    @Override protected NodeLocalGatewayMetaState nodeOperation(NodeRequest request) throws ElasticSearchException {
        return new NodeLocalGatewayMetaState(clusterService.localNode(), gateway.currentMetaState());
    }

    @Override protected boolean accumulateExceptions() {
        return true;
    }

    static class Request extends NodesOperationRequest {

        public Request() {
        }

        public Request(Set<String> nodesIds) {
            super(nodesIds.toArray(new String[nodesIds.size()]));
        }

        @Override public Request timeout(TimeValue timeout) {
            super.timeout(timeout);
            return this;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodesLocalGatewayMetaState extends NodesOperationResponse<NodeLocalGatewayMetaState> {

        private FailedNodeException[] failures;

        NodesLocalGatewayMetaState() {
        }

        public NodesLocalGatewayMetaState(ClusterName clusterName, NodeLocalGatewayMetaState[] nodes, FailedNodeException[] failures) {
            super(clusterName, nodes);
            this.failures = failures;
        }

        public FailedNodeException[] failures() {
            return failures;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            nodes = new NodeLocalGatewayMetaState[in.readVInt()];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = new NodeLocalGatewayMetaState();
                nodes[i].readFrom(in);
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(nodes.length);
            for (NodeLocalGatewayMetaState response : nodes) {
                response.writeTo(out);
            }
        }
    }


    static class NodeRequest extends NodeOperationRequest {

        NodeRequest() {
        }

        NodeRequest(String nodeId) {
            super(nodeId);
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodeLocalGatewayMetaState extends NodeOperationResponse {

        private LocalGatewayMetaState state;

        NodeLocalGatewayMetaState() {
        }

        public NodeLocalGatewayMetaState(DiscoveryNode node, LocalGatewayMetaState state) {
            super(node);
            this.state = state;
        }

        public LocalGatewayMetaState state() {
            return state;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.readBoolean()) {
                state = LocalGatewayMetaState.Builder.readFrom(in);
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (state == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                LocalGatewayMetaState.Builder.writeTo(state, out);
            }
        }
    }
}
