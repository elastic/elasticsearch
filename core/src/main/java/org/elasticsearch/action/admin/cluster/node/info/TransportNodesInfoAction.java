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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class TransportNodesInfoAction extends TransportNodesAction<NodesInfoRequest,
                                                                   NodesInfoResponse,
                                                                   TransportNodesInfoAction.NodeInfoRequest,
                                                                   NodeInfo> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesInfoAction(Settings settings, ThreadPool threadPool,
                                    ClusterService clusterService, TransportService transportService,
                                    NodeService nodeService, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, NodesInfoAction.NAME, threadPool, clusterService, transportService, actionFilters,
              indexNameExpressionResolver, NodesInfoRequest::new, NodeInfoRequest::new, ThreadPool.Names.MANAGEMENT, NodeInfo.class);
        this.nodeService = nodeService;
    }

    @Override
    protected NodesInfoResponse newResponse(NodesInfoRequest nodesInfoRequest,
                                            List<NodeInfo> responses, List<FailedNodeException> failures) {
        return new NodesInfoResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeInfoRequest newNodeRequest(String nodeId, NodesInfoRequest request) {
        return new NodeInfoRequest(nodeId, request);
    }

    @Override
    protected NodeInfo newNodeResponse() {
        return new NodeInfo();
    }

    @Override
    protected NodeInfo nodeOperation(NodeInfoRequest nodeRequest) {
        NodesInfoRequest request = nodeRequest.request;
        return nodeService.info(request.settings(), request.os(), request.process(), request.jvm(), request.threadPool(),
                request.transport(), request.http(), request.plugins(), request.ingest());
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    public static class NodeInfoRequest extends BaseNodeRequest {

        NodesInfoRequest request;

        public NodeInfoRequest() {
        }

        public NodeInfoRequest(String nodeId, NodesInfoRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new NodesInfoRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
