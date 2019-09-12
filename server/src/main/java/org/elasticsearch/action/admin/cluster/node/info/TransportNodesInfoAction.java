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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodesInfoAction extends TransportNodesAction<NodesInfoRequest,
                                                                   NodesInfoResponse,
                                                                   TransportNodesInfoAction.NodeInfoRequest,
                                                                   NodeInfo> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesInfoAction(ThreadPool threadPool, ClusterService clusterService,
                                    TransportService transportService, NodeService nodeService, ActionFilters actionFilters) {
        super(NodesInfoAction.NAME, threadPool, clusterService, transportService, actionFilters,
            NodesInfoRequest::new, NodeInfoRequest::new, ThreadPool.Names.MANAGEMENT, NodeInfo.class);
        this.nodeService = nodeService;
    }

    @Override
    protected NodesInfoResponse newResponse(NodesInfoRequest nodesInfoRequest,
                                            List<NodeInfo> responses, List<FailedNodeException> failures) {
        return new NodesInfoResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeInfoRequest newNodeRequest(NodesInfoRequest request) {
        return new NodeInfoRequest(request);
    }

    @Override
    protected NodeInfo newNodeResponse(StreamInput in) throws IOException {
        return new NodeInfo(in);
    }

    @Override
    protected NodeInfo nodeOperation(NodeInfoRequest nodeRequest, Task task) {
        NodesInfoRequest request = nodeRequest.request;
        return nodeService.info(request.settings(), request.os(), request.process(), request.jvm(), request.threadPool(),
                request.transport(), request.http(), request.plugins(), request.ingest(), request.indices());
    }

    public static class NodeInfoRequest extends BaseNodeRequest {

        NodesInfoRequest request;

        public NodeInfoRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesInfoRequest(in);
        }

        public NodeInfoRequest(NodesInfoRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
