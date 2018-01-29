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

package org.elasticsearch.action.admin.cluster.reinit;

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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodesReInitAction extends TransportNodesAction<NodesReInitRequest,
                                                                    NodesReInitResponse,
                                                                    TransportNodesReInitAction.NodeRequest,
                                                                    NodesReInitResponse.NodeResponse> {

    @Inject
    public TransportNodesReInitAction(Settings settings, ThreadPool threadPool,
                                     ClusterService clusterService, TransportService transportService,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, NodesReInitAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                NodesReInitRequest::new, NodeRequest::new, ThreadPool.Names.MANAGEMENT, NodesReInitResponse.NodeResponse.class);
    }

    @Override
    protected NodesReInitResponse newResponse(NodesReInitRequest request, List<NodesReInitResponse.NodeResponse> responses, List<FailedNodeException> failures) {
        return new NodesReInitResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected TransportNodesReInitAction.NodeRequest newNodeRequest(String nodeId, NodesReInitRequest request) {
        return new NodeRequest(nodeId, request);
    }

    @Override
    protected NodesReInitResponse.NodeResponse newNodeResponse() {
        return new NodesReInitResponse.NodeResponse();
    }

    @Override
    protected NodesReInitResponse.NodeResponse nodeOperation(TransportNodesReInitAction.NodeRequest nodeStatsRequest) {
        final NodesReInitRequest request = nodeStatsRequest.request;
        return new NodesReInitResponse.NodeResponse(clusterService.localNode());
    }

    public static class NodeRequest extends BaseNodeRequest {

        NodesReInitRequest request;

        public NodeRequest() {
        }

        NodeRequest(String nodeId, NodesReInitRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new NodesReInitRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
