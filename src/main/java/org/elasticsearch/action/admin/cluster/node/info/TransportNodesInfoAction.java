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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportNodesInfoAction extends TransportNodesOperationAction<NodesInfoRequest, NodesInfoResponse, TransportNodesInfoAction.NodeInfoRequest, NodeInfo> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesInfoAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                    ClusterService clusterService, TransportService transportService,
                                    NodeService nodeService, ActionFilters actionFilters) {
        super(settings, NodesInfoAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters,
                NodesInfoRequest.class, NodeInfoRequest.class, ThreadPool.Names.MANAGEMENT);
        this.nodeService = nodeService;
    }

    @Override
    protected NodesInfoResponse newResponse(NodesInfoRequest nodesInfoRequest, AtomicReferenceArray responses) {
        final List<NodeInfo> nodesInfos = new ArrayList<>();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeInfo) {
                nodesInfos.add((NodeInfo) resp);
            }
        }
        return new NodesInfoResponse(clusterName, nodesInfos.toArray(new NodeInfo[nodesInfos.size()]));
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
    protected NodeInfo nodeOperation(NodeInfoRequest nodeRequest) throws ElasticsearchException {
        NodesInfoRequest request = nodeRequest.request;
        return nodeService.info(request.settings(), request.os(), request.process(), request.jvm(), request.threadPool(),
                request.network(), request.transport(), request.http(), request.plugins());
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    static class NodeInfoRequest extends NodeOperationRequest {

        NodesInfoRequest request;

        NodeInfoRequest() {
        }

        NodeInfoRequest(String nodeId, NodesInfoRequest request) {
            super(request, nodeId);
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
