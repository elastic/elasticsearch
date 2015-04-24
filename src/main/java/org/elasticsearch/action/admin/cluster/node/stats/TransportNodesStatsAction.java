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

package org.elasticsearch.action.admin.cluster.node.stats;

import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportNodesStatsAction extends TransportNodesOperationAction<NodesStatsRequest, NodesStatsResponse, TransportNodesStatsAction.NodeStatsRequest, NodeStats> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesStatsAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                     ClusterService clusterService, TransportService transportService,
                                     NodeService nodeService, ActionFilters actionFilters) {
        super(settings, NodesStatsAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters,
                NodesStatsRequest.class, NodeStatsRequest.class, ThreadPool.Names.MANAGEMENT);
        this.nodeService = nodeService;
    }

    @Override
    protected NodesStatsResponse newResponse(NodesStatsRequest nodesInfoRequest, AtomicReferenceArray responses) {
        final List<NodeStats> nodeStats = Lists.newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeStats) {
                nodeStats.add((NodeStats) resp);
            }
        }
        return new NodesStatsResponse(clusterName, nodeStats.toArray(new NodeStats[nodeStats.size()]));
    }

    @Override
    protected NodeStatsRequest newNodeRequest(String nodeId, NodesStatsRequest request) {
        return new NodeStatsRequest(nodeId, request);
    }

    @Override
    protected NodeStats newNodeResponse() {
        return new NodeStats();
    }

    @Override
    protected NodeStats nodeOperation(NodeStatsRequest nodeStatsRequest) throws ElasticsearchException {
        NodesStatsRequest request = nodeStatsRequest.request;
        return nodeService.stats(request.indices(), request.os(), request.process(), request.jvm(), request.threadPool(), request.network(),
                request.fs(), request.transport(), request.http(), request.breaker());
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    static class NodeStatsRequest extends NodeOperationRequest {

        NodesStatsRequest request;

        NodeStatsRequest() {
        }

        NodeStatsRequest(String nodeId, NodesStatsRequest request) {
            super(request, nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new NodesStatsRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
