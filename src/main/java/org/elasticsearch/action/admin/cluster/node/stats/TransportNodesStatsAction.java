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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (shay.banon)
 */
public class TransportNodesStatsAction extends TransportNodesOperationAction<NodesStatsRequest, NodesStatsResponse, TransportNodesStatsAction.NodeStatsRequest, NodeStats> {

    private final NodeService nodeService;

    @Inject public TransportNodesStatsAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                             ClusterService clusterService, TransportService transportService,
                                             NodeService nodeService) {
        super(settings, clusterName, threadPool, clusterService, transportService);
        this.nodeService = nodeService;
    }

    @Override protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Cluster.Node.STATS;
    }

    @Override protected String transportNodeAction() {
        return "/cluster/nodes/stats/node";
    }

    @Override protected NodesStatsResponse newResponse(NodesStatsRequest nodesInfoRequest, AtomicReferenceArray responses) {
        final List<NodeStats> nodeStats = Lists.newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeStats) {
                nodeStats.add((NodeStats) resp);
            }
        }
        return new NodesStatsResponse(clusterName, nodeStats.toArray(new NodeStats[nodeStats.size()]));
    }

    @Override protected NodesStatsRequest newRequest() {
        return new NodesStatsRequest();
    }

    @Override protected NodeStatsRequest newNodeRequest() {
        return new NodeStatsRequest();
    }

    @Override protected NodeStatsRequest newNodeRequest(String nodeId, NodesStatsRequest request) {
        return new NodeStatsRequest(nodeId);
    }

    @Override protected NodeStats newNodeResponse() {
        return new NodeStats();
    }

    @Override protected NodeStats nodeOperation(NodeStatsRequest request) throws ElasticSearchException {
        return nodeService.stats();
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    protected static class NodeStatsRequest extends NodeOperationRequest {

        private NodeStatsRequest() {
        }

        private NodeStatsRequest(String nodeId) {
            super(nodeId);
        }
    }
}