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

package org.elasticsearch.action.admin.cluster.node.info;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportNodesInfo extends TransportNodesOperationAction<NodesInfoRequest, NodesInfoResponse, TransportNodesInfo.NodeInfoRequest, NodeInfo> {

    @Inject public TransportNodesInfo(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                      ClusterService clusterService, TransportService transportService) {
        super(settings, clusterName, threadPool, clusterService, transportService);
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Cluster.Node.INFO;
    }

    @Override protected String transportNodeAction() {
        return "/cluster/nodes/info/node";
    }

    @Override protected NodesInfoResponse newResponse(NodesInfoRequest nodesInfoRequest, AtomicReferenceArray responses) {
        final List<NodeInfo> nodesInfos = new ArrayList<NodeInfo>();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeInfo) {
                nodesInfos.add((NodeInfo) resp);
            }
        }
        return new NodesInfoResponse(clusterName, nodesInfos.toArray(new NodeInfo[nodesInfos.size()]));
    }

    @Override protected NodesInfoRequest newRequest() {
        return new NodesInfoRequest();
    }

    @Override protected NodeInfoRequest newNodeRequest() {
        return new NodeInfoRequest();
    }

    @Override protected NodeInfoRequest newNodeRequest(String nodeId, NodesInfoRequest request) {
        return new NodeInfoRequest(nodeId);
    }

    @Override protected NodeInfo newNodeResponse() {
        return new NodeInfo();
    }

    @Override protected NodeInfo nodeOperation(NodeInfoRequest nodeInfoRequest) throws ElasticSearchException {
        return new NodeInfo(clusterService.state().nodes().localNode());
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    protected static class NodeInfoRequest extends NodeOperationRequest {

        private NodeInfoRequest() {
        }

        private NodeInfoRequest(String nodeId) {
            super(nodeId);
        }
    }
}
