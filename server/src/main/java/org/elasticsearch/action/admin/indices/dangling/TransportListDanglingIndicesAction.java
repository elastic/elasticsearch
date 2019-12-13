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

package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.gateway.DanglingIndicesState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportListDanglingIndicesAction extends TransportNodesAction<
    ListDanglingIndicesRequest,
    ListDanglingIndicesResponse,
    NodeDanglingIndicesRequest,
    NodeDanglingIndicesResponse> {
    private final TransportService transportService;
    private final DanglingIndicesState danglingIndicesState;

    @Inject
    public TransportListDanglingIndicesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DanglingIndicesState danglingIndicesState
    ) {
        super(
            ListDanglingIndicesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ListDanglingIndicesRequest::new,
            NodeDanglingIndicesRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeDanglingIndicesResponse.class
        );
        this.transportService = transportService;
        this.danglingIndicesState = danglingIndicesState;
    }

    @Override
    protected ListDanglingIndicesResponse newResponse(
        ListDanglingIndicesRequest request,
        List<NodeDanglingIndicesResponse> nodeDanglingIndicesResponses,
        List<FailedNodeException> failures
    ) {
        return new ListDanglingIndicesResponse(clusterService.getClusterName(), nodeDanglingIndicesResponses, failures);
    }

    @Override
    protected NodeDanglingIndicesRequest newNodeRequest(ListDanglingIndicesRequest request) {
        return new NodeDanglingIndicesRequest();
    }

    @Override
    protected NodeDanglingIndicesResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeDanglingIndicesResponse(in);
    }

    @Override
    protected NodeDanglingIndicesResponse nodeOperation(NodeDanglingIndicesRequest request, Task task) {
        final DiscoveryNode localNode = transportService.getLocalNode();

        final List<IndexMetaData> indexMetaData = new ArrayList<>(danglingIndicesState.getDanglingIndices().values());

        return new NodeDanglingIndicesResponse(localNode, indexMetaData);
    }
}
