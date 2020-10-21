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

package org.elasticsearch.action.admin.indices.dangling.find;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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

/**
 * Finds a specified dangling index by its UUID, searching across all nodes.
 */
public class TransportFindDanglingIndexAction extends TransportNodesAction<
    FindDanglingIndexRequest,
    FindDanglingIndexResponse,
    NodeFindDanglingIndexRequest,
    NodeFindDanglingIndexResponse> {

    private final TransportService transportService;
    private final DanglingIndicesState danglingIndicesState;

    @Inject
    public TransportFindDanglingIndexAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DanglingIndicesState danglingIndicesState
    ) {
        super(
            FindDanglingIndexAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            FindDanglingIndexRequest::new,
            NodeFindDanglingIndexRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeFindDanglingIndexResponse.class
        );
        this.transportService = transportService;
        this.danglingIndicesState = danglingIndicesState;
    }

    @Override
    protected FindDanglingIndexResponse newResponse(
        FindDanglingIndexRequest request,
        List<NodeFindDanglingIndexResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new FindDanglingIndexResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeFindDanglingIndexRequest newNodeRequest(FindDanglingIndexRequest request) {
        return new NodeFindDanglingIndexRequest(request.getIndexUUID());
    }

    @Override
    protected NodeFindDanglingIndexResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeFindDanglingIndexResponse(in);
    }

    @Override
    protected NodeFindDanglingIndexResponse nodeOperation(NodeFindDanglingIndexRequest request, Task task) {
        final DiscoveryNode localNode = transportService.getLocalNode();
        final String indexUUID = request.getIndexUUID();

        final List<IndexMetadata> danglingIndexInfo = new ArrayList<>();

        for (IndexMetadata each : danglingIndicesState.getDanglingIndices().values()) {
            if (each.getIndexUUID().equals(indexUUID)) {
                danglingIndexInfo.add(each);
            }
        }

        return new NodeFindDanglingIndexResponse(localNode, danglingIndexInfo);
    }
}
