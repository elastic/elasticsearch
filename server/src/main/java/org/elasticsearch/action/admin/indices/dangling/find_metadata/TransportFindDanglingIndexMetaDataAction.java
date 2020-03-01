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

package org.elasticsearch.action.admin.indices.dangling.find_metadata;

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

/**
 * Finds a specified dangling index by its UUID, searching across all nodes.
 */
public class TransportFindDanglingIndexMetaDataAction extends TransportNodesAction<
    FindDanglingIndexMetaDataRequest,
    FindDanglingIndexMetaDataResponse,
    NodeFindDanglingIndexMetaDataRequest,
    NodeFindDanglingIndexMetaDataResponse> {

    private final TransportService transportService;
    private final DanglingIndicesState danglingIndicesState;

    @Inject
    public TransportFindDanglingIndexMetaDataAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DanglingIndicesState danglingIndicesState
    ) {
        super(
            FindDanglingIndexMetaDataAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            FindDanglingIndexMetaDataRequest::new,
            NodeFindDanglingIndexMetaDataRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeFindDanglingIndexMetaDataResponse.class
        );
        this.transportService = transportService;
        this.danglingIndicesState = danglingIndicesState;
    }

    @Override
    protected FindDanglingIndexMetaDataResponse newResponse(
        FindDanglingIndexMetaDataRequest request,
        List<NodeFindDanglingIndexMetaDataResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new FindDanglingIndexMetaDataResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeFindDanglingIndexMetaDataRequest newNodeRequest(FindDanglingIndexMetaDataRequest request) {
        return new NodeFindDanglingIndexMetaDataRequest(request.getIndexUUID());
    }

    @Override
    protected NodeFindDanglingIndexMetaDataResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeFindDanglingIndexMetaDataResponse(in);
    }

    @Override
    protected NodeFindDanglingIndexMetaDataResponse nodeOperation(NodeFindDanglingIndexMetaDataRequest request, Task task) {
        final DiscoveryNode localNode = transportService.getLocalNode();
        final String indexUUID = request.getIndexUUID();

        final List<IndexMetaData> danglingIndexInfo = new ArrayList<>();

        for (IndexMetaData each : danglingIndicesState.getDanglingIndices().values()) {
            if (each.getIndexUUID().equals(indexUUID)) {
                danglingIndexInfo.add(each);
            }
        }

        return new NodeFindDanglingIndexMetaDataResponse(localNode, danglingIndexInfo);
    }
}
