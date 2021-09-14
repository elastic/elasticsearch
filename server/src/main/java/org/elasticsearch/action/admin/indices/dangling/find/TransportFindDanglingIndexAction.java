/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
