/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transport action for retrieving information regarding the cluster formation.
 * This will fetch the information from the requested nodes, and it'll likely be sourced from
 * {@link Coordinator}.
 */
public class TransportClusterFormationInfoAction extends TransportNodesAction<
    ClusterFormationInfoRequest,
    ClusterFormationInfoResponse,
    TransportClusterFormationInfoAction.ClusterFormationInfoTransportRequest,
    ClusterFormationInfoNodeResponse> {

    private final Coordinator coordinator;

    @Inject
    public TransportClusterFormationInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Coordinator coordinator
    ) {
        super(
            ClusterFormationInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClusterFormationInfoRequest::new,
            ClusterFormationInfoTransportRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ThreadPool.Names.MANAGEMENT,
            ClusterFormationInfoNodeResponse.class
        );
        this.coordinator = coordinator;
    }

    @Override
    protected ClusterFormationInfoResponse newResponse(ClusterFormationInfoRequest request,
                                                       List<ClusterFormationInfoNodeResponse> responses,
                                                       List<FailedNodeException> failures) {
        return new ClusterFormationInfoResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ClusterFormationInfoTransportRequest newNodeRequest(ClusterFormationInfoRequest request) {
        return new ClusterFormationInfoTransportRequest(request);
    }

    @Override
    protected ClusterFormationInfoNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new ClusterFormationInfoNodeResponse(in);
    }

    @Override
    protected ClusterFormationInfoNodeResponse nodeOperation(ClusterFormationInfoTransportRequest nodeRequest, Task task) {
        return new ClusterFormationInfoNodeResponse(
            clusterService.localNode(),
            coordinator.getClusterFormationWarning().orElse(null)
        );
    }

    public static class ClusterFormationInfoTransportRequest extends TransportRequest {

        ClusterFormationInfoRequest request;

        public ClusterFormationInfoTransportRequest(StreamInput in) throws IOException {
            super(in);
            request = new ClusterFormationInfoRequest(in);
        }

        ClusterFormationInfoTransportRequest(ClusterFormationInfoRequest request) {
            this.request = request;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

}
