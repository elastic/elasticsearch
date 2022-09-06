/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.IsSafe;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.Result;

// TODO: should this instead extend TransportMasterNodeReadAction?
public class TransportPrevalidateNodeRemovalAction extends TransportAction<PrevalidateNodeRemovalRequest, PrevalidateNodeRemovalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPrevalidateNodeRemovalAction.class);

    private final NodeClient client;

    @Inject
    public TransportPrevalidateNodeRemovalAction(ActionFilters actionFilters, TransportService transportService, NodeClient client) {
        super(PrevalidateNodeRemovalAction.NAME, actionFilters, transportService.getTaskManager());
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, PrevalidateNodeRemovalRequest request, ActionListener<PrevalidateNodeRemovalResponse> listener) {
        // TODO: Need to set masterNodeTimeOut?
        client.admin().cluster().health(new ClusterHealthRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                doPrevalidation(request, clusterHealthResponse, listener);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("failed to get cluster health", e);
                listener.onFailure(e);
            }
        });
    }

    private void doPrevalidation(
        PrevalidateNodeRemovalRequest prevalidationRequest,
        ClusterHealthResponse clusterHealthResponse,
        ActionListener<PrevalidateNodeRemovalResponse> listener
    ) {
        switch (clusterHealthResponse.getStatus()) {
            case GREEN, YELLOW -> {
                Result overall = new Result(IsSafe.YES, "");
                Map<String, Result> nodeResults = prevalidationRequest.getNodeIds()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), id -> new Result(IsSafe.YES, "")));
                listener.onResponse(new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(overall, nodeResults)));
            }
            case RED -> {
                Result overall = new Result(IsSafe.UNKNOWN, "cluster health is RED");
                Map<String, Result> nodeResults = prevalidationRequest.getNodeIds()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), id -> new Result(IsSafe.UNKNOWN, "")));
                listener.onResponse(new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(overall, nodeResults)));
            }
        }
    }
}
