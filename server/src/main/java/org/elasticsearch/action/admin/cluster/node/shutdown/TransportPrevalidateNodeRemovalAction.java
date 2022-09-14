/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.IsSafe;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.Result;

// TODO: action with
public class TransportPrevalidateNodeRemovalAction extends TransportMasterNodeReadAction<
    PrevalidateNodeRemovalRequest,
    PrevalidateNodeRemovalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPrevalidateNodeRemovalAction.class);

    @Inject
    public TransportPrevalidateNodeRemovalAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PrevalidateNodeRemovalAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PrevalidateNodeRemovalRequest::new,
            indexNameExpressionResolver,
            PrevalidateNodeRemovalResponse::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        PrevalidateNodeRemovalRequest request,
        ClusterState state,
        ActionListener<PrevalidateNodeRemovalResponse> listener
    ) throws Exception {
        // TODO: Need to set masterNodeTimeOut
        List<String> nodes = request.getNodeIds();
        try {
            List<DiscoveryNode> concreteNodes = resolveNodes(nodes, state.nodes());
            request.setConcreteNodes(concreteNodes.toArray(new DiscoveryNode[0]));
            doPrevalidation(request, new ClusterStateHealth(state), listener);
        } catch (IllegalArgumentException e) {
            listener.onFailure(new ElasticsearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST));
        } catch (ResourceNotFoundException e) {
            listener.onFailure(e);
        }
    }

    public static List<DiscoveryNode> resolveNodes(List<String> nodes, DiscoveryNodes discoveryNodes) {
        List<DiscoveryNode> concreteNodes = new ArrayList<>(nodes.size());
        for (String node : nodes) {
            List<DiscoveryNode> matches = discoveryNodes.stream()
                .filter(dn -> dn.getId().equals(node) || dn.getName().equals(node) || dn.getExternalId().equals(node))
                .toList();
            if (matches.isEmpty()) {
                throw new ResourceNotFoundException("node [{}] not found", node);
            }
            if (matches.size() > 1) {
                throw new IllegalArgumentException("more than one node matches [" + node + "]");
            }
            concreteNodes.add(matches.get(0));
        }
        assert concreteNodes.size() == nodes.size();
        return concreteNodes;
    }

    @Override
    protected ClusterBlockException checkBlock(PrevalidateNodeRemovalRequest request, ClusterState state) {
        // Allow running this action even when there are blocks on the cluster
        return null;
    }

    private void doPrevalidation(
        PrevalidateNodeRemovalRequest prevalidationRequest,
        ClusterStateHealth clusterStateHealth,
        ActionListener<PrevalidateNodeRemovalResponse> listener
    ) {
        switch (clusterStateHealth.getStatus()) {
            case GREEN, YELLOW -> {
                Result overall = new Result(IsSafe.YES, "");
                Map<String, Result> nodeResults = prevalidationRequest.getNodeIds()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), id -> new Result(IsSafe.YES, "")));
                listener.onResponse(new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(overall, nodeResults)));
            }
            case RED -> {
                // TODO: search for RED indices which are searchable snapshot based.
                Result overall = new Result(IsSafe.UNKNOWN, "cluster health is RED");
                Map<String, Result> nodeResults = prevalidationRequest.getNodeIds()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), id -> new Result(IsSafe.UNKNOWN, "")));
                listener.onResponse(new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(overall, nodeResults)));
            }
        }
    }
}
