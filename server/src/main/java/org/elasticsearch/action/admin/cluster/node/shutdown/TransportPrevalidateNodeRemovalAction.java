/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.IsSafe;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.NodeResult;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.Result;

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
    ) {
        List<String> nodes = request.getNodeIds();
        try {
            Set<DiscoveryNode> concreteNodes = resolveNodes(nodes, state.nodes());
            request.setConcreteNodes(concreteNodes.toArray(new DiscoveryNode[0]));
            doPrevalidation(request, state, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public static Set<DiscoveryNode> resolveNodes(List<String> nodes, DiscoveryNodes discoveryNodes) {
        Set<DiscoveryNode> concreteNodes = new HashSet<>(nodes.size());
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
        if (concreteNodes.size() != nodes.size()) {
            throw new IllegalArgumentException(
                Strings.format("provided {} values for <nodes> which resolved to {} nodes", nodes.size(), concreteNodes.size())
            );
        }
        ;
        return concreteNodes;
    }

    @Override
    protected ClusterBlockException checkBlock(PrevalidateNodeRemovalRequest request, ClusterState state) {
        // Allow running this action even when there are blocks on the cluster
        return null;
    }

    private void doPrevalidation(
        PrevalidateNodeRemovalRequest prevalidationRequest,
        ClusterState clusterState,
        ActionListener<PrevalidateNodeRemovalResponse> listener
    ) {
        assert prevalidationRequest.getConcreteNodes() != null && prevalidationRequest.getConcreteNodes().isEmpty() == false;
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState);
        Metadata metadata = clusterState.metadata();
        switch (clusterStateHealth.getStatus()) {
            case GREEN, YELLOW -> {
                Result result = new Result(IsSafe.YES, "");
                List<NodeResult> nodes = prevalidationRequest.getConcreteNodes()
                    .stream()
                    .map(dn -> new NodeResult(dn.getName(), dn.getId(), dn.getExternalId(), new Result(IsSafe.YES, "")))
                    .toList();
                listener.onResponse(new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(result, nodes)));
            }
            case RED -> {
                Set<String> redIndices = clusterStateHealth.getIndices()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().getStatus() == ClusterHealthStatus.RED)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
                // If all red indices are searchable snapshot indices, it is safe to remove any node.
                Set<String> redNonSSIndices = redIndices.stream()
                    .map(metadata::index)
                    .filter(i -> i.isSearchableSnapshot() == false && i.isPartialSearchableSnapshot() == false)
                    .map(im -> im.getIndex().getName())
                    .collect(Collectors.toSet());
                Result result;
                List<NodeResult> nodes;
                if (redNonSSIndices.isEmpty()) {
                    result = new Result(IsSafe.YES, "all red indices are searchable snapshot indices");
                    nodes = prevalidationRequest.getConcreteNodes()
                        .stream()
                        .map(dn -> new NodeResult(dn.getName(), dn.getId(), dn.getExternalId(), new Result(IsSafe.YES, "")))
                        .toList();
                } else {
                    result = new Result(IsSafe.UNKNOWN, "cluster health is RED");
                    nodes = prevalidationRequest.getConcreteNodes()
                        .stream()
                        .map(dn -> new NodeResult(dn.getName(), dn.getId(), dn.getExternalId(), new Result(IsSafe.UNKNOWN, "")))
                        .toList();
                }
                listener.onResponse(new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(result, nodes)));
            }
        }
    }
}
