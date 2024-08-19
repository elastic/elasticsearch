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
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.NodeResult;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.Result;

public class TransportPrevalidateNodeRemovalAction extends TransportMasterNodeReadAction<
    PrevalidateNodeRemovalRequest,
    PrevalidateNodeRemovalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPrevalidateNodeRemovalAction.class);

    private final NodeClient client;

    @Inject
    public TransportPrevalidateNodeRemovalAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client
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
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        PrevalidateNodeRemovalRequest request,
        ClusterState state,
        ActionListener<PrevalidateNodeRemovalResponse> responseListener
    ) {
        ActionListener.run(responseListener, listener -> {
            Set<DiscoveryNode> requestNodes = resolveNodes(request, state.nodes());
            doPrevalidation(request, requestNodes, state, listener);
        });
    }

    public static Set<DiscoveryNode> resolveNodes(PrevalidateNodeRemovalRequest request, DiscoveryNodes discoveryNodes) {
        // Only one of the three arrays must be non-empty.
        assert Stream.of(request.getNames(), request.getIds(), request.getExternalIds())
            .filter(TransportPrevalidateNodeRemovalAction::notEmpty)
            .toList()
            .size() == 1;
        // Resolve by name
        if (notEmpty(request.getNames())) {
            logger.debug("resolving nodes for prevalidation using name");
            var names = new HashSet<>(Arrays.asList(request.getNames()));
            var resolvedNodes = discoveryNodes.stream().filter(n -> names.contains(n.getName())).collect(Collectors.toSet());
            if (resolvedNodes.size() < names.size()) {
                // find out which one wasn't found
                var existingNodeNames = discoveryNodes.stream().map(DiscoveryNode::getName).collect(Collectors.toSet());
                names.removeAll(existingNodeNames);
                throw new ResourceNotFoundException("could not resolve node names {}", names);
            }
            assert resolvedNodes.size() == request.getNames().length;
            return resolvedNodes;
        }
        // Resolve by ID
        if (notEmpty(request.getIds())) {
            logger.debug("resolving nodes for prevalidation using ID");
            var ids = request.getIds();
            var resolvedNode = Arrays.stream(ids).map(discoveryNodes::get).filter(Objects::nonNull).collect(Collectors.toSet());
            if (resolvedNode.size() < ids.length) {
                // find out which one wasn't found
                var existingNodeIds = discoveryNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
                var idsNotFound = Arrays.stream(ids).filter(id -> existingNodeIds.contains(id) == false).collect(Collectors.toSet());
                throw new ResourceNotFoundException("could not resolve node IDs {}", idsNotFound);
            }
            return resolvedNode;
        }
        // Resolve by external ID
        logger.debug("resolving nodes for prevalidation using external ID");
        var externalIds = new HashSet<>(Arrays.asList(request.getExternalIds()));
        var resolvedNodes = discoveryNodes.stream().filter(n -> externalIds.contains(n.getExternalId())).collect(Collectors.toSet());
        if (resolvedNodes.size() < externalIds.size()) {
            // find out which one wasn't found
            var existingExternalIds = discoveryNodes.stream().map(DiscoveryNode::getExternalId).collect(Collectors.toSet());
            externalIds.removeAll(existingExternalIds);
            throw new ResourceNotFoundException("could not resolve node external IDs {}", externalIds);
        }
        assert resolvedNodes.size() == request.getExternalIds().length;
        return resolvedNodes;
    }

    private static boolean notEmpty(String[] a) {
        return a != null && a.length > 0;
    }

    @Override
    protected ClusterBlockException checkBlock(PrevalidateNodeRemovalRequest request, ClusterState state) {
        // Allow running this action even when there are blocks on the cluster
        return null;
    }

    private void doPrevalidation(
        PrevalidateNodeRemovalRequest request,
        Set<DiscoveryNode> requestNodes,
        ClusterState clusterState,
        ActionListener<PrevalidateNodeRemovalResponse> listener
    ) {
        assert requestNodes != null && requestNodes.isEmpty() == false;

        logger.debug(() -> "prevalidate node removal for nodes " + requestNodes);
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState);
        Metadata metadata = clusterState.metadata();
        DiscoveryNodes clusterNodes = clusterState.getNodes();
        if (clusterStateHealth.getStatus() == ClusterHealthStatus.GREEN || clusterStateHealth.getStatus() == ClusterHealthStatus.YELLOW) {
            List<NodeResult> nodesResults = requestNodes.stream()
                .map(
                    dn -> new NodeResult(
                        dn.getName(),
                        dn.getId(),
                        dn.getExternalId(),
                        new Result(true, NodesRemovalPrevalidation.Reason.NO_PROBLEMS, "")
                    )
                )
                .toList();
            listener.onResponse(
                new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(true, "cluster status is not RED", nodesResults))
            );
            return;
        }
        // RED cluster state
        Set<String> redIndices = clusterStateHealth.getIndices()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getStatus() == ClusterHealthStatus.RED)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        // If all red indices are searchable snapshot indices, it is safe to remove any node.
        Set<String> redNonSSIndices = redIndices.stream()
            .map(metadata::index)
            .filter(i -> i.isSearchableSnapshot() == false)
            .map(im -> im.getIndex().getName())
            .collect(Collectors.toSet());
        if (redNonSSIndices.isEmpty()) {
            List<NodeResult> nodeResults = requestNodes.stream()
                .map(
                    dn -> new NodeResult(
                        dn.getName(),
                        dn.getId(),
                        dn.getExternalId(),
                        new Result(true, NodesRemovalPrevalidation.Reason.NO_RED_SHARDS_EXCEPT_SEARCHABLE_SNAPSHOTS, "")
                    )
                )
                .toList();
            listener.onResponse(
                new PrevalidateNodeRemovalResponse(
                    new NodesRemovalPrevalidation(true, "all red indices are searchable snapshot indices", nodeResults)
                )
            );
        } else {
            // Reach out to the nodes to find out whether they contain copies of the red non-searchable-snapshot indices
            Set<ShardId> redShards = clusterStateHealth.getIndices()
                .entrySet()
                .stream()
                .filter(indexHealthEntry -> redNonSSIndices.contains(indexHealthEntry.getKey()))
                .map(Map.Entry::getValue) // ClusterHealthIndex of red non-searchable-snapshot indices
                .flatMap(
                    redIndexHealth -> redIndexHealth.getShards()
                        .values()
                        .stream()
                        .filter(shardHealth -> shardHealth.getStatus() == ClusterHealthStatus.RED)
                        .map(redShardHealth -> Tuple.tuple(redIndexHealth.getIndex(), redShardHealth))
                ) // (Index, ClusterShardHealth) of all red shards
                .map(
                    redIndexShardHealthTuple -> new ShardId(
                        metadata.index(redIndexShardHealthTuple.v1()).getIndex(),
                        redIndexShardHealthTuple.v2().getShardId()
                    )
                ) // Convert to ShardId
                .collect(Collectors.toSet());
            var nodeIds = requestNodes.stream().map(DiscoveryNode::getId).toList().toArray(new String[0]);
            var checkShardsRequest = new PrevalidateShardPathRequest(redShards, nodeIds).timeout(request.timeout());
            client.execute(TransportPrevalidateShardPathAction.TYPE, checkShardsRequest, new ActionListener<>() {
                @Override
                public void onResponse(PrevalidateShardPathResponse response) {
                    listener.onResponse(new PrevalidateNodeRemovalResponse(createPrevalidationResult(clusterNodes, response)));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    private static NodesRemovalPrevalidation createPrevalidationResult(DiscoveryNodes nodes, PrevalidateShardPathResponse response) {
        List<NodeResult> nodeResults = new ArrayList<>(response.getNodes().size() + response.failures().size());
        for (NodePrevalidateShardPathResponse nodeResponse : response.getNodes()) {
            Result result;
            if (nodeResponse.getShardIds().isEmpty()) {
                result = new Result(true, NodesRemovalPrevalidation.Reason.NO_RED_SHARDS_ON_NODE, "");
            } else {
                result = new Result(
                    false,
                    NodesRemovalPrevalidation.Reason.RED_SHARDS_ON_NODE,
                    Strings.format("node contains copies of the following red shards: %s", nodeResponse.getShardIds())
                );
            }
            nodeResults.add(
                new NodeResult(
                    nodeResponse.getNode().getName(),
                    nodeResponse.getNode().getId(),
                    nodeResponse.getNode().getExternalId(),
                    result
                )
            );
        }
        for (FailedNodeException failedResponse : response.failures()) {
            DiscoveryNode node = nodes.get(failedResponse.nodeId());
            nodeResults.add(
                new NodeResult(
                    node.getName(),
                    node.getId(),
                    node.getExternalId(),
                    new Result(
                        false,
                        NodesRemovalPrevalidation.Reason.UNABLE_TO_VERIFY,
                        Strings.format("failed contacting the node: %s", failedResponse.getDetailedMessage())
                    )
                )
            );
        }
        // determine overall result from the node results.
        Set<String> unsafeNodeRemovals = response.getNodes()
            .stream()
            .filter(r -> r.getShardIds().isEmpty() == false)
            .map(r -> r.getNode().getId())
            .collect(Collectors.toSet());
        if (unsafeNodeRemovals.isEmpty() == false) {
            return new NodesRemovalPrevalidation(
                false,
                Strings.format("removal of the following nodes might not be safe: %s", unsafeNodeRemovals),
                nodeResults
            );
        }
        if (response.failures().isEmpty() == false) {
            Set<String> unknownNodeRemovals = response.failures().stream().map(FailedNodeException::nodeId).collect(Collectors.toSet());
            return new NodesRemovalPrevalidation(
                false,
                Strings.format("cannot prevalidate removal of nodes with the following IDs: %s", unknownNodeRemovals),
                nodeResults
            );
        }
        return new NodesRemovalPrevalidation(true, "", nodeResults);
    }
}
