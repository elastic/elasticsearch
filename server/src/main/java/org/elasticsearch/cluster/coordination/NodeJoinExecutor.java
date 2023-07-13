/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class NodeJoinExecutor implements ClusterStateTaskExecutor<JoinTask> {

    private static final Logger logger = LogManager.getLogger(NodeJoinExecutor.class);

    /**
     * The transport versions that are forbidden from joining a cluster with this node due to critical bugs with those versions
     */
    private static final Set<TransportVersion> FORBIDDEN_VERSIONS = Set.of();

    private final AllocationService allocationService;
    private final RerouteService rerouteService;
    private final Function<ClusterState, ClusterState> maybeReconfigureAfterMasterElection;

    public NodeJoinExecutor(AllocationService allocationService, RerouteService rerouteService) {
        this(allocationService, rerouteService, Function.identity());
    }

    public NodeJoinExecutor(
        AllocationService allocationService,
        RerouteService rerouteService,
        Function<ClusterState, ClusterState> maybeReconfigureAfterMasterElection
    ) {
        this.allocationService = allocationService;
        this.rerouteService = rerouteService;
        this.maybeReconfigureAfterMasterElection = maybeReconfigureAfterMasterElection;
    }

    @Override
    public ClusterState execute(BatchExecutionContext<JoinTask> batchExecutionContext) throws Exception {
        // The current state that MasterService uses might have been updated by a (different) master in a higher term already. If so, stop
        // processing the current cluster state update, there's no point in continuing to compute it as it will later be rejected by
        // Coordinator#publish anyhow.
        assert batchExecutionContext.taskContexts().isEmpty() == false : "Expected to have non empty join tasks list";

        var term = batchExecutionContext.taskContexts().stream().mapToLong(t -> t.getTask().term()).max().getAsLong();

        var split = batchExecutionContext.taskContexts().stream().collect(Collectors.partitioningBy(t -> t.getTask().term() == term));
        for (TaskContext<JoinTask> outdated : split.get(false)) {
            outdated.onFailure(
                new NotMasterException("Higher term encountered (encountered: " + term + " > used: " + outdated.getTask().term() + ")")
            );
        }

        final var joinTaskContexts = split.get(true);
        final var initialState = batchExecutionContext.initialState();

        if (initialState.term() > term) {
            logger.trace("encountered higher term {} than current {}, there is a newer master", initialState.term(), term);
            throw new NotMasterException(
                "Higher term encountered (current: " + initialState.term() + " > used: " + term + "), there is a newer master"
            );
        }

        final boolean isBecomingMaster = joinTaskContexts.stream().anyMatch(t -> t.getTask().isBecomingMaster());
        final DiscoveryNodes currentNodes = initialState.nodes();
        boolean nodesChanged = false;
        ClusterState.Builder newState;

        if (currentNodes.getMasterNode() == null && isBecomingMaster) {
            assert initialState.term() < term : "there should be at most one become master task per election (= by term)";
            // use these joins to try and become the master.
            // Note that we don't have to do any validation of the amount of joining nodes - the commit
            // during the cluster state publishing guarantees that we have enough
            try (var ignored = batchExecutionContext.dropHeadersContext()) {
                // suppress deprecation warnings e.g. from reroute()
                newState = becomeMasterAndTrimConflictingNodes(initialState, joinTaskContexts, term);
            }
            nodesChanged = true;
        } else if (currentNodes.isLocalNodeElectedMaster()) {
            assert initialState.term() == term : "term should be stable for the same master";
            newState = ClusterState.builder(initialState);
        } else {
            logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
            throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
        }

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());
        Map<String, TransportVersion> transportVersions = new HashMap<>(newState.transportVersions());

        assert nodesBuilder.isLocalNodeElectedMaster();

        Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
        Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
        // if the cluster is not fully-formed then the min version is not meaningful
        final boolean enforceVersionBarrier = initialState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
        // processing any joins
        Map<String, String> joinedNodeIdsByNodeName = new HashMap<>();
        for (final var joinTaskContext : joinTaskContexts) {
            final var joinTask = joinTaskContext.getTask();
            final List<Runnable> onTaskSuccess = new ArrayList<>(joinTask.nodeCount());
            for (final JoinTask.NodeJoinTask nodeJoinTask : joinTask.nodeJoinTasks()) {
                final DiscoveryNode node = nodeJoinTask.node();
                if (currentNodes.nodeExistsWithSameRoles(node)) {
                    logger.debug("received a join request for an existing node [{}]", node);
                } else {
                    try {
                        TransportVersion transportVersion = nodeJoinTask.transportVersion();
                        if (enforceVersionBarrier) {
                            ensureVersionBarrier(node.getVersion(), minClusterNodeVersion);
                            ensureTransportVersionBarrier(transportVersion, transportVersions.values());
                        }
                        blockForbiddenVersions(transportVersion);
                        ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                        // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                        // we have to reject nodes that don't support all indices we have in this cluster
                        ensureIndexCompatibility(node.getMinIndexVersion(), node.getMaxIndexVersion(), initialState.getMetadata());
                        nodesBuilder.add(node);
                        transportVersions.put(node.getId(), transportVersion);
                        nodesChanged = true;
                        minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                        maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                        if (node.isMasterNode()) {
                            joinedNodeIdsByNodeName.put(node.getName(), node.getId());
                        }
                    } catch (IllegalArgumentException | IllegalStateException e) {
                        onTaskSuccess.add(() -> nodeJoinTask.listener().onFailure(e));
                        continue;
                    }
                }
                onTaskSuccess.add(() -> {
                    final var reason = nodeJoinTask.reason();
                    if (reason.guidanceDocs() == null) {
                        logger.info(
                            "node-join: [{}] with reason [{}]",
                            nodeJoinTask.node().descriptionWithoutAttributes(),
                            reason.message()
                        );
                    } else {
                        logger.warn(
                            "node-join: [{}] with reason [{}]; for troubleshooting guidance, see {}",
                            nodeJoinTask.node().descriptionWithoutAttributes(),
                            reason.message(),
                            reason.guidanceDocs()
                        );
                    }
                    nodeJoinTask.listener().onResponse(null);
                });
            }
            joinTaskContext.success(() -> {
                for (Runnable joinCompleter : onTaskSuccess) {
                    joinCompleter.run();
                }
            });
        }

        if (nodesChanged) {
            rerouteService.reroute(
                "post-join reroute",
                Priority.HIGH,
                ActionListener.wrap(r -> logger.trace("post-join reroute completed"), e -> logger.debug("post-join reroute failed", e))
            );

            if (joinedNodeIdsByNodeName.isEmpty() == false) {
                final var currentVotingConfigExclusions = initialState.getVotingConfigExclusions();
                final var newVotingConfigExclusions = currentVotingConfigExclusions.stream().map(e -> {
                    // Update nodeId in VotingConfigExclusion when a new node with excluded node name joins
                    if (CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER.equals(e.getNodeId())
                        && joinedNodeIdsByNodeName.containsKey(e.getNodeName())) {
                        return new CoordinationMetadata.VotingConfigExclusion(
                            joinedNodeIdsByNodeName.get(e.getNodeName()),
                            e.getNodeName()
                        );
                    } else {
                        return e;
                    }
                }).collect(Collectors.toSet());

                // if VotingConfigExclusions did get updated
                if (newVotingConfigExclusions.equals(currentVotingConfigExclusions) == false) {
                    final var coordMetadataBuilder = CoordinationMetadata.builder(initialState.coordinationMetadata())
                        .term(term)
                        .clearVotingConfigExclusions();
                    newVotingConfigExclusions.forEach(coordMetadataBuilder::addVotingConfigExclusion);
                    newState.metadata(Metadata.builder(initialState.metadata()).coordinationMetadata(coordMetadataBuilder.build()).build());
                }
            }

            final ClusterState clusterStateWithNewNodesAndDesiredNodes = DesiredNodes.updateDesiredNodesStatusIfNeeded(
                newState.nodes(nodesBuilder).transportVersions(transportVersions).build()
            );
            final ClusterState updatedState = allocationService.adaptAutoExpandReplicas(clusterStateWithNewNodesAndDesiredNodes);
            assert enforceVersionBarrier == false
                || updatedState.nodes().getMinNodeVersion().onOrAfter(initialState.nodes().getMinNodeVersion())
                : "min node version decreased from ["
                    + initialState.nodes().getMinNodeVersion()
                    + "] to ["
                    + updatedState.nodes().getMinNodeVersion()
                    + "]";
            return updatedState;
        } else {
            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize its join and set us as a master
            return newState.build();
        }
    }

    @SuppressForbidden(reason = "maintaining ClusterState#transportVersions requires reading them")
    private static Map<String, TransportVersion> getTransportVersions(ClusterState clusterState) {
        return clusterState.transportVersions();
    }

    protected ClusterState.Builder becomeMasterAndTrimConflictingNodes(
        ClusterState currentState,
        List<? extends TaskContext<JoinTask>> taskContexts,
        long term
    ) {
        final ClusterState initialState = currentState;
        currentState = taskContexts.stream()
            .map(TaskContext::getTask)
            .map(JoinTask::initialState)
            .filter(Objects::nonNull)
            .max(Comparator.comparingLong(ClusterState::term).thenComparingLong(ClusterState::version))
            .filter(
                clusterState -> clusterState.term() > initialState.term()
                    || (clusterState.term() == initialState.term() && clusterState.version() > initialState.version())
            )
            .orElse(currentState);

        assert currentState.nodes().getMasterNodeId() == null : currentState;
        assert currentState.term() < term : term + " vs " + currentState;
        DiscoveryNodes currentNodes = currentState.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        Map<String, TransportVersion> transportVersions = new HashMap<>(getTransportVersions(currentState));
        nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

        for (final var taskContext : taskContexts) {
            for (final var joiningNode : taskContext.getTask().nodes()) {
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                    transportVersions.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    logger.debug(
                        "removing existing node [{}], which conflicts with incoming join from [{}]",
                        nodeWithSameAddress,
                        joiningNode
                    );
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                    transportVersions.remove(nodeWithSameAddress.getId());
                }
            }
        }

        // now trim any left over dead nodes - either left there when the previous master stepped down
        // or removed by us above
        ClusterState tmpState = ClusterState.builder(currentState)
            .nodes(nodesBuilder)
            .transportVersions(transportVersions)
            .blocks(ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ID))
            .metadata(
                Metadata.builder(currentState.metadata())
                    .coordinationMetadata(CoordinationMetadata.builder(currentState.coordinationMetadata()).term(term).build())
                    .build()
            )
            .build();
        logger.trace("becomeMasterAndTrimConflictingNodes: {}", tmpState.nodes());
        allocationService.cleanCaches();
        tmpState = PersistentTasksCustomMetadata.disassociateDeadNodes(tmpState);
        tmpState = maybeReconfigureAfterMasterElection.apply(tmpState);
        return ClusterState.builder(allocationService.disassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
    }

    @Override
    public boolean runOnlyOnMaster() {
        // we validate that we are allowed to change the cluster state during cluster state processing
        return false;
    }

    private static void blockForbiddenVersions(TransportVersion joiningTransportVersion) {
        if (FORBIDDEN_VERSIONS.contains(joiningTransportVersion)) {
            throw new IllegalStateException(
                "A node with transport version " + joiningTransportVersion + " is forbidden from joining this cluster"
            );
        }
    }

    /**
     * Ensures that all indices are compatible with the given index version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of elasticsearch as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     * @see IndexVersion#MINIMUM_COMPATIBLE
     * @throws IllegalStateException if any index is incompatible with the given version
     */
    public static void ensureIndexCompatibility(IndexVersion minSupportedVersion, IndexVersion maxSupportedVersion, Metadata metadata) {
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetadata idxMetadata : metadata) {
            if (idxMetadata.getCompatibilityVersion().after(maxSupportedVersion)) {
                throw new IllegalStateException(
                    "index "
                        + idxMetadata.getIndex()
                        + " version not supported: "
                        + idxMetadata.getCompatibilityVersion()
                        + " maximum compatible index version is: "
                        + maxSupportedVersion
                );
            }
            if (idxMetadata.getCompatibilityVersion().before(minSupportedVersion)) {
                throw new IllegalStateException(
                    "index "
                        + idxMetadata.getIndex()
                        + " version not supported: "
                        + idxMetadata.getCompatibilityVersion()
                        + " minimum compatible index version is: "
                        + minSupportedVersion
                );
            }
        }
    }

    /** ensures that the joining node has a version that's compatible with all current nodes*/
    public static void ensureNodesCompatibility(final Version joiningNodeVersion, DiscoveryNodes currentNodes) {
        final Version minNodeVersion = currentNodes.getMinNodeVersion();
        final Version maxNodeVersion = currentNodes.getMaxNodeVersion();
        ensureNodesCompatibility(joiningNodeVersion, minNodeVersion, maxNodeVersion);
    }

    /** ensures that the joining node has a version that's compatible with a given version range */
    public static void ensureNodesCompatibility(Version joiningNodeVersion, Version minClusterNodeVersion, Version maxClusterNodeVersion) {
        assert minClusterNodeVersion.onOrBefore(maxClusterNodeVersion) : minClusterNodeVersion + " > " + maxClusterNodeVersion;
        if (joiningNodeVersion.isCompatible(maxClusterNodeVersion) == false) {
            throw new IllegalStateException(
                "node version ["
                    + joiningNodeVersion
                    + "] is not supported. "
                    + "The cluster contains nodes with version ["
                    + maxClusterNodeVersion
                    + "], which is incompatible."
            );
        }
        if (joiningNodeVersion.isCompatible(minClusterNodeVersion) == false) {
            throw new IllegalStateException(
                "node version ["
                    + joiningNodeVersion
                    + "] is not supported."
                    + "The cluster contains nodes with version ["
                    + minClusterNodeVersion
                    + "], which is incompatible."
            );
        }
    }

    /**
     * ensures that the joining node's transport version is equal or higher to the minClusterTransportVersion. This is needed
     * to ensure that the minimum transport version of the cluster doesn't go backwards.
     **/
    static void ensureTransportVersionBarrier(
        TransportVersion joiningTransportVersion,
        Collection<TransportVersion> existingTransportVersions
    ) {
        TransportVersion minClusterTransportVersion = existingTransportVersions.stream()
            .min(Comparator.naturalOrder())
            .orElse(TransportVersion.current());
        if (joiningTransportVersion.before(minClusterTransportVersion)) {
            throw new IllegalStateException(
                "node with transport version ["
                    + joiningTransportVersion
                    + "] may not join a cluster with minimum transport version ["
                    + minClusterTransportVersion
                    + "]"
            );
        }
    }

    /**
     * ensures that the joining node's version is equal or higher to the minClusterNodeVersion. This is needed
     * to ensure that if the master is already fully operating under the new version, it doesn't go back to mixed
     * version mode
     **/
    public static void ensureVersionBarrier(Version joiningNodeVersion, Version minClusterNodeVersion) {
        if (joiningNodeVersion.before(minClusterNodeVersion)) {
            throw new IllegalStateException(
                "node version ["
                    + joiningNodeVersion
                    + "] may not join a cluster comprising only nodes of version ["
                    + minClusterNodeVersion
                    + "] or greater"
            );
        }
    }

    public static Collection<BiConsumer<DiscoveryNode, ClusterState>> addBuiltInJoinValidators(
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators
    ) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
        validators.add((node, state) -> {
            ensureNodesCompatibility(node.getVersion(), state.getNodes());
            ensureIndexCompatibility(node.getMinIndexVersion(), node.getMaxIndexVersion(), state.getMetadata());
        });
        validators.addAll(onJoinValidators);
        return Collections.unmodifiableCollection(validators);
    }
}
