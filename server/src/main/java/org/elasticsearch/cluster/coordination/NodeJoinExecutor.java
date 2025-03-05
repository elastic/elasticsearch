/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterFeatures;
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
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadataVerifier.isReadOnlySupportedVersion;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class NodeJoinExecutor implements ClusterStateTaskExecutor<JoinTask> {

    private static final Logger logger = LogManager.getLogger(NodeJoinExecutor.class);

    /**
     * The transport versions that are forbidden from joining a cluster with this node due to critical bugs with those versions
     */
    private static final Set<TransportVersion> FORBIDDEN_VERSIONS = Set.of();

    private final AllocationService allocationService;
    private final RerouteService rerouteService;
    private final FeatureService featureService;
    private final Function<ClusterState, ClusterState> maybeReconfigureAfterMasterElection;

    public NodeJoinExecutor(AllocationService allocationService, RerouteService rerouteService, FeatureService featureService) {
        this(allocationService, rerouteService, featureService, Function.identity());
    }

    public NodeJoinExecutor(
        AllocationService allocationService,
        RerouteService rerouteService,
        FeatureService featureService,
        Function<ClusterState, ClusterState> maybeReconfigureAfterMasterElection
    ) {
        this.allocationService = allocationService;
        this.rerouteService = rerouteService;
        this.featureService = featureService;
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
            throw new NotMasterException(
                Strings.format(
                    "Node [%s] not master for join request. Current known master [%s], current term [%d]",
                    currentNodes.getLocalNode(),
                    currentNodes.getMasterNode(),
                    term
                )
            );
        }

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());
        Map<String, CompatibilityVersions> compatibilityVersionsMap = new HashMap<>(newState.compatibilityVersions());
        Map<String, Set<String>> nodeFeatures = new HashMap<>(newState.nodeFeatures()); // as present in cluster state
        Set<String> effectiveClusterFeatures = calculateEffectiveClusterFeatures(newState.nodes(), nodeFeatures);

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

                    // update the node's feature set if it has one
                    // this can happen if the master has just moved from a pre-features version to a post-features version
                    assert Version.V_8_12_0.onOrBefore(Version.CURRENT) : "This can be removed once 8.12.0 is no longer a valid version";
                    if (Objects.equals(nodeFeatures.get(node.getId()), nodeJoinTask.features()) == false) {
                        logger.debug("updating node [{}] features {}", node.getId(), nodeJoinTask.features());
                        nodeFeatures.put(node.getId(), nodeJoinTask.features());
                        nodesChanged = true;
                    }
                } else {
                    try {
                        CompatibilityVersions compatibilityVersions = nodeJoinTask.compatibilityVersions();
                        Set<String> features = nodeJoinTask.features();
                        if (enforceVersionBarrier) {
                            ensureVersionBarrier(node.getVersion(), minClusterNodeVersion);
                            CompatibilityVersions.ensureVersionsCompatibility(compatibilityVersions, compatibilityVersionsMap.values());
                        }
                        blockForbiddenVersions(compatibilityVersions.transportVersion());
                        ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                        Set<String> newNodeEffectiveFeatures = enforceNodeFeatureBarrier(node, effectiveClusterFeatures, features);
                        // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                        // we have to reject nodes that don't support all indices we have in this cluster
                        ensureIndexCompatibility(
                            node.getMinIndexVersion(),
                            node.getMinReadOnlyIndexVersion(),
                            node.getMaxIndexVersion(),
                            initialState.getMetadata()
                        );

                        nodesBuilder.add(node);
                        compatibilityVersionsMap.put(node.getId(), compatibilityVersions);
                        // store the actual node features here, not including assumed features, as this is persisted in cluster state
                        nodeFeatures.put(node.getId(), features);
                        effectiveClusterFeatures.retainAll(newNodeEffectiveFeatures);

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
                newState.nodes(nodesBuilder).nodeIdsToCompatibilityVersions(compatibilityVersionsMap).nodeFeatures(nodeFeatures).build()
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

        ClusterState.Builder builder = ClusterState.builder(currentState);

        DiscoveryNodes currentNodes = builder.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        Map<String, CompatibilityVersions> compatibilityVersions = new HashMap<>(builder.compatibilityVersions());
        Map<String, Set<String>> nodeFeatures = new HashMap<>(builder.nodeFeatures());
        nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());
        nodesBuilder.resetNodeLeftGeneration();

        for (final var taskContext : taskContexts) {
            for (final var joiningNode : taskContext.getTask().nodes()) {
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                    compatibilityVersions.remove(nodeWithSameId.getId());
                    nodeFeatures.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    logger.debug(
                        "removing existing node [{}], which conflicts with incoming join from [{}]",
                        nodeWithSameAddress,
                        joiningNode
                    );
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                    compatibilityVersions.remove(nodeWithSameAddress.getId());
                    nodeFeatures.remove(nodeWithSameAddress.getId());
                }
            }
        }

        // now trim any left over dead nodes - either left there when the previous master stepped down
        // or removed by us above
        ClusterState tmpState = builder.nodes(nodesBuilder)
            .nodeIdsToCompatibilityVersions(compatibilityVersions)
            .nodeFeatures(nodeFeatures)
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
                "A node with transport version " + joiningTransportVersion.toReleaseVersion() + " is forbidden from joining this cluster"
            );
        }
    }

    /**
     * Calculate the cluster's effective features. This includes all features that are assumed on any nodes in the cluster,
     * that are also present across the whole cluster as a result.
     */
    private Set<String> calculateEffectiveClusterFeatures(DiscoveryNodes nodes, Map<String, Set<String>> nodeFeatures) {
        if (FeatureService.featuresCanBeAssumedForNodes(nodes)) {
            Set<String> assumedFeatures = featureService.getNodeFeatures()
                .values()
                .stream()
                .filter(NodeFeature::assumedAfterNextCompatibilityBoundary)
                .map(NodeFeature::id)
                .collect(Collectors.toSet());

            // add all assumed features to the featureset of all nodes of the next major version
            nodeFeatures = new HashMap<>(nodeFeatures);
            for (var node : nodes.getNodes().entrySet()) {
                if (FeatureService.featuresCanBeAssumedForNode(node.getValue())) {
                    assert nodeFeatures.containsKey(node.getKey()) : "Node " + node.getKey() + " does not have any features";
                    nodeFeatures.computeIfPresent(node.getKey(), (k, v) -> {
                        var newFeatures = new HashSet<>(v);
                        return newFeatures.addAll(assumedFeatures) ? newFeatures : v;
                    });
                }
            }
        }

        return ClusterFeatures.calculateAllNodeFeatures(nodeFeatures.values());
    }

    /**
     * Ensures that all indices are compatible with the given index version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of elasticsearch as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     * @see IndexVersions#MINIMUM_COMPATIBLE
     * @see IndexVersions#MINIMUM_READONLY_COMPATIBLE
     * @throws IllegalStateException if any index is incompatible with the given version
     */
    public static void ensureIndexCompatibility(
        IndexVersion minSupportedVersion,
        IndexVersion minReadOnlySupportedVersion,
        IndexVersion maxSupportedVersion,
        Metadata metadata
    ) {
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetadata idxMetadata : metadata.indicesAllProjects()) {
            if (idxMetadata.getCompatibilityVersion().after(maxSupportedVersion)) {
                throw new IllegalStateException(
                    "index "
                        + idxMetadata.getIndex()
                        + " version not supported: "
                        + idxMetadata.getCompatibilityVersion().toReleaseVersion()
                        + " maximum compatible index version is: "
                        + maxSupportedVersion.toReleaseVersion()
                );
            }
            if (idxMetadata.getCompatibilityVersion().before(minSupportedVersion)) {
                boolean isReadOnlySupported = isReadOnlySupportedVersion(idxMetadata, minSupportedVersion, minReadOnlySupportedVersion);
                if (isReadOnlySupported == false) {
                    throw new IllegalStateException(
                        "index "
                            + idxMetadata.getIndex()
                            + " version not supported: "
                            + idxMetadata.getCompatibilityVersion().toReleaseVersion()
                            + " minimum compatible index version is: "
                            + minSupportedVersion.toReleaseVersion()
                    );
                }
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
        CompatibilityVersions joiningCompatibilityVersions,
        Collection<CompatibilityVersions> existingTransportVersions
    ) {
        TransportVersion minClusterTransportVersion = existingTransportVersions.stream()
            .map(CompatibilityVersions::transportVersion)
            .min(Comparator.naturalOrder())
            .orElse(TransportVersion.current());
        if (joiningCompatibilityVersions.transportVersion().before(minClusterTransportVersion)) {
            throw new IllegalStateException(
                "node with transport version ["
                    + joiningCompatibilityVersions.transportVersion().toReleaseVersion()
                    + "] may not join a cluster with minimum transport version ["
                    + minClusterTransportVersion.toReleaseVersion()
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

    /**
     * Enforces the feature join barrier - a joining node should have all features already present in all existing nodes in the cluster
     *
     * @return The set of features that this node has (including assumed features)
     */
    private Set<String> enforceNodeFeatureBarrier(DiscoveryNode node, Set<String> effectiveClusterFeatures, Set<String> newNodeFeatures) {
        // prevent join if it does not have one or more features that all other nodes have
        Set<String> missingFeatures = new HashSet<>(effectiveClusterFeatures);
        missingFeatures.removeAll(newNodeFeatures);

        if (missingFeatures.isEmpty()) {
            // nothing missing - all ok
            return newNodeFeatures;
        }

        if (FeatureService.featuresCanBeAssumedForNode(node)) {
            // it might still be ok for this node to join if this node can have assumed features,
            // and all the missing features are assumed
            // we can get the NodeFeature object direct from this node's registered features
            // as all existing nodes in the cluster have the features present in existingNodesFeatures, including this one
            newNodeFeatures = new HashSet<>(newNodeFeatures);
            for (Iterator<String> it = missingFeatures.iterator(); it.hasNext();) {
                String feature = it.next();
                NodeFeature nf = featureService.getNodeFeatures().get(feature);
                if (nf.assumedAfterNextCompatibilityBoundary()) {
                    // its ok for this feature to be missing from this node
                    it.remove();
                    // and it should be assumed to still be in the cluster
                    newNodeFeatures.add(feature);
                }
                // even if we don't remove it, still continue, so the exception message below is accurate
            }
        }

        if (missingFeatures.isEmpty()) {
            return newNodeFeatures;
        } else {
            throw new IllegalStateException("Node " + node.getId() + " is missing required features " + missingFeatures);
        }
    }

    public static Collection<BiConsumer<DiscoveryNode, ClusterState>> addBuiltInJoinValidators(
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators
    ) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
        validators.add((node, state) -> {
            ensureNodesCompatibility(node.getVersion(), state.getNodes());
            ensureIndexCompatibility(
                node.getMinIndexVersion(),
                node.getMinReadOnlyIndexVersion(),
                node.getMaxIndexVersion(),
                state.getMetadata()
            );
        });
        validators.addAll(onJoinValidators);
        return Collections.unmodifiableCollection(validators);
    }
}
