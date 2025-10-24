/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Listens for a node to go over the high watermark and kicks off an empty
 * reroute if it does. Also responsible for logging about nodes that have
 * passed the disk watermarks
 */
public class DiskThresholdMonitor {

    private static final Logger logger = LogManager.getLogger(DiskThresholdMonitor.class);

    private static final Settings READ_ONLY_ALLOW_DELETE_SETTINGS = Settings.builder()
        .put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, Boolean.TRUE.toString())
        .build();

    private static final Settings NOT_READ_ONLY_ALLOW_DELETE_SETTINGS = Settings.builder()
        .putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE)
        .build();

    private final DiskThresholdSettings diskThresholdSettings;
    private final Client client;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;
    private final ProjectResolver projectResolver;
    private final AtomicLong lastRunTimeMillis = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean checkInProgress = new AtomicBoolean();
    // Keeps track of whether the cleanup of existing index blocks (upon disabling
    // the Disk Threshold Monitor) was successfully done or not.
    private final AtomicBoolean cleanupUponDisableCalled = new AtomicBoolean();

    /**
     * The IDs of the nodes that were over the low threshold in the last check (and maybe over another threshold too). Tracked so that we
     * can log when such nodes are no longer over the low threshold.
     */
    private final Set<String> nodesOverLowThreshold = ConcurrentCollections.newConcurrentSet();

    /**
     * The IDs of the nodes that were over the high threshold in the last check (and maybe over another threshold too). Tracked so that we
     * can log when such nodes are no longer over the high threshold.
     */
    private final Set<String> nodesOverHighThreshold = ConcurrentCollections.newConcurrentSet();

    /**
     * The IDs of the nodes that were over the high threshold in the last check, but which are relocating shards that will bring them
     * under the high threshold again. Tracked so that we can log when such nodes are no longer in this state.
     */
    private final Set<String> nodesOverHighThresholdAndRelocating = ConcurrentCollections.newConcurrentSet();

    /**
     * The IDs of the nodes in the last info received. Tracked because when a new node joins we consider its disk usage to be equal to
     * the average disk usage in the cluster but we don't keep track of whether this puts it over any of the watermarks so when we receive
     * its actual disk usage we may be able to move some more shards around. No need for synchronization, all access is protected by
     * {@code checkInProgress}.
     */
    private Set<String> lastNodes = Collections.emptySet();

    public DiskThresholdMonitor(
        Settings settings,
        Supplier<ClusterState> clusterStateSupplier,
        ClusterSettings clusterSettings,
        Client client,
        LongSupplier currentTimeMillisSupplier,
        RerouteService rerouteService,
        ProjectResolver projectResolver
    ) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.rerouteService = rerouteService;
        this.projectResolver = projectResolver;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        this.client = client;
    }

    private void checkFinished() {
        final boolean checkFinished = checkInProgress.compareAndSet(true, false);
        assert checkFinished;
        logger.trace("checkFinished");
    }

    public void onNewInfo(ClusterInfo info) {
        final ClusterState state = clusterStateSupplier.get();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        // TODO find a better way to limit concurrent updates (and potential associated reroutes) while allowing tests to ensure that
        // all ClusterInfo updates are processed and never ignored
        if (checkInProgress.compareAndSet(false, true) == false) {
            logger.info("skipping monitor as a check is already in progress");
            return;
        }

        if (diskThresholdSettings.isEnabled() == false) {
            removeExistingIndexBlocks();
            return;
        } else {
            // reset this for the next disable call.
            cleanupUponDisableCalled.set(false);
        }

        final Map<String, DiskUsage> usages = info.getNodeLeastAvailableDiskUsages();
        if (usages == null) {
            logger.trace("skipping monitor as no disk usage information is available");
            lastNodes = Collections.emptySet();
            checkFinished();
            return;
        }

        logger.trace("processing new cluster info");

        boolean reroute = false;
        String explanation = "";
        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();

        // Clean up nodes that have been removed from the cluster
        final Set<String> nodes = new HashSet<>(usages.keySet());
        cleanUpRemovedNodes(nodes, nodesOverLowThreshold);
        cleanUpRemovedNodes(nodes, nodesOverHighThreshold);
        cleanUpRemovedNodes(nodes, nodesOverHighThresholdAndRelocating);

        if (lastNodes.equals(nodes) == false) {
            if (lastNodes.containsAll(nodes) == false) {
                logger.debug("rerouting because disk usage info received from new nodes");
                reroute = true;
            }
            lastNodes = Collections.unmodifiableSet(nodes);
        }

        final Set<Index> indicesToMarkReadOnly = new HashSet<>();
        final Set<Index> indicesNotToAutoRelease = new HashSet<>();
        RoutingNodes routingNodes = state.getRoutingNodes();
        markNodesMissingUsageIneligibleForRelease(routingNodes, usages, indicesNotToAutoRelease);

        final List<DiskUsage> usagesOverHighThreshold = new ArrayList<>();

        for (final Map.Entry<String, DiskUsage> entry : usages.entrySet()) {
            final String node = entry.getKey();
            final DiskUsage usage = entry.getValue();
            final RoutingNode routingNode = routingNodes.node(node);
            final ByteSizeValue total = ByteSizeValue.ofBytes(usage.totalBytes());

            if (isDedicatedFrozenNode(routingNode)) {
                if (usage.freeBytes() < diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(total).getBytes()) {
                    logger.warn(
                        "flood stage disk watermark [{}] exceeded on {}",
                        diskThresholdSettings.describeFrozenFloodStageThreshold(total, false),
                        usage
                    );
                }
                // skip checking high/low watermarks for frozen nodes, since frozen shards have only insignificant local storage footprint
                // and this allows us to use more of the local storage for cache.
                continue;
            }

            if (usage.freeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage(total).getBytes()) {
                nodesOverLowThreshold.add(node);
                nodesOverHighThreshold.add(node);
                nodesOverHighThresholdAndRelocating.remove(node);

                if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                    for (ShardRouting routing : routingNode) {
                        indicesToMarkReadOnly.add(routing.index());
                        indicesNotToAutoRelease.add(routing.index());
                    }
                }

                logger.warn(
                    "flood stage disk watermark [{}] exceeded on {}, all indices on this node will be marked read-only",
                    diskThresholdSettings.describeFloodStageThreshold(total, false),
                    usage
                );

                continue;
            }

            if (usage.freeBytes() < diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes()) {
                if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                    for (ShardRouting routing : routingNode) {
                        indicesNotToAutoRelease.add(routing.index());
                    }
                }
            }

            final long reservedSpace = info.getReservedSpace(usage.nodeId(), usage.path()).total();
            final DiskUsage usageWithReservedSpace = new DiskUsage(
                usage.nodeId(),
                usage.nodeName(),
                usage.path(),
                usage.totalBytes(),
                Math.max(0L, usage.freeBytes() - reservedSpace)
            );

            if (usageWithReservedSpace.freeBytes() < diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes()) {
                nodesOverLowThreshold.add(node);
                nodesOverHighThreshold.add(node);

                if (lastRunTimeMillis.get() <= currentTimeMillis - diskThresholdSettings.getRerouteInterval().millis()) {
                    reroute = true;
                    explanation = "high disk watermark exceeded on one or more nodes";
                    usagesOverHighThreshold.add(usage);
                    // will log about this node when the reroute completes
                } else {
                    logger.debug(
                        "high disk watermark exceeded on {} but an automatic reroute has occurred " + "in the last [{}], skipping reroute",
                        node,
                        diskThresholdSettings.getRerouteInterval()
                    );
                }

            } else if (usageWithReservedSpace.freeBytes() < diskThresholdSettings.getFreeBytesThresholdLowStage(total).getBytes()) {
                nodesOverHighThresholdAndRelocating.remove(node);

                final boolean wasUnderLowThreshold = nodesOverLowThreshold.add(node);
                final boolean wasOverHighThreshold = nodesOverHighThreshold.remove(node);
                assert (wasUnderLowThreshold && wasOverHighThreshold) == false;

                if (wasUnderLowThreshold) {
                    logger.info(
                        "low disk watermark [{}] exceeded on {}, replicas will not be assigned to this node",
                        diskThresholdSettings.describeLowThreshold(total, false),
                        usage
                    );
                } else if (wasOverHighThreshold) {
                    logger.info(
                        "high disk watermark [{}] no longer exceeded on {}, but low disk watermark [{}] is still exceeded",
                        diskThresholdSettings.describeHighThreshold(total, false),
                        usage,
                        diskThresholdSettings.describeLowThreshold(total, false)
                    );
                }

            } else {
                nodesOverHighThresholdAndRelocating.remove(node);

                if (nodesOverLowThreshold.contains(node)) {
                    // The node has previously been over the low watermark, but is no longer, so it may be possible to allocate more
                    // shards if we reroute now.
                    if (lastRunTimeMillis.get() <= currentTimeMillis - diskThresholdSettings.getRerouteInterval().millis()) {
                        reroute = true;
                        explanation = "one or more nodes has gone under the high or low watermark";
                        nodesOverLowThreshold.remove(node);
                        nodesOverHighThreshold.remove(node);

                        logger.info(
                            "low disk watermark [{}] no longer exceeded on {}",
                            diskThresholdSettings.describeLowThreshold(total, false),
                            usage
                        );

                    } else {
                        logger.debug(
                            "{} has gone below a disk threshold, but an automatic reroute has occurred "
                                + "in the last [{}], skipping reroute",
                            node,
                            diskThresholdSettings.getRerouteInterval()
                        );
                    }
                }

            }
        }

        try (var asyncRefs = new RefCountingRunnable(this::checkFinished)) {

            if (reroute) {
                logger.debug("rerouting shards: [{}]", explanation);
                rerouteService.reroute(
                    "disk threshold monitor",
                    Priority.HIGH,
                    ActionListener.releaseAfter(ActionListener.runAfter(ActionListener.wrap(ignored -> {
                        final var reroutedClusterState = clusterStateSupplier.get();
                        for (DiskUsage diskUsage : usagesOverHighThreshold) {
                            final RoutingNode routingNode = reroutedClusterState.getRoutingNodes().node(diskUsage.nodeId());
                            final DiskUsage usageIncludingRelocations;
                            final long relocatingShardsSize;
                            if (routingNode != null) { // might be temporarily null if ClusterInfoService and ClusterService are out of step
                                relocatingShardsSize = sizeOfRelocatingShards(routingNode, diskUsage, info, reroutedClusterState);
                                usageIncludingRelocations = new DiskUsage(
                                    diskUsage.nodeId(),
                                    diskUsage.nodeName(),
                                    diskUsage.path(),
                                    diskUsage.totalBytes(),
                                    diskUsage.freeBytes() - relocatingShardsSize
                                );
                            } else {
                                usageIncludingRelocations = diskUsage;
                                relocatingShardsSize = 0L;
                            }
                            final ByteSizeValue total = ByteSizeValue.ofBytes(usageIncludingRelocations.totalBytes());

                            if (usageIncludingRelocations.freeBytes() < diskThresholdSettings.getFreeBytesThresholdHighStage(total)
                                .getBytes()) {
                                nodesOverHighThresholdAndRelocating.remove(diskUsage.nodeId());
                                logger.warn("""
                                    high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; currently \
                                    relocating away shards totalling [{}] bytes; the node is expected to continue to exceed the high disk \
                                    watermark when these relocations are complete\
                                    """, diskThresholdSettings.describeHighThreshold(total, false), diskUsage, -relocatingShardsSize);
                            } else if (nodesOverHighThresholdAndRelocating.add(diskUsage.nodeId())) {
                                logger.info("""
                                    high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; currently \
                                    relocating away shards totalling [{}] bytes; the node is expected to be below the high disk watermark \
                                    when these relocations are complete\
                                    """, diskThresholdSettings.describeHighThreshold(total, false), diskUsage, -relocatingShardsSize);
                            } else {
                                logger.debug("""
                                    high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; currently \
                                    relocating away shards totalling [{}] bytes\
                                    """, diskThresholdSettings.describeHighThreshold(total, false), diskUsage, -relocatingShardsSize);
                            }
                        }
                    }, e -> logger.debug("reroute failed", e)), this::setLastRunTimeMillis), asyncRefs.acquire())
                );
            } else {
                logger.trace("no reroute required");
            }

            // Generate a map of node name to ID so we can use it to look up node replacement targets
            final Map<String, List<String>> nodeNameToIds = state.getRoutingNodes()
                .stream()
                .collect(Collectors.groupingBy(rn -> rn.node().getName(), Collectors.mapping(RoutingNode::nodeId, Collectors.toList())));

            // Generate a set of the valid node IDs so we can use it to filter valid sources
            final Set<String> routingNodeIds = state.getRoutingNodes().stream().map(RoutingNode::nodeId).collect(Collectors.toSet());

            // Calculate both the source node id and the target node id of a "replace" type shutdown
            final Set<String> nodesIdsPartOfReplacement = state.metadata()
                .nodeShutdowns()
                .getAll()
                .values()
                .stream()
                .filter(meta -> meta.getType() == SingleNodeShutdownMetadata.Type.REPLACE)
                .flatMap(meta -> Stream.concat(Stream.of(meta.getNodeId()), nodeIdsOrEmpty(meta, nodeNameToIds)))
                .filter(routingNodeIds::contains) // The REPLACE source node might already have been removed from RoutingNodes
                .collect(Collectors.toSet());

            // Generate a set of all the indices that exist on either the target or source of a node replacement
            final Set<Index> indicesOnReplaceSourceOrTarget = new HashSet<>();
            for (String nodeId : nodesIdsPartOfReplacement) {
                for (ShardRouting shardRouting : state.getRoutingNodes().node(nodeId)) {
                    indicesOnReplaceSourceOrTarget.add(shardRouting.index());
                }
            }

            Set<Index> indicesToAutoRelease = new HashSet<>();
            for (IndexRoutingTable indexRouting : state.globalRoutingTable().indexRouting()) {
                Index index = indexRouting.getIndex();
                if (indicesNotToAutoRelease.contains(index)) {
                    continue;
                }
                // Do not auto release indices that are on either the source or the target of a node replacement
                if (indicesOnReplaceSourceOrTarget.contains(index)) {
                    continue;
                }
                var projectId = state.metadata().projectFor(index).id();
                if (state.getBlocks().hasIndexBlock(projectId, index.getName(), IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)) {
                    indicesToAutoRelease.add(index);
                }
            }

            if (indicesToAutoRelease.isEmpty() == false) {
                logger.info(
                    "releasing read-only block on indices "
                        + indicesToAutoRelease
                        + " since they are now allocated to nodes with sufficient disk space"
                );
                updateIndicesReadOnly(state, indicesToAutoRelease, asyncRefs.acquire(), false);
            } else {
                logger.trace("no auto-release required");
            }

            indicesToMarkReadOnly.removeIf(index -> {
                final String indexName = index.getName();
                return state.getBlocks().indexBlocked(state.metadata().projectFor(index).id(), ClusterBlockLevel.WRITE, indexName);
            });
            logger.trace("marking indices as read-only: [{}]", indicesToMarkReadOnly);
            if (indicesToMarkReadOnly.isEmpty() == false) {
                updateIndicesReadOnly(state, indicesToMarkReadOnly, asyncRefs.acquire(), true);
            }
        }
    }

    private static Stream<String> nodeIdsOrEmpty(SingleNodeShutdownMetadata meta, Map<String, List<String>> nodeNameToIds) {
        var ids = nodeNameToIds.get(meta.getTargetNodeName()); // The REPLACE target node might not still be in RoutingNodes
        return ids == null ? Stream.empty() : ids.stream();
    }

    // exposed for tests to override
    long sizeOfRelocatingShards(RoutingNode routingNode, DiskUsage diskUsage, ClusterInfo info, ClusterState reroutedClusterState) {
        return DiskThresholdDecider.sizeOfUnaccountedShards(
            routingNode,
            true,
            diskUsage.path(),
            info,
            SnapshotShardSizeInfo.EMPTY,
            reroutedClusterState.metadata(),
            reroutedClusterState.globalRoutingTable(),
            0L
        );
    }

    private static void markNodesMissingUsageIneligibleForRelease(
        RoutingNodes routingNodes,
        Map<String, DiskUsage> usages,
        Set<Index> indicesToMarkIneligibleForAutoRelease
    ) {
        for (RoutingNode routingNode : routingNodes) {
            if (usages.containsKey(routingNode.nodeId()) == false) {
                for (ShardRouting routing : routingNode) {
                    indicesToMarkIneligibleForAutoRelease.add(routing.index());
                }
            }
        }
    }

    private void setLastRunTimeMillis() {
        lastRunTimeMillis.getAndUpdate(l -> Math.max(l, currentTimeMillisSupplier.getAsLong()));
    }

    protected void updateIndicesReadOnly(ClusterState clusterState, Set<Index> indicesToUpdate, Releasable onCompletion, boolean readOnly) {
        // set read-only block but don't block on the response
        Settings readOnlySettings = readOnly ? READ_ONLY_ALLOW_DELETE_SETTINGS : NOT_READ_ONLY_ALLOW_DELETE_SETTINGS;
        final Map<ProjectId, Set<String>> indicesByProject = new HashMap<>();
        for (Index index : indicesToUpdate) {
            var project = clusterState.metadata().projectFor(index);
            indicesByProject.computeIfAbsent(project.id(), ignore -> Sets.newHashSetWithExpectedSize(indicesToUpdate.size()))
                .add(index.getName());
        }

        final ActionListener<AcknowledgedResponse> countdownListener = new CountDownActionListener(
            indicesByProject.size(),
            ActionListener.releaseAfter(
                ActionListener.runAfter(
                    ActionListener.<Void>noop()
                        .delegateResponse((l, e) -> logger.debug(() -> "setting indices [" + readOnly + "] read-only failed", e)),
                    this::setLastRunTimeMillis
                ),
                onCompletion
            )
        ).map(ignore -> null);

        indicesByProject.forEach(
            (projectId, indices) -> projectResolver.executeOnProject(
                projectId,
                () -> client.admin()
                    .indices()
                    .prepareUpdateSettings(indices.toArray(Strings.EMPTY_ARRAY))
                    .setSettings(readOnlySettings)
                    .origin("disk-threshold-monitor")
                    .execute(countdownListener)
            )
        );
    }

    private void removeExistingIndexBlocks() {
        if (cleanupUponDisableCalled.get()) {
            checkFinished();
            return;
        }
        final ClusterState state = clusterStateSupplier.get();
        ActionListener<Void> wrappedListener = new CountDownActionListener(state.metadata().projects().size(), ActionListener.wrap(r -> {
            cleanupUponDisableCalled.set(true);
            checkFinished();
        }, e -> {
            logger.debug("removing read-only blocks from indices failed", e);
            checkFinished();
        }));
        for (var projectId : state.metadata().projects().keySet()) {
            final Set<String> indicesToRelease = state.getBlocks()
                .indices(projectId)
                .keySet()
                .stream()
                .filter(index -> state.getBlocks().hasIndexBlock(projectId, index, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
                .collect(Collectors.toUnmodifiableSet());
            logger.trace("removing read-only block from indices [{}] for project [{}]", indicesToRelease, projectId);
            if (indicesToRelease.isEmpty() == false) {
                projectResolver.executeOnProject(
                    projectId,
                    () -> client.admin()
                        .indices()
                        .prepareUpdateSettings(indicesToRelease.toArray(Strings.EMPTY_ARRAY))
                        .setSettings(NOT_READ_ONLY_ALLOW_DELETE_SETTINGS)
                        .origin("disk-threshold-monitor")
                        .execute(wrappedListener.map(r -> null))
                );
            } else {
                wrappedListener.onResponse(null);
            }
        }
    }

    private static void cleanUpRemovedNodes(Set<String> nodesToKeep, Set<String> nodesToCleanUp) {
        for (String node : nodesToCleanUp) {
            if (nodesToKeep.contains(node) == false) {
                nodesToCleanUp.remove(node);
            }
        }
    }

    private static boolean isDedicatedFrozenNode(RoutingNode routingNode) {
        if (routingNode == null) {
            return false;
        }
        DiscoveryNode node = routingNode.node();
        return node.isDedicatedFrozenNode();
    }
}
