/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.Collections;
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
    private final AtomicLong lastRunTimeMillis = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean checkInProgress = new AtomicBoolean();
    // Keeps track of whether the cleanup of existing index blocks (upon disabling
    // the Disk Threshold Monitor) was successfully done or not.
    private final AtomicBoolean cleanupUponDisableCalled = new AtomicBoolean();

    /**
     * The IDs of the nodes that were over the low threshold in the last check (and maybe over another threshold too). Tracked so that we
     * can log when such nodes are no longer over the low threshold.
     */
    private final Set<String> nodesOverLowThreshold = Sets.newConcurrentHashSet();

    /**
     * The IDs of the nodes that were over the high threshold in the last check (and maybe over another threshold too). Tracked so that we
     * can log when such nodes are no longer over the high threshold.
     */
    private final Set<String> nodesOverHighThreshold = Sets.newConcurrentHashSet();

    /**
     * The IDs of the nodes that were over the high threshold in the last check, but which are relocating shards that will bring them
     * under the high threshold again. Tracked so that we can log when such nodes are no longer in this state.
     */
    private final Set<String> nodesOverHighThresholdAndRelocating = Sets.newConcurrentHashSet();

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
        RerouteService rerouteService
    ) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.rerouteService = rerouteService;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        this.client = client;
    }

    private void checkFinished() {
        final boolean checkFinished = checkInProgress.compareAndSet(true, false);
        assert checkFinished;
        logger.trace("checkFinished");
    }

    public void onNewInfo(ClusterInfo info) {
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

        final ClusterState state = clusterStateSupplier.get();
        final Set<String> indicesToMarkReadOnly = new HashSet<>();
        RoutingNodes routingNodes = state.getRoutingNodes();
        Set<String> indicesNotToAutoRelease = new HashSet<>();
        markNodesMissingUsageIneligibleForRelease(routingNodes, usages, indicesNotToAutoRelease);

        final List<DiskUsage> usagesOverHighThreshold = new ArrayList<>();

        for (final Map.Entry<String, DiskUsage> entry : usages.entrySet()) {
            final String node = entry.getKey();
            final DiskUsage usage = entry.getValue();
            final RoutingNode routingNode = routingNodes.node(node);
            final ByteSizeValue total = ByteSizeValue.ofBytes(usage.getTotalBytes());

            if (isDedicatedFrozenNode(routingNode)) {
                if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(total).getBytes()) {
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

            if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage(total).getBytes()) {
                nodesOverLowThreshold.add(node);
                nodesOverHighThreshold.add(node);
                nodesOverHighThresholdAndRelocating.remove(node);

                if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                    for (ShardRouting routing : routingNode) {
                        String indexName = routing.index().getName();
                        indicesToMarkReadOnly.add(indexName);
                        indicesNotToAutoRelease.add(indexName);
                    }
                }

                logger.warn(
                    "flood stage disk watermark [{}] exceeded on {}, all indices on this node will be marked read-only",
                    diskThresholdSettings.describeFloodStageThreshold(total, false),
                    usage
                );

                continue;
            }

            if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes()) {
                if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                    for (ShardRouting routing : routingNode) {
                        String indexName = routing.index().getName();
                        indicesNotToAutoRelease.add(indexName);
                    }
                }
            }

            final long reservedSpace = info.getReservedSpace(usage.getNodeId(), usage.getPath()).getTotal();
            final DiskUsage usageWithReservedSpace = new DiskUsage(
                usage.getNodeId(),
                usage.getNodeName(),
                usage.getPath(),
                usage.getTotalBytes(),
                Math.max(0L, usage.getFreeBytes() - reservedSpace)
            );

            if (usageWithReservedSpace.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes()) {
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

            } else if (usageWithReservedSpace.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdLowStage(total).getBytes()) {
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

        final ActionListener<Void> listener = new GroupedActionListener<>(ActionListener.wrap(this::checkFinished), 3);

        if (reroute) {
            logger.debug("rerouting shards: [{}]", explanation);
            rerouteService.reroute("disk threshold monitor", Priority.HIGH, ActionListener.wrap(reroutedClusterState -> {

                for (DiskUsage diskUsage : usagesOverHighThreshold) {
                    final RoutingNode routingNode = reroutedClusterState.getRoutingNodes().node(diskUsage.getNodeId());
                    final DiskUsage usageIncludingRelocations;
                    final long relocatingShardsSize;
                    if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                        relocatingShardsSize = sizeOfRelocatingShards(routingNode, diskUsage, info, reroutedClusterState);
                        usageIncludingRelocations = new DiskUsage(
                            diskUsage.getNodeId(),
                            diskUsage.getNodeName(),
                            diskUsage.getPath(),
                            diskUsage.getTotalBytes(),
                            diskUsage.getFreeBytes() - relocatingShardsSize
                        );
                    } else {
                        usageIncludingRelocations = diskUsage;
                        relocatingShardsSize = 0L;
                    }
                    final ByteSizeValue total = ByteSizeValue.ofBytes(usageIncludingRelocations.getTotalBytes());

                    if (usageIncludingRelocations.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes()) {
                        nodesOverHighThresholdAndRelocating.remove(diskUsage.getNodeId());
                        logger.warn(
                            "high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; "
                                + "currently relocating away shards totalling [{}] bytes; the node is expected to continue to exceed "
                                + "the high disk watermark when these relocations are complete",
                            diskThresholdSettings.describeHighThreshold(total, false),
                            diskUsage,
                            -relocatingShardsSize
                        );
                    } else if (nodesOverHighThresholdAndRelocating.add(diskUsage.getNodeId())) {
                        logger.info(
                            "high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; "
                                + "currently relocating away shards totalling [{}] bytes; the node is expected to be below the high "
                                + "disk watermark when these relocations are complete",
                            diskThresholdSettings.describeHighThreshold(total, false),
                            diskUsage,
                            -relocatingShardsSize
                        );
                    } else {
                        logger.debug(
                            "high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; "
                                + "currently relocating away shards totalling [{}] bytes",
                            diskThresholdSettings.describeHighThreshold(total, false),
                            diskUsage,
                            -relocatingShardsSize
                        );
                    }
                }

                setLastRunTimeMillis();
                listener.onResponse(null);
            }, e -> {
                logger.debug("reroute failed", e);
                setLastRunTimeMillis();
                listener.onFailure(e);
            }));
        } else {
            logger.trace("no reroute required");
            listener.onResponse(null);
        }

        // Generate a map of node name to ID so we can use it to look up node replacement targets
        final Map<String, String> nodeNameToId = state.getRoutingNodes()
            .stream()
            .collect(Collectors.toMap(rn -> rn.node().getName(), RoutingNode::nodeId, (s1, s2) -> s2));

        // Calculate both the source node id and the target node id of a "replace" type shutdown
        final Set<String> nodesIdsPartOfReplacement = state.metadata()
            .nodeShutdowns()
            .values()
            .stream()
            .filter(meta -> meta.getType() == SingleNodeShutdownMetadata.Type.REPLACE)
            .flatMap(meta -> Stream.of(meta.getNodeId(), nodeNameToId.get(meta.getTargetNodeName())))
            .collect(Collectors.toSet());

        // Generate a set of all the indices that exist on either the target or source of a node replacement
        final Set<String> indicesOnReplaceSourceOrTarget = new HashSet<>();
        for (String nodeId : nodesIdsPartOfReplacement) {
            for (ShardRouting shardRouting : state.getRoutingNodes().node(nodeId)) {
                indicesOnReplaceSourceOrTarget.add(shardRouting.index().getName());
            }
        }

        final Set<String> indicesToAutoRelease = state.routingTable()
            .indicesRouting()
            .keySet()
            .stream()
            .filter(index -> indicesNotToAutoRelease.contains(index) == false)
            .filter(index -> state.getBlocks().hasIndexBlock(index, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            // Do not auto release indices that are on either the source or the target of a node replacement
            .filter(index -> indicesOnReplaceSourceOrTarget.contains(index) == false)
            .collect(Collectors.toSet());

        if (indicesToAutoRelease.isEmpty() == false) {
            logger.info(
                "releasing read-only block on indices "
                    + indicesToAutoRelease
                    + " since they are now allocated to nodes with sufficient disk space"
            );
            updateIndicesReadOnly(indicesToAutoRelease, listener, false);
        } else {
            logger.trace("no auto-release required");
            listener.onResponse(null);
        }

        indicesToMarkReadOnly.removeIf(index -> state.getBlocks().indexBlocked(ClusterBlockLevel.WRITE, index));
        logger.trace("marking indices as read-only: [{}]", indicesToMarkReadOnly);
        if (indicesToMarkReadOnly.isEmpty() == false) {
            updateIndicesReadOnly(indicesToMarkReadOnly, listener, true);
        } else {
            listener.onResponse(null);
        }
    }

    // exposed for tests to override
    long sizeOfRelocatingShards(RoutingNode routingNode, DiskUsage diskUsage, ClusterInfo info, ClusterState reroutedClusterState) {
        return DiskThresholdDecider.sizeOfRelocatingShards(
            routingNode,
            true,
            diskUsage.getPath(),
            info,
            reroutedClusterState.metadata(),
            reroutedClusterState.routingTable()
        );
    }

    private static void markNodesMissingUsageIneligibleForRelease(
        RoutingNodes routingNodes,
        Map<String, DiskUsage> usages,
        Set<String> indicesToMarkIneligibleForAutoRelease
    ) {
        for (RoutingNode routingNode : routingNodes) {
            if (usages.containsKey(routingNode.nodeId()) == false) {
                for (ShardRouting routing : routingNode) {
                    String indexName = routing.index().getName();
                    indicesToMarkIneligibleForAutoRelease.add(indexName);
                }
            }
        }
    }

    private void setLastRunTimeMillis() {
        lastRunTimeMillis.getAndUpdate(l -> Math.max(l, currentTimeMillisSupplier.getAsLong()));
    }

    protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
        // set read-only block but don't block on the response
        ActionListener<Void> wrappedListener = ActionListener.wrap(r -> {
            setLastRunTimeMillis();
            listener.onResponse(r);
        }, e -> {
            logger.debug(() -> "setting indices [" + readOnly + "] read-only failed", e);
            setLastRunTimeMillis();
            listener.onFailure(e);
        });
        Settings readOnlySettings = readOnly ? READ_ONLY_ALLOW_DELETE_SETTINGS : NOT_READ_ONLY_ALLOW_DELETE_SETTINGS;
        client.admin()
            .indices()
            .prepareUpdateSettings(indicesToUpdate.toArray(Strings.EMPTY_ARRAY))
            .setSettings(readOnlySettings)
            .origin("disk-threshold-monitor")
            .execute(wrappedListener.map(r -> null));
    }

    private void removeExistingIndexBlocks() {
        if (cleanupUponDisableCalled.get()) {
            checkFinished();
            return;
        }
        ActionListener<Void> wrappedListener = ActionListener.wrap(r -> {
            cleanupUponDisableCalled.set(true);
            checkFinished();
        }, e -> {
            logger.debug("removing read-only blocks from indices failed", e);
            checkFinished();
        });
        final ClusterState state = clusterStateSupplier.get();
        final Set<String> indicesToRelease = state.getBlocks()
            .indices()
            .keySet()
            .stream()
            .filter(index -> state.getBlocks().hasIndexBlock(index, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .collect(Collectors.toUnmodifiableSet());
        logger.trace("removing read-only block from indices [{}]", indicesToRelease);
        if (indicesToRelease.isEmpty() == false) {
            client.admin()
                .indices()
                .prepareUpdateSettings(indicesToRelease.toArray(Strings.EMPTY_ARRAY))
                .setSettings(NOT_READ_ONLY_ALLOW_DELETE_SETTINGS)
                .origin("disk-threshold-monitor")
                .execute(wrappedListener.map(r -> null));
        } else {
            wrappedListener.onResponse(null);
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
