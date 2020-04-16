/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation;

import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Listens for a node to go over the high watermark and kicks off an empty
 * reroute if it does. Also responsible for logging about nodes that have
 * passed the disk watermarks
 */
public class DiskThresholdMonitor {

    private static final Logger logger = LogManager.getLogger(DiskThresholdMonitor.class);

    private final DiskThresholdSettings diskThresholdSettings;
    private final Client client;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;
    private final AtomicLong lastRunTimeMillis = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean checkInProgress = new AtomicBoolean();

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

    public DiskThresholdMonitor(Settings settings, Supplier<ClusterState> clusterStateSupplier, ClusterSettings clusterSettings,
                                Client client, LongSupplier currentTimeMillisSupplier, RerouteService rerouteService) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.rerouteService = rerouteService;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        this.client = client;
    }

    private void checkFinished() {
        final boolean checkFinished = checkInProgress.compareAndSet(true, false);
        assert checkFinished;
    }

    public void onNewInfo(ClusterInfo info) {

        if (checkInProgress.compareAndSet(false, true) == false) {
            logger.info("skipping monitor as a check is already in progress");
            return;
        }

        final ImmutableOpenMap<String, DiskUsage> usages = info.getNodeLeastAvailableDiskUsages();
        if (usages == null) {
            checkFinished();
            return;
        }

        boolean reroute = false;
        String explanation = "";
        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();

        // Clean up nodes that have been removed from the cluster
        final ObjectLookupContainer<String> nodes = usages.keys();
        cleanUpRemovedNodes(nodes, nodesOverLowThreshold);
        cleanUpRemovedNodes(nodes, nodesOverHighThreshold);
        cleanUpRemovedNodes(nodes, nodesOverHighThresholdAndRelocating);

        final ClusterState state = clusterStateSupplier.get();
        final Set<String> indicesToMarkReadOnly = new HashSet<>();
        RoutingNodes routingNodes = state.getRoutingNodes();
        Set<String> indicesNotToAutoRelease = new HashSet<>();
        markNodesMissingUsageIneligibleForRelease(routingNodes, usages, indicesNotToAutoRelease);

        final List<DiskUsage> usagesOverHighThreshold = new ArrayList<>();

        for (final ObjectObjectCursor<String, DiskUsage> entry : usages) {
            final String node = entry.key;
            final DiskUsage usage = entry.value;
            final RoutingNode routingNode = routingNodes.node(node);

            if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage().getBytes() ||
                usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdFloodStage()) {

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

                logger.warn("flood stage disk watermark [{}] exceeded on {}, all indices on this node will be marked read-only",
                    diskThresholdSettings.describeFloodStageThreshold(), usage);

            } else if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes() ||
                usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdHigh()) {

                nodesOverLowThreshold.add(node);
                nodesOverHighThreshold.add(node);

                if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                    for (ShardRouting routing : routingNode) {
                        String indexName = routing.index().getName();
                        indicesNotToAutoRelease.add(indexName);
                    }
                }
                if (lastRunTimeMillis.get() < currentTimeMillis - diskThresholdSettings.getRerouteInterval().millis()) {
                    reroute = true;
                    explanation = "high disk watermark exceeded on one or more nodes";
                    usagesOverHighThreshold.add(usage);
                    // will log about this node when the reroute completes
                } else {
                    logger.debug("high disk watermark exceeded on {} but an automatic reroute has occurred " +
                            "in the last [{}], skipping reroute",
                        node, diskThresholdSettings.getRerouteInterval());
                }

            } else if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdLow().getBytes() ||
                usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdLow()) {

                nodesOverHighThresholdAndRelocating.remove(node);

                final boolean wasUnderLowThreshold = nodesOverLowThreshold.add(node);
                final boolean wasOverHighThreshold = nodesOverHighThreshold.remove(node);
                assert (wasUnderLowThreshold && wasOverHighThreshold) == false;

                if (wasUnderLowThreshold) {
                    logger.info("low disk watermark [{}] exceeded on {}, replicas will not be assigned to this node",
                        diskThresholdSettings.describeLowThreshold(), usage);
                } else if (wasOverHighThreshold) {
                    logger.info("high disk watermark [{}] no longer exceeded on {}, but low disk watermark [{}] is still exceeded",
                        diskThresholdSettings.describeHighThreshold(), usage, diskThresholdSettings.describeLowThreshold());
                }

            } else {

                nodesOverHighThresholdAndRelocating.remove(node);

                if (nodesOverLowThreshold.contains(node)) {
                    // The node has previously been over the low watermark, but is no longer, so it may be possible to allocate more shards
                    // if we reroute now.
                    if (lastRunTimeMillis.get() < currentTimeMillis - diskThresholdSettings.getRerouteInterval().millis()) {
                        reroute = true;
                        explanation = "one or more nodes has gone under the high or low watermark";
                        nodesOverLowThreshold.remove(node);
                        nodesOverHighThreshold.remove(node);

                        logger.info("low disk watermark [{}] no longer exceeded on {}",
                            diskThresholdSettings.describeLowThreshold(), usage);

                    } else {
                        logger.debug("{} has gone below a disk threshold, but an automatic reroute has occurred " +
                                "in the last [{}], skipping reroute",
                            node, diskThresholdSettings.getRerouteInterval());
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
                        usageIncludingRelocations = new DiskUsage(diskUsage.getNodeId(), diskUsage.getNodeName(),
                            diskUsage.getPath(), diskUsage.getTotalBytes(), diskUsage.getFreeBytes() - relocatingShardsSize);
                    } else {
                        usageIncludingRelocations = diskUsage;
                        relocatingShardsSize = 0L;
                    }

                    if (usageIncludingRelocations.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()
                        || usageIncludingRelocations.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdHigh()) {

                        nodesOverHighThresholdAndRelocating.remove(diskUsage.getNodeId());
                        logger.warn("high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; " +
                                "currently relocating away shards totalling [{}] bytes; the node is expected to continue to exceed " +
                                "the high disk watermark when these relocations are complete",
                            diskThresholdSettings.describeHighThreshold(), diskUsage, -relocatingShardsSize);
                    } else if (nodesOverHighThresholdAndRelocating.add(diskUsage.getNodeId())) {
                        logger.info("high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; " +
                                "currently relocating away shards totalling [{}] bytes; the node is expected to be below the high " +
                                "disk watermark when these relocations are complete",
                            diskThresholdSettings.describeHighThreshold(), diskUsage, -relocatingShardsSize);
                    } else {
                        logger.debug("high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; " +
                                "currently relocating away shards totalling [{}] bytes",
                            diskThresholdSettings.describeHighThreshold(), diskUsage, -relocatingShardsSize);
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
            listener.onResponse(null);
        }
        final Set<String> indicesToAutoRelease = StreamSupport.stream(state.routingTable().indicesRouting()
            .spliterator(), false)
            .map(c -> c.key)
            .filter(index -> indicesNotToAutoRelease.contains(index) == false)
            .filter(index -> state.getBlocks().hasIndexBlock(index, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .collect(Collectors.toSet());

        if (indicesToAutoRelease.isEmpty() == false) {
            logger.info("releasing read-only block on indices " + indicesToAutoRelease
                + " since they are now allocated to nodes with sufficient disk space");
            updateIndicesReadOnly(indicesToAutoRelease, listener, false);
        } else {
            listener.onResponse(null);
        }

        indicesToMarkReadOnly.removeIf(index -> state.getBlocks().indexBlocked(ClusterBlockLevel.WRITE, index));
        if (indicesToMarkReadOnly.isEmpty() == false) {
            updateIndicesReadOnly(indicesToMarkReadOnly, listener, true);
        } else {
            listener.onResponse(null);
        }
    }

    // exposed for tests to override
    long sizeOfRelocatingShards(RoutingNode routingNode, DiskUsage diskUsage, ClusterInfo info, ClusterState reroutedClusterState) {
        return DiskThresholdDecider.sizeOfRelocatingShards(routingNode, true,
            diskUsage.getPath(), info, reroutedClusterState.metadata(), reroutedClusterState.routingTable());
    }

    private void markNodesMissingUsageIneligibleForRelease(RoutingNodes routingNodes, ImmutableOpenMap<String, DiskUsage> usages,
                                                           Set<String> indicesToMarkIneligibleForAutoRelease) {
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
            logger.debug(new ParameterizedMessage("setting indices [{}] read-only failed", readOnly), e);
            setLastRunTimeMillis();
            listener.onFailure(e);
        });
        Settings readOnlySettings = readOnly ? Settings.builder()
            .put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, Boolean.TRUE.toString()).build() :
            Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE).build();
        client.admin().indices().prepareUpdateSettings(indicesToUpdate.toArray(Strings.EMPTY_ARRAY))
            .setSettings(readOnlySettings)
            .execute(ActionListener.map(wrappedListener, r -> null));
    }

    private static void cleanUpRemovedNodes(ObjectLookupContainer<String> nodesToKeep, Set<String> nodesToCleanUp) {
        for (String node : nodesToCleanUp) {
            if (nodesToKeep.contains(node) == false) {
                nodesToCleanUp.remove(node);
            }
        }
    }
}
