/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IngestMetricsService implements ClusterStateListener {

    /**
     * Ingest load samples older than this value will be considered not exact ingest loads.
     * The default (35s) is based on {@link IngestLoadSampler#MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING} plus some
     * delay for receiving the updates.
     */
    public static final Setting<TimeValue> ACCURATE_LOAD_WINDOW = Setting.timeSetting(
        "serverless.autoscaling.ingest_metrics.accurate_load_window",
        TimeValue.timeValueSeconds(35),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Ingest load samples older than this value will be removed from the list of ingest loads.
    public static final Setting<TimeValue> STALE_LOAD_WINDOW = Setting.timeSetting(
        "serverless.autoscaling.ingest_metrics.stale_load_window",
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Double> HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING = Setting.doubleSetting(
        "serverless.autoscaling.ingest_metrics.high_ingestion_load_weight_during_scaling",
        0.0,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Double> LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING = Setting.doubleSetting(
        "serverless.autoscaling.ingest_metrics.low_ingestion_load_weight_during_scaling",
        1.0,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW = Setting.timeSetting(
        "serverless.autoscaling.ingest_metrics.load_adjustment_after_scaling_window",
        TimeValue.THIRTY_SECONDS,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME = "es.autoscaling.indexing.node_ingest_load.current";

    private static final Logger logger = LogManager.getLogger(IngestMetricsService.class);

    private volatile TimeValue accurateLoadWindow;
    private volatile TimeValue staleLoadWindow;
    private volatile boolean initialized;
    private volatile double highIngestionLoadWeightDuringScaling;
    private volatile double lowIngestionLoadWeightDuringScaling;
    /**
     * The period of time (defaults to 30s, configurable) to keep adjusting ingest load after all shutting down
     * indexing nodes have left the cluster.
     */
    private volatile long loadAdjustmentAfterScalingWindowInNanos;
    /**
     * This is the base timestamp to compute the elapsed time since the last shutting down indexing node has left
     * the cluster. It is updated in {@link #clusterChanged} when the indexing node is removed from the cluster,
     * regardless whether it has left any unassigned shards. It is also updated in {@link #maybeAdjustIngestLoads}
     * when adjustment is applied due to shutting down indexing nodes. Updates in these two places should give
     * an accurate estimation on when we should or should not apply the after-scaling adjustment regardless the
     * execution orders of {@link #clusterChanged} and {@link #getIndexTierMetrics}.
     */
    private AtomicLong loadAdjustmentAfterScalingBaseTimeInNanos = new AtomicLong(0L);
    private final LongSupplier relativeTimeInNanosSupplier;
    private final MemoryMetricsService memoryMetricsService;
    private final Map<String, NodeIngestLoad> nodesIngestLoad = ConcurrentCollections.newConcurrentMap();
    private final AtomicReference<RawAndAdjustedNodeIngestLoadSnapshots> lastNodeIngestLoadSnapshotsRef = new AtomicReference<>(null);

    record RawAndAdjustedNodeIngestLoadSnapshots(List<NodeIngestLoadSnapshot> raw, @Nullable List<NodeIngestLoadSnapshot> adjusted) {}

    public IngestMetricsService(
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeInNanosSupplier,
        MemoryMetricsService memoryMetricsService,
        MeterRegistry meterRegistry
    ) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.memoryMetricsService = memoryMetricsService;
        clusterSettings.initializeAndWatch(ACCURATE_LOAD_WINDOW, value -> this.accurateLoadWindow = value);
        clusterSettings.initializeAndWatch(STALE_LOAD_WINDOW, value -> this.staleLoadWindow = value);
        clusterSettings.initializeAndWatch(
            HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING,
            value -> this.highIngestionLoadWeightDuringScaling = value
        );
        clusterSettings.initializeAndWatch(
            LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING,
            value -> this.lowIngestionLoadWeightDuringScaling = value
        );
        clusterSettings.initializeAndWatch(
            LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW,
            value -> this.loadAdjustmentAfterScalingWindowInNanos = value.getNanos()
        );

        setupMetrics(meterRegistry);
    }

    private void setupMetrics(MeterRegistry meterRegistry) {
        meterRegistry.registerDoublesGauge(NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME, "The last polled node ingest loads", "unit", () -> {
            final var lastNodeIngestLoadSnapshots = lastNodeIngestLoadSnapshotsRef.getAndSet(null);
            if (lastNodeIngestLoadSnapshots == null) {
                return List.of();
            }
            final List<NodeIngestLoadSnapshot> raw = lastNodeIngestLoadSnapshots.raw();
            final List<NodeIngestLoadSnapshot> adjusted = lastNodeIngestLoadSnapshots.adjusted();
            final List<DoubleWithAttributes> values = new ArrayList<>(raw.size() + (adjusted == null ? 0 : adjusted.size()));
            raw.forEach(nodeIngestLoadSnapshot -> values.add(buildNodeIngestLoadMetricValue(nodeIngestLoadSnapshot, false)));
            if (adjusted != null) {
                adjusted.forEach(nodeIngestLoadSnapshot -> values.add(buildNodeIngestLoadMetricValue(nodeIngestLoadSnapshot, true)));
            }
            return values;
        });
    }

    private static DoubleWithAttributes buildNodeIngestLoadMetricValue(NodeIngestLoadSnapshot nodeIngestLoadSnapshot, boolean adjusted) {
        return new DoubleWithAttributes(
            nodeIngestLoadSnapshot.load(),
            Map.of(
                "node_id",
                nodeIngestLoadSnapshot.nodeId(),
                "node_name",
                nodeIngestLoadSnapshot.nodeName(),
                "quality",
                nodeIngestLoadSnapshot.metricQuality().getLabel(),
                "adjusted",
                adjusted
            )
        );
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesDelta().hasChanges() == false) {
            return;
        }

        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            nodesIngestLoad.clear();
            initialized = false;
            return;
        }

        if (event.nodesDelta().masterNodeChanged() || initialized == false) {
            for (DiscoveryNode node : event.state().nodes()) {
                if (isIndexNode(node)) {
                    nodesIngestLoad.computeIfAbsent(node.getId(), unused -> new NodeIngestLoad(node.getId(), node.getName()));
                }
            }
            initialized = true;
        }

        if (event.nodesChanged()) {
            for (DiscoveryNode node : event.nodesDelta().addedNodes()) {
                if (isIndexNode(node)) {
                    nodesIngestLoad.computeIfAbsent(node.getId(), unused -> new NodeIngestLoad(node.getId(), node.getName()));
                }
            }

            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                if (isIndexNode(removedNode)) {
                    var removedNodeId = removedNode.getId();
                    var removedNodeIngestLoad = nodesIngestLoad.get(removedNodeId);
                    if (removedNodeIngestLoad != null) {
                        if (successfulPlannedNodeRemoval(event.state(), removedNodeId)) {
                            // Planned node removal that finished successfully, no need to keep reporting its ingestion load.
                            nodesIngestLoad.remove(removedNodeId);
                        } else {
                            // Potentially unexpected node removal, or a planned removal that left some shards unassigned.
                            // Keep reporting the last ingestion load but with a MINIMUM quality to avoid scaling down
                            removedNodeIngestLoad.setQualityToMinimum();
                        }
                        if (hasLastShuttingDownIndexNodeLeft(event.state(), removedNodeId)) {
                            loadAdjustmentAfterScalingBaseTimeInNanos.updateAndGet(existing -> Math.max(relativeTimeInNanos(), existing));
                        }
                    }
                }
            }
        }
    }

    void trackNodeIngestLoad(ClusterState state, String nodeId, String nodeName, long metricSeqNo, double newIngestLoad) {
        // Drop a (delayed) metric publication from a planned removal that finished successfully (i.e. left no unassigned shards behind).
        // However, if the metric arrives after the node is gone and there is no shutdown metadata, we're treating it as we
        // do for nodes that disappear w/o any shutdown marker, i.e., we assume this is a node that temporarily dropped
        // out (or very recently joined) and to be safe, we keep reporting its ingestion load with a MINIMUM quality. The recorded
        // ingestion load gets removed once there are no unassigned entries from this node.
        if (successfulPlannedNodeRemoval(state, nodeId)) {
            logger.debug("dropping ingestion load metric received from removed node {} which left no shards unassigned", nodeId);
            return;
        }

        var nodeIngestStats = nodesIngestLoad.computeIfAbsent(nodeId, unused -> new NodeIngestLoad(nodeId, nodeName));
        // We track ingestion loads from nodes that left unassigned shards with a MINIMUM quality, to avoid scale down.
        var quality = state.nodes().get(nodeId) != null ? MetricQuality.EXACT : MetricQuality.MINIMUM;
        nodeIngestStats.setLatestReadingTo(newIngestLoad, metricSeqNo, quality);
    }

    public IndexTierMetrics getIndexTierMetrics(ClusterState clusterState) {
        var nodeLoadIterator = nodesIngestLoad.entrySet().iterator();
        final List<NodeIngestLoadSnapshot> ingestLoads = new ArrayList<>();
        while (nodeLoadIterator.hasNext()) {
            var nodeIngestStatsEntry = nodeLoadIterator.next();
            var nodeIngestLoad = nodeIngestStatsEntry.getValue();
            if (shouldRemoveIngestLoadEntry(clusterState, nodeIngestStatsEntry.getKey(), nodeIngestLoad)) {
                nodeLoadIterator.remove();
            } else {
                if (nodeIngestLoad.isWithinAccurateWindow() == false) {
                    logger.warn(
                        "reported node ingest load is older than {} seconds (accurate_load_window) for node ID [{}}]",
                        accurateLoadWindow.getSeconds(),
                        nodeIngestStatsEntry.getKey()
                    );
                }
                ingestLoads.add(nodeIngestLoad.getIngestLoadSnapshot());
            }
        }
        final var adjustedIngestLoads = maybeAdjustIngestLoads(clusterState, ingestLoads);
        lastNodeIngestLoadSnapshotsRef.set(
            new RawAndAdjustedNodeIngestLoadSnapshots(ingestLoads, adjustedIngestLoads == ingestLoads ? null : adjustedIngestLoads)
        );
        return new IndexTierMetrics(adjustedIngestLoads, memoryMetricsService.getMemoryMetrics());
    }

    // Package private for testing
    RawAndAdjustedNodeIngestLoadSnapshots getLastNodeIngestLoadSnapshots() {
        return lastNodeIngestLoadSnapshotsRef.get();
    }

    private boolean shouldRemoveIngestLoadEntry(ClusterState state, String nodeId, NodeIngestLoad nodeIngestLoad) {
        if (nodeIngestLoad.isStale()) {
            return true;
        }
        // Remove non-exact ingestion loads belonging to nodes no longer in the cluster and
        // (no longer) have unassigned shards attributed to them.
        if (nodeIngestLoad.quality.equals(MetricQuality.MINIMUM)) {
            return nonExistingNodeWithNoUnassignedShards(state, nodeId);
        }
        return false;
    }

    // Package-private for testing
    List<NodeIngestLoadSnapshot> maybeAdjustIngestLoads(ClusterState clusterState, List<NodeIngestLoadSnapshot> ingestLoads) {
        boolean adjustIngestionLoadWeight = highIngestionLoadWeightDuringScaling < 1.0 || lowIngestionLoadWeightDuringScaling < 1.0;
        if (adjustIngestionLoadWeight == false) {
            return ingestLoads;
        }
        // During a scaling event, we need to account for the extra load that is caused by ongoing relocations. Poor relocation performance
        // can lead to frequent autoscaling events, where during a scale down we see an increased ingestion load and scale up, only to scale
        // down as soon as possible. We detect a scaling event by looking for shutdown metadata in the cluster. During this time we adjust
        // the ingestion loads reported by applying some weight to control the impact of the relocation-related load on the autoscaling
        // behaviour. One simple approach is to just report the lowest ingestion loads for the number of non-shutting-down nodes with a
        // non-exact metric quality. This could be enough to prevent scale ups due to the load caused by relocations, while still allowing
        // scale ups if the cluster sees so much load that it leads to an increase in all ingestion loads. (The non-exact quality prevents
        // an unwanted scale down.) However, it is still possible that the extra load is only on one (or few) node(s) and simply dropping
        // the high ingestion loads during the scaling event can lead to suppressing a scale up when it would be beneficial. To be able to
        // control this behaviour we generalize this simple approach to allow setting weights on the ingestion loads. We sort the list of
        // ingestion loads and apply two different weights, one to the firs N entries (with smaller values) and one to the remaining S
        // entries (with larger values), where S is the number of nodes in the indexing tier that have a shutdown marker, and N is the
        // number of indexing nodes that have none. This allows controlling how much the S highest ingestion loads and the N lowest ones
        // impact the total reported ingestion load. As an example to keep the lowest N (number of nodes w/o a shutdown marker) ingestion
        // loads and 50% of each of the highest ingestion loads, we can use lowIngestionLoadWeightDuringScaling = 1.0 and
        // highIngestionLoadWeightDuringScaling = 0.5. Note that, N and S is used here to split the list, and it doesn't necessarily mean
        // that the N entries in the list all belong to nodes that have no shutdown markers.
        final var indexingNodesByShutdownStatus = groupIndexNodesByShutdownStatus(clusterState);
        final int shuttingDownIndexingNodes = indexingNodesByShutdownStatus.get(true);
        final int notShuttingDownIndexingNodes = indexingNodesByShutdownStatus.get(false);

        // No adjustment is needed if (1) No shutting down node and (2) Sufficient time has passed since last shutting down node left
        if (shuttingDownIndexingNodes == 0 && shouldAdjustForAfterScaling() == false) {
            return ingestLoads;
        }

        final int numNodesForLowWeight;
        if (shuttingDownIndexingNodes != 0) {
            numNodesForLowWeight = notShuttingDownIndexingNodes;
            // The cluster still has shutting down nodes. The after scaling base time cannot be earlier than this time.
            // We update the timestamp here because this method may see a new cluster state before it is processed by the
            // clusterChanged method. If we update the timestamp only in clusterChanged, this method may see a new state
            // with no shutdown nodes and immediately stop adjustment before the timestamp can be updated in clusterChanged.
            // Therefore, updating the timestamp here as well can help maintain its freshness. Another way to look at this
            // is that we keep updating the timestamp here while a node is shutting down and clusterChanged updates it
            // one last time when the node finally leaves. Thus updating in both places provides a greater coverage.
            loadAdjustmentAfterScalingBaseTimeInNanos.updateAndGet(existing -> Math.max(relativeTimeInNanos(), existing));
        } else {
            // Apply high weight to only a single node for after scaling adjustment
            numNodesForLowWeight = ingestLoads.size() - 1;
        }
        ingestLoads.sort(Comparator.comparingDouble(NodeIngestLoadSnapshot::load));
        final List<NodeIngestLoadSnapshot> adjustedIngestLoads = IntStream.range(0, ingestLoads.size()).mapToObj(i -> {
            final NodeIngestLoadSnapshot ingestLoadSnapshot = ingestLoads.get(i);
            var weight = i < numNodesForLowWeight ? lowIngestionLoadWeightDuringScaling : highIngestionLoadWeightDuringScaling;
            return new NodeIngestLoadSnapshot(
                ingestLoadSnapshot.nodeId(),
                ingestLoadSnapshot.nodeName(),
                ingestLoadSnapshot.load() * weight,
                MetricQuality.MINIMUM
            );
        }).toList();
        logger.debug(
            () -> Strings.format(
                "adjusting ingest loads from %s to %s "
                    + "(number of indexing nodes: %d, number of indexing nodes with a shutdown marker: %d, %s: %.2f, %s: %.2f",
                ingestLoads,
                adjustedIngestLoads,
                shuttingDownIndexingNodes + notShuttingDownIndexingNodes,
                shuttingDownIndexingNodes,
                HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(),
                highIngestionLoadWeightDuringScaling,
                LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(),
                lowIngestionLoadWeightDuringScaling
            )
        );
        return adjustedIngestLoads;
    }

    private boolean shouldAdjustForAfterScaling() {
        if (loadAdjustmentAfterScalingWindowInNanos <= 0 || loadAdjustmentAfterScalingBaseTimeInNanos.get() == 0) {
            return false;
        }
        return relativeTimeInNanos() - loadAdjustmentAfterScalingBaseTimeInNanos.get() <= loadAdjustmentAfterScalingWindowInNanos;
    }

    // Package private for testing
    long getLoadAdjustmentAfterScalingBaseTimeInNanos() {
        return loadAdjustmentAfterScalingBaseTimeInNanos.get();
    }

    static Map<Boolean, Integer> groupIndexNodesByShutdownStatus(ClusterState clusterState) {
        final var shuttingDownNodes = Set.copyOf(clusterState.metadata().nodeShutdowns().getAllNodeIds());
        return clusterState.nodes()
            .stream()
            .filter(IngestMetricsService::isIndexNode)
            .collect(
                Collectors.toMap(
                    node -> shuttingDownNodes.contains(node.getId()),
                    node -> 1,
                    Integer::sum,
                    () -> new HashMap<>(Map.of(true, 0, false, 0))
                )
            );
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    private static boolean isIndexNode(DiscoveryNode node) {
        // TODO: move to core
        return node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE);
    }

    private static long getUnassignedShardsForNodeId(ClusterState state, String nodeId) {
        return state.getRoutingNodes()
            .unassigned()
            .stream()
            .filter(s -> s.isPromotableToPrimary() && s.unassignedInfo().lastAllocatedNodeId().equals(nodeId))
            .count();
    }

    // Whether the given node is not in the cluster and there are no unasigned shards that are attributed to it.
    private static boolean nonExistingNodeWithNoUnassignedShards(ClusterState state, String nodeId) {
        return state.nodes().get(nodeId) == null && getUnassignedShardsForNodeId(state, nodeId) == 0;
    }

    private static boolean isNodeMarkedForRemoval(String nodeId, NodesShutdownMetadata shutdownMetadata) {
        var nodeShutdownMetadata = shutdownMetadata.get(nodeId);
        return nodeShutdownMetadata != null && nodeShutdownMetadata.getType().isRemovalType();
    }

    // Whether the node is removed from the cluster after being marked for removal, and has left the cluster w/o leaving unassigned shards.
    // Note that this relies on the shutdown marker being present in the immediate state that comes after the node leaves the cluster.
    private static boolean successfulPlannedNodeRemoval(ClusterState state, String nodeId) {
        return isNodeMarkedForRemoval(nodeId, state.metadata().nodeShutdowns()) && nonExistingNodeWithNoUnassignedShards(state, nodeId);
    }

    private boolean hasLastShuttingDownIndexNodeLeft(ClusterState state, String nodeId) {
        return isNodeMarkedForRemoval(nodeId, state.metadata().nodeShutdowns()) && groupIndexNodesByShutdownStatus(state).get(true) == 0;
    }

    // Package-private for testing
    double getHighIngestionLoadWeightDuringScaling() {
        return highIngestionLoadWeightDuringScaling;
    }

    // Package-private for testing
    double getLowIngestionLoadWeightDuringScaling() {
        return lowIngestionLoadWeightDuringScaling;
    }

    private class NodeIngestLoad {
        private final String nodeId;
        private final String nodeName;
        private double ingestLoad;
        private long latestSampleTimeInNanos = relativeTimeInNanos();
        private long maxSeqNo = Long.MIN_VALUE;
        private MetricQuality quality = MetricQuality.MISSING;

        NodeIngestLoad(String nodeId, String nodeName) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
        }

        synchronized void setLatestReadingTo(double ingestLoad, long metricSeqNo, MetricQuality quality) {
            if (metricSeqNo > maxSeqNo) {
                this.ingestLoad = ingestLoad;
                this.quality = quality;
                this.latestSampleTimeInNanos = relativeTimeInNanos();
                this.maxSeqNo = metricSeqNo;
            }
        }

        synchronized void setQualityToMinimum() {
            this.quality = MetricQuality.MINIMUM;
        }

        synchronized NodeIngestLoadSnapshot getIngestLoadSnapshot() {
            if (quality == MetricQuality.EXACT && isWithinAccurateWindow() == false) {
                quality = MetricQuality.MINIMUM;
            }
            return new NodeIngestLoadSnapshot(nodeId, nodeName, ingestLoad, quality);
        }

        synchronized boolean isWithinAccurateWindow() {
            return timeSinceLastSampleInNanos() < accurateLoadWindow.getNanos();
        }

        synchronized boolean isStale() {
            return timeSinceLastSampleInNanos() >= staleLoadWindow.getNanos();
        }

        private long timeSinceLastSampleInNanos() {
            return relativeTimeInNanos() - latestSampleTimeInNanos;
        }

        @Override
        public String toString() {
            return "NodeIngestLoad{"
                + "ingestLoad="
                + ingestLoad
                + ", latestSampleTimeInNanos="
                + latestSampleTimeInNanos
                + ", maxSeqNo="
                + maxSeqNo
                + ", quality="
                + quality
                + '}';
        }
    }
}
