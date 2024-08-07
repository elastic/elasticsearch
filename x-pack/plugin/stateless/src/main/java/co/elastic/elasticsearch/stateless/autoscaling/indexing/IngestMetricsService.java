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
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

public class IngestMetricsService implements ClusterStateListener {

    // Ingest load samples older than this value will be considered not exact ingest loads.
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

    public static final Setting<Boolean> SHUTDOWN_ATTENUATION_ENABLED = Setting.boolSetting(
        "serverless.autoscaling.ingest_metrics.shutdown_attenuation.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(IngestMetricsService.class);

    private volatile TimeValue accurateLoadWindow;
    private volatile TimeValue staleLoadWindow;
    private volatile boolean initialized;
    private volatile boolean shutdownAttenuationEnabled;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final MemoryMetricsService memoryMetricsService;
    private final Map<String, NodeIngestLoad> nodesIngestLoad = ConcurrentCollections.newConcurrentMap();

    public IngestMetricsService(
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeInNanosSupplier,
        MemoryMetricsService memoryMetricsService
    ) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.memoryMetricsService = memoryMetricsService;
        clusterSettings.initializeAndWatch(ACCURATE_LOAD_WINDOW, value -> this.accurateLoadWindow = value);
        clusterSettings.initializeAndWatch(STALE_LOAD_WINDOW, value -> this.staleLoadWindow = value);
        clusterSettings.initializeAndWatch(SHUTDOWN_ATTENUATION_ENABLED, value -> this.shutdownAttenuationEnabled = value);
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
                    nodesIngestLoad.computeIfAbsent(node.getId(), unused -> new NodeIngestLoad());
                }
            }
            initialized = true;
        }

        if (event.nodesChanged()) {
            for (DiscoveryNode node : event.nodesDelta().addedNodes()) {
                if (isIndexNode(node)) {
                    nodesIngestLoad.computeIfAbsent(node.getId(), unused -> new NodeIngestLoad());
                }
            }

            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                if (isIndexNode(removedNode)) {
                    var removedNodeId = removedNode.getId();
                    var removedNodeIngestLoad = nodesIngestLoad.get(removedNodeId);
                    if (removedNodeIngestLoad != null) {
                        if (isNodeMarkedForRemoval(removedNodeId, event.state().metadata().nodeShutdowns())) {
                            // Planned node removal, no need to track the ingestion load.
                            nodesIngestLoad.remove(removedNodeId);
                        } else {
                            // Potentially unexpected node removal, keep reporting the ingestion load.
                            removedNodeIngestLoad.setQualityToMinimum();
                        }
                    }
                }
            }
        }
    }

    void trackNodeIngestLoad(ClusterState state, String nodeId, long metricSeqNo, double newIngestLoad) {
        // Prevent a delayed metric publication to add back a removed node to the list of ingestion loads which would
        // lead to continued reporting of the ingestion load until it gets stale, although we know that node removal
        // was planned and the node does not come back.
        // However, if the metric arrives after the node is gone and there is no shutdown metadata, we're treating it as we
        // do for nodes that disappear w/o any shutdown marker, i.e., we assume this is a node that temporarily dropped
        // out and to be safe, we keep reporting its ingestion load until it gets stale and removed.
        if (state.nodes().get(nodeId) == null && isNodeMarkedForRemoval(nodeId, state.metadata().nodeShutdowns())) {
            logger.debug("dropping ingestion load metric received from removed node {}", nodeId);
            return;
        }
        var nodeIngestStats = nodesIngestLoad.computeIfAbsent(nodeId, unused -> new NodeIngestLoad());
        nodeIngestStats.setLatestReadingTo(newIngestLoad, metricSeqNo);
    }

    private static boolean isNodeMarkedForRemoval(String nodeId, NodesShutdownMetadata shutdownMetadata) {
        var nodeShutdownMetadata = shutdownMetadata.get(nodeId);
        return nodeShutdownMetadata != null && nodeShutdownMetadata.getType().isRemovalType();
    }

    public IndexTierMetrics getIndexTierMetrics(ClusterState clusterState) {
        var nodeLoadIterator = nodesIngestLoad.entrySet().iterator();
        List<NodeIngestLoadSnapshot> ingestLoads = new ArrayList<>();
        while (nodeLoadIterator.hasNext()) {
            var nodeIngestStatsEntry = nodeLoadIterator.next();
            var value = nodeIngestStatsEntry.getValue();
            if (value.isStale()) {
                nodeLoadIterator.remove();
            } else {
                if (value.isWithinAccurateWindow() == false) {
                    logger.warn(
                        "reported node ingest load is older than {} seconds (accurate_load_window) for node ID [{}}]",
                        accurateLoadWindow.getSeconds(),
                        nodeIngestStatsEntry.getKey()
                    );
                }
                ingestLoads.add(value.getIngestLoadSnapshot());
            }
        }

        if (shutdownAttenuationEnabled) {
            // If there are shutting-down nodes, we attenuate the overall reporting by removing entries with higher values
            // so that the final number of entries equals to the number of "not-shutting-down" nodes. The idea is that if
            // relocation causes temporary queuing on either source or target node, we don't want them to impact autoscaling
            // decision and potentially lead to over upsizing. In case of a genuine high load during scaling, it is likely
            // that most nodes are affected by the load and the extra load might show itself in the lower ingestion values
            // as well to trigger a scale up. That said, we'd still be missing the genuine extra load that might on the
            // shutting-down nodes (waiting for relocation).
            final var shuttingDownNodes = Set.copyOf(clusterState.metadata().nodeShutdowns().getAllNodeIds());
            if (shuttingDownNodes.isEmpty() == false) {
                final int numberNotShuttingDownIndexingNodes = (int) clusterState.nodes()
                    .stream()
                    .filter(IngestMetricsService::isIndexNode)
                    .filter(node -> shuttingDownNodes.contains(node.getId()) == false)
                    .count();
                // When a node leaves the cluster, we set its metric quality to minimum but do not remove it from
                // nodesIngestLoad immediately. Before its metric becomes stale, we will still report it even when
                // it is no longer in the cluster and does not count towards numberNotShuttingDownIndexingNodes.
                // This might skew the following sorting and limiting a bit. We consider this a corner case which
                // will be addressed separately by ES-6956
                final int numberIngestLoads = ingestLoads.size();
                if (numberIngestLoads > numberNotShuttingDownIndexingNodes) {
                    final var existingIngestLoads = ingestLoads;
                    final List<NodeIngestLoadSnapshot> adjustedIngestLoads = ingestLoads.stream()
                        .sorted(Comparator.comparingDouble(NodeIngestLoadSnapshot::load))
                        .limit(numberNotShuttingDownIndexingNodes)
                        .map(e -> new NodeIngestLoadSnapshot(e.load(), MetricQuality.MINIMUM))
                        .toList();
                    logger.debug(
                        () -> Strings.format(
                            "adjusting ingest loads from %s to %s for shutting down nodes",
                            existingIngestLoads,
                            adjustedIngestLoads
                        )
                    );
                    ingestLoads = adjustedIngestLoads;
                }
                // In most cases, ingestLoads.size() >= numberNotShuttingDownIndexingNodes should hold.
                // However, since ingestLoads filters out stale reports and removes the nodesIngestLoad map,
                // in extreme case, a node may fail to report metrics continuously so that it becomes stale
                // for metric while still being part of the cluster. Very unlikely, but technically not impossible.
            }
        }
        return new IndexTierMetrics(Collections.unmodifiableList(ingestLoads), memoryMetricsService.getMemoryMetrics());
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    private static boolean isIndexNode(DiscoveryNode node) {
        // TODO: move to core
        return node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE);
    }

    private class NodeIngestLoad {
        private double ingestLoad;
        private long latestSampleTimeInNanos;
        private long maxSeqNo = Long.MIN_VALUE;
        private MetricQuality quality = MetricQuality.MISSING;

        synchronized void setLatestReadingTo(double ingestLoad, long metricSeqNo) {
            if (metricSeqNo > maxSeqNo) {
                this.ingestLoad = ingestLoad;
                this.quality = MetricQuality.EXACT;
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
            return new NodeIngestLoadSnapshot(ingestLoad, quality);
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
    }
}
