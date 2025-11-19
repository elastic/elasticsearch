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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.isNodeMarkedForRemoval;

public class NodeIngestionLoadTracker {

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

    private static final Logger logger = LogManager.getLogger(NodeIngestionLoadTracker.class);

    private volatile TimeValue accurateLoadWindow;
    private volatile TimeValue staleLoadWindow;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final Map<String, Entry> nodesIngestLoad = ConcurrentCollections.newConcurrentMap();

    public NodeIngestionLoadTracker(ClusterSettings clusterSettings, LongSupplier relativeTimeInNanosSupplier) {
        clusterSettings.initializeAndWatch(ACCURATE_LOAD_WINDOW, value -> this.accurateLoadWindow = value);
        clusterSettings.initializeAndWatch(STALE_LOAD_WINDOW, value -> this.staleLoadWindow = value);
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
    }

    Map<String, Entry> getCurrentNodesIngestLoad() {
        return nodesIngestLoad;
    }

    public void currentNodeNoLongerMaster() {
        nodesIngestLoad.clear();
    }

    public void currentNodeBecomesMaster(ClusterState state) {
        for (DiscoveryNode node : state.nodes()) {
            if (IngestMetricsService.isIndexNode(node)) {
                nodesIngestLoad.computeIfAbsent(node.getId(), unused -> new Entry(node.getId(), node.getName()));
            }
        }
    }

    public void indexNodeAdded(DiscoveryNode node) {
        nodesIngestLoad.computeIfAbsent(node.getId(), unused -> new Entry(node.getId(), node.getName()));
    }

    public Entry indexNodeRemoved(ClusterState state, String removedNodeId) {
        var removedNodeIngestLoad = nodesIngestLoad.get(removedNodeId);
        if (removedNodeIngestLoad != null) {
            if (successfulPlannedNodeRemoval(state, removedNodeId)) {
                // Planned node removal that finished successfully, no need to keep reporting its ingestion load.
                nodesIngestLoad.remove(removedNodeId);
            } else {
                // Potentially unexpected node removal, or a planned removal that left some shards unassigned.
                // Keep reporting the last ingestion load but with a MINIMUM quality to avoid scaling down
                removedNodeIngestLoad.setQualityToMinimum();
            }
        }
        return removedNodeIngestLoad;
    }

    public Entry get(String nodeId) {
        return nodesIngestLoad.get(nodeId);
    }

    public void trackNodeIngestLoad(
        ClusterState state,
        String nodeId,
        String nodeName,
        long metricSeqNo,
        IngestionLoad.NodeIngestionLoad newIngestLoad
    ) {
        // Drop a (delayed) metric publication from a planned removal that finished successfully (i.e. left no unassigned shards
        // behind). However, if the metric arrives after the node is gone and there is no shutdown metadata, we're treating it as we
        // do for nodes that disappear w/o any shutdown marker, i.e., we assume this is a node that temporarily dropped
        // out (or very recently joined) and to be safe, we keep reporting its ingestion load with a MINIMUM quality. The recorded
        // ingestion load gets removed once there are no unassigned entries from this node.
        if (successfulPlannedNodeRemoval(state, nodeId)) {
            logger.debug("dropping ingestion load metric received from removed node {} which left no shards unassigned", nodeId);
            return;
        }

        var nodeIngestStats = nodesIngestLoad.computeIfAbsent(nodeId, unused -> new Entry(nodeId, nodeName));
        // We track ingestion loads from nodes that left unassigned shards with a MINIMUM quality, to avoid scale down.
        var quality = state.nodes().get(nodeId) != null ? MetricQuality.EXACT : MetricQuality.MINIMUM;
        nodeIngestStats.setLatestReadingTo(newIngestLoad, metricSeqNo, quality);
    }

    public List<NodeIngestLoadSnapshot> getIngestLoadSnapshots(ClusterState clusterState) {
        final var nodeLoadIterator = nodesIngestLoad.entrySet().iterator();
        final List<NodeIngestLoadSnapshot> ingestLoads = new ArrayList<>(nodesIngestLoad.size());
        while (nodeLoadIterator.hasNext()) {
            final var nodeIngestStatsEntry = nodeLoadIterator.next();
            final var nodeIngestLoad = nodeIngestStatsEntry.getValue();
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
        return ingestLoads;
    }

    private boolean shouldRemoveIngestLoadEntry(ClusterState state, String nodeId, Entry nodeIngestionLoadEntry) {
        if (nodeIngestionLoadEntry.isStale()) {
            return true;
        }
        // Remove non-exact ingestion loads belonging to nodes no longer in the cluster and
        // (no longer) have unassigned shards attributed to them.
        if (nodeIngestionLoadEntry.quality.equals(MetricQuality.MINIMUM)) {
            return nonExistingNodeWithNoUnassignedShards(state, nodeId);
        }
        return false;
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    // Whether the node is removed from the cluster after being marked for removal, and has left the cluster w/o leaving unassigned shards.
    // Note that this relies on the shutdown marker being present in the immediate state that comes after the node leaves the cluster.
    private static boolean successfulPlannedNodeRemoval(ClusterState state, String nodeId) {
        return isNodeMarkedForRemoval(nodeId, state.metadata().nodeShutdowns()) && nonExistingNodeWithNoUnassignedShards(state, nodeId);
    }

    // Whether the given node is not in the cluster and there are no unasigned shards that are attributed to it.
    private static boolean nonExistingNodeWithNoUnassignedShards(ClusterState state, String nodeId) {
        return state.nodes().get(nodeId) == null && getUnassignedShardsForNodeId(state, nodeId) == 0;
    }

    private static long getUnassignedShardsForNodeId(ClusterState state, String nodeId) {
        return state.getRoutingNodes()
            .unassigned()
            .stream()
            .filter(s -> s.isPromotableToPrimary() && s.unassignedInfo().lastAllocatedNodeId().equals(nodeId))
            .count();
    }

    // Package-private for testing
    class Entry {
        private final String nodeId;
        private final String nodeName;
        private IngestionLoad.NodeIngestionLoad ingestLoad;
        private long latestSampleTimeInNanos = relativeTimeInNanos();
        private long maxSeqNo = Long.MIN_VALUE;
        private MetricQuality quality = MetricQuality.MISSING;

        Entry(String nodeId, String nodeName) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.ingestLoad = IngestionLoad.NodeIngestionLoad.EMPTY;
        }

        synchronized void setLatestReadingTo(IngestionLoad.NodeIngestionLoad ingestLoad, long metricSeqNo, MetricQuality quality) {
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
            return new NodeIngestLoadSnapshot(nodeId, nodeName, ingestLoad.totalIngestionLoad(), quality);
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

        // Package private for testing
        IngestionLoad.NodeIngestionLoad getIngestLoad() {
            return ingestLoad;
        }
    }
}
