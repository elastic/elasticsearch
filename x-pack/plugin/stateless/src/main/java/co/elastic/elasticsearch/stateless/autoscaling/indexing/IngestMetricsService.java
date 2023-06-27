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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;

public class IngestMetricsService implements ClusterStateListener {
    // TODO: align defaults with leader/follower checks and default metric push frequency
    public static final Setting<TimeValue> ACCURATE_LOAD_WINDOW = Setting.timeSetting(
        "serverless.autoscaling.ingest_metrics.accurate_load_window",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> INACCURATE_LOAD_WINDOW = Setting.timeSetting(
        "serverless.autoscaling.ingest_metrics.inaccurate_load_window",
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope
    );

    private final TimeValue accurateLoadWindow;
    private final TimeValue inaccurateLoadWindow;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final MemoryMetricsService memoryMetricsService;
    private final Map<String, NodeIngestLoad> nodesIngestLoad = ConcurrentCollections.newConcurrentMap();

    public IngestMetricsService(Settings settings, LongSupplier relativeTimeInNanosSupplier, MemoryMetricsService memoryMetricsService) {
        this.accurateLoadWindow = ACCURATE_LOAD_WINDOW.get(settings);
        this.inaccurateLoadWindow = INACCURATE_LOAD_WINDOW.get(settings);
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.memoryMetricsService = memoryMetricsService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesDelta().hasChanges() == false) {
            return;
        }

        if (event.localNodeMaster() == false) {
            nodesIngestLoad.clear();
            return;
        }

        if (event.nodesDelta().masterNodeChanged()) {
            for (DiscoveryNode node : event.state().nodes()) {
                if (isIndexNode(node)) {
                    nodesIngestLoad.computeIfAbsent(node.getId(), unused -> new NodeIngestLoad());
                }
            }
        }

        if (event.nodesChanged()) {
            for (DiscoveryNode node : event.nodesDelta().addedNodes()) {
                if (isIndexNode(node)) {
                    nodesIngestLoad.computeIfAbsent(node.getId(), unused -> new NodeIngestLoad());
                }
            }

            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                if (isIndexNode(removedNode)) {
                    nodesIngestLoad.get(removedNode.getId()).setQualityToMinimum();
                }
            }
        }
    }

    void trackNodeIngestLoad(String nodeId, long metricSeqNo, double newIngestLoad) {
        var nodeIngestStats = nodesIngestLoad.computeIfAbsent(nodeId, unused -> new NodeIngestLoad());
        nodeIngestStats.setLatestReadingTo(newIngestLoad, metricSeqNo);
    }

    @Nullable
    public IndexTierMetrics getIndexTierMetrics() {
        var nodeLoadIterator = nodesIngestLoad.entrySet().iterator();
        var ingestLoads = new ArrayList<NodeIngestLoadSnapshot>();
        while (nodeLoadIterator.hasNext()) {
            var nodeIngestStatsEntry = nodeLoadIterator.next();
            var value = nodeIngestStatsEntry.getValue();
            if (value.isStale()) {
                nodeLoadIterator.remove();
            } else {
                ingestLoads.add(value.getIngestLoadSnapshot());
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
            if (quality == MetricQuality.EXACT && timeSinceLastSampleInNanos() >= accurateLoadWindow.getNanos()) {
                quality = MetricQuality.MINIMUM;
            }
            return new NodeIngestLoadSnapshot(ingestLoad, quality);
        }

        synchronized boolean isStale() {
            return timeSinceLastSampleInNanos() >= inaccurateLoadWindow.getNanos();
        }

        private long timeSinceLastSampleInNanos() {
            return relativeTimeInNanos() - latestSampleTimeInNanos;
        }
    }
}
