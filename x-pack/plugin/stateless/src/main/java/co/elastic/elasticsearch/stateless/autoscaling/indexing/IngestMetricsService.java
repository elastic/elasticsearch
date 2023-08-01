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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;

public class IngestMetricsService implements ClusterStateListener {
    // Ingest load samples older than this value will be considered not exact ingest loads.
    public static final Setting<TimeValue> ACCURATE_LOAD_WINDOW = Setting.timeSetting(
        "serverless.autoscaling.ingest_metrics.accurate_load_window",
        TimeValue.timeValueSeconds(25),
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

    private volatile TimeValue accurateLoadWindow;
    private volatile TimeValue staleLoadWindow;
    private volatile boolean initialized;
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
        return new IndexTierMetrics(
            Collections.unmodifiableList(ingestLoads),
            memoryMetricsService.getMemoryMetrics(DiscoveryNodeRole.INDEX_ROLE)
        );
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
            return timeSinceLastSampleInNanos() >= staleLoadWindow.getNanos();
        }

        private long timeSinceLastSampleInNanos() {
            return relativeTimeInNanos() - latestSampleTimeInNanos;
        }
    }
}
