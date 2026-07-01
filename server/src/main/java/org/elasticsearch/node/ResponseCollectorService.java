/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects statistics about queue size, response time, and service time of
 * tasks executed on each node, making the EWMA of the values available to the
 * coordinating node.
 */
public final class ResponseCollectorService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ResponseCollectorService.class);

    /**
     * The weight parameter used for all moving averages of parameters.
     */
    public static final double ALPHA = 0.3;

    public static final FeatureFlag ARS_FORMULA_ADJUSTMENT_FEATURE_FLAG = new FeatureFlag("ars_formula_adjustment");

    private final ClusterService clusterService;
    private final ConcurrentMap<String, NodeStatistics> nodeIdToStats = ConcurrentCollections.newConcurrentMap();
    /**
     * Tracks when each ARS-candidate node joined the cluster (nanoseconds from
     * {@link System#nanoTime()}). Used to compute the probing-phase duration when the
     * first observation for that node is received. Entries are removed either when the
     * first observation arrives or when the node leaves the cluster.
     */
    private final ConcurrentMap<String, Long> nodeJoinTimeNanos = ConcurrentCollections.newConcurrentMap();
    /**
     * Tracks when each node received its first ARS observation (nanoseconds from
     * {@link System#nanoTime()}). Used to compute the warming-phase duration when the
     * node graduates to warm. Entries are removed either on graduation or when the node
     * leaves the cluster.
     */
    private final ConcurrentMap<String, Long> nodeFirstObservationTimeNanos = ConcurrentCollections.newConcurrentMap();
    /**
     * Minimum observation count for a node to be considered warm. Kept in sync with
     * {@link OperationRouting#ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING} so the
     * warming gauge and graduation log use the same threshold as the routing logic.
     */
    private volatile int warmupSamples;
    private LongHistogram probingDurationHistogram;
    private LongHistogram warmingDurationHistogram;

    public ResponseCollectorService(ClusterService clusterService, MeterRegistry meterRegistry) {
        this.clusterService = clusterService;
        this.warmupSamples = OperationRouting.ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(OperationRouting.ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING, v -> this.warmupSamples = v);
        // Register metrics before adding the cluster-state listener so that all fields
        // (including the histogram references) are fully initialized before any
        // clusterChanged callback can fire on the cluster-applier thread.
        registerMetrics(meterRegistry);
        clusterService.addListener(this);
    }

    /**
     * Registers all OTel instruments for ARS node lifecycle observability.
     * <ul>
     *   <li><b>es.ars.nodes.probing.current</b> (gauge) — data/search nodes for which this
     *       coordinator has not yet received a single ARS response. Computed on each OTel
     *       collection tick (~10 s) from live maps; zero overhead between ticks.</li>
     *   <li><b>es.ars.nodes.warming.current</b> (gauge) — nodes whose observation count is
     *       above zero but still below the warmup threshold.</li>
     *   <li><b>es.ars.nodes.probing.time</b> (histogram) — milliseconds a node spent in
     *       the probing state, recorded when its first ARS observation arrives.</li>
     *   <li><b>es.ars.nodes.warming.time</b> (histogram) — milliseconds a node spent in
     *       the warming state, recorded when it graduates to warm.</li>
     * </ul>
     */
    @SuppressWarnings("resource")
    private void registerMetrics(MeterRegistry meterRegistry) {
        meterRegistry.registerLongGauge(
            "es.ars.nodes.probing.current",
            "Number of data/search nodes in the cluster for which this coordinator has no ARS statistics yet",
            "1",
            this::countProbingNodes
        );
        meterRegistry.registerLongGauge(
            "es.ars.nodes.warming.current",
            "Number of nodes whose ARS observation count is above zero but below the warmup threshold",
            "1",
            this::countWarmingNodes
        );
        probingDurationHistogram = meterRegistry.registerLongHistogram(
            "es.ars.nodes.probing.time",
            "Milliseconds from a data/search node joining the cluster to this coordinator's first ARS observation for it",
            "ms"
        );
        warmingDurationHistogram = meterRegistry.registerLongHistogram(
            "es.ars.nodes.warming.time",
            "Time in milliseconds from the first ARS observation for a node until it accumulates enough observations to graduate to warm",
            "ms"
        );
    }

    private LongWithAttributes countProbingNodes() {
        long count = 0;
        for (DiscoveryNode node : clusterService.state().nodes()) {
            if (isArsCandidate(node) && nodeIdToStats.containsKey(node.getId()) == false) {
                count++;
            }
        }
        return new LongWithAttributes(count);
    }

    private LongWithAttributes countWarmingNodes() {
        final int threshold = warmupSamples;
        long count = 0;
        for (NodeStatistics ns : nodeIdToStats.values()) {
            if (ns.observationCount < threshold) {
                count++;
            }
        }
        return new LongWithAttributes(count);
    }

    /**
     * Returns {@code true} for nodes that are candidates for ARS shard routing: traditional
     * data-tier nodes and stateless search nodes. Pure master, ingest, and coordinating-only
     * nodes are excluded because they never receive shard-level search requests.
     */
    private static boolean isArsCandidate(DiscoveryNode node) {
        return node.getRoles().stream().anyMatch(r -> r.canContainData() || "search".equals(r.roleName()));
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesAdded()) {
            final long now = System.nanoTime();
            for (DiscoveryNode addedNode : event.nodesDelta().addedNodes()) {
                if (isArsCandidate(addedNode)) {
                    nodeJoinTimeNanos.put(addedNode.getId(), now);
                }
            }
        }
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNode(removedNode.getId());
            }
        }
    }

    void removeNode(String nodeId) {
        nodeIdToStats.remove(nodeId);
        nodeJoinTimeNanos.remove(nodeId);
        nodeFirstObservationTimeNanos.remove(nodeId);
    }

    public void addNodeStatistics(String nodeId, int queueSize, long responseTimeNanos, long avgServiceTimeNanos) {
        // Snapshot the observation count inside the compute lambda so we can log state
        // transitions after the segment lock is released. Reading observationCount from the
        // returned NodeStatistics is unsafe: that object is shared in the map and another
        // thread may already have incremented the field before we read it outside the lock.
        final AtomicLong observationCountSnapshot = new AtomicLong();
        nodeIdToStats.compute(nodeId, (id, ns) -> {
            if (ns == null) {
                observationCountSnapshot.set(1);
                ExponentiallyWeightedMovingAverage queueEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, queueSize);
                ExponentiallyWeightedMovingAverage responseEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, responseTimeNanos);
                return new NodeStatistics(nodeId, queueEWMA, responseEWMA, avgServiceTimeNanos);
            } else {
                ns.queueSize.addValue((double) queueSize);
                ns.responseTime.addValue((double) responseTimeNanos);
                ns.serviceTime = avgServiceTimeNanos;
                ns.observationCount++;
                observationCountSnapshot.set(ns.observationCount);
                return ns;
            }
        });

        recordTransition(nodeId, observationCountSnapshot.get());
    }

    /**
     * Logs and records telemetry for ARS node lifecycle transitions triggered by a new
     * observation. Called immediately after the {@code compute()} block so the segment lock
     * is no longer held.
     * <ul>
     *   <li>{@code count == 1}: node leaves probing, enters warming — logs the event, records
     *       probing duration if a join timestamp is available, and stores the warming-start
     *       timestamp.</li>
     *   <li>{@code count == warmupSamples}: node graduates from warming to warm — logs the
     *       event and records warming duration if a warming-start timestamp is available.</li>
     * </ul>
     */
    private void recordTransition(String nodeId, long count) {
        if (count == 1) {
            logger.info("Node [{}] entered ARS warming state (first observation recorded)", nodeId);
            final long now = System.nanoTime();
            final Long joinTime = nodeJoinTimeNanos.remove(nodeId);
            if (joinTime != null) {
                probingDurationHistogram.record((now - joinTime) / 1_000_000L);
            }
            nodeFirstObservationTimeNanos.put(nodeId, now);
        } else {
            final int threshold = warmupSamples;
            if (threshold > 0 && count == threshold) {
                logger.info("Node [{}] graduated from ARS warming to warm (observations={})", nodeId, threshold);
                final Long warmingStart = nodeFirstObservationTimeNanos.remove(nodeId);
                if (warmingStart != null) {
                    warmingDurationHistogram.record((System.nanoTime() - warmingStart) / 1_000_000L);
                }
            }
        }
    }

    public Map<String, ComputedNodeStats> getAllNodeStatistics() {
        final int clientNum = nodeIdToStats.size();
        // Transform the mutable object internally used for accounting into the computed version
        Map<String, ComputedNodeStats> nodeStats = Maps.newMapWithExpectedSize(nodeIdToStats.size());
        nodeIdToStats.forEach((k, v) -> { nodeStats.put(k, new ComputedNodeStats(clientNum, v)); });
        return nodeStats;
    }

    public AdaptiveSelectionStats getAdaptiveStats(Map<String, Long> clientSearchConnections) {
        return new AdaptiveSelectionStats(clientSearchConnections, getAllNodeStatistics());
    }

    /**
     * Optionally return a {@code NodeStatistics} for the given nodeid, if
     * response information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     */
    public Optional<ComputedNodeStats> getNodeStatistics(final String nodeId) {
        final int clientNum = nodeIdToStats.size();
        return Optional.ofNullable(nodeIdToStats.get(nodeId)).map(ns -> new ComputedNodeStats(clientNum, ns));
    }

    /**
     * Struct-like class encapsulating a point-in-time snapshot of a particular
     * node's statistics. This includes the EWMA of queue size, response time,
     * and service time.
     */
    public static class ComputedNodeStats implements Writeable {
        // We store timestamps with nanosecond precision, however, the
        // formula specifies milliseconds, therefore we need to convert
        // the values so the times don't unduely weight the formula
        private final double FACTOR = 1000000.0;
        private final int clientNum;

        private double cachedRank = 0;

        public final String nodeId;
        public final int queueSize;
        public final double responseTime;
        public final double serviceTime;
        /**
         * Number of EWMA observations that have been folded into this coordinator's view of the
         * node. Read by {@code IndexShardRoutingTable#rankNodes} to decide whether the node is
         * warm enough to anchor warming-up peers against, or still in its post-probe warmup
         * window. Stored as {@code long} so the unbounded counter cannot overflow in practice
         * (would take centuries even at extreme request rates). Not serialized — only meaningful
         * on the coordinator that produced this snapshot, so wire-deserialized instances default
         * to {@code 0} (which the routing caller treats as "still warming up").
         */
        public final long observationCount;

        public ComputedNodeStats(String nodeId, int clientNum, int queueSize, double responseTime, double serviceTime) {
            this(nodeId, clientNum, queueSize, responseTime, serviceTime, 0L);
        }

        public ComputedNodeStats(
            String nodeId,
            int clientNum,
            int queueSize,
            double responseTime,
            double serviceTime,
            long observationCount
        ) {
            this.nodeId = nodeId;
            this.clientNum = clientNum;
            this.queueSize = queueSize;
            this.responseTime = responseTime;
            this.serviceTime = serviceTime;
            this.observationCount = observationCount;
        }

        ComputedNodeStats(int clientNum, NodeStatistics nodeStats) {
            this(
                nodeStats.nodeId,
                clientNum,
                (int) nodeStats.queueSize.getAverage(),
                nodeStats.responseTime.getAverage(),
                nodeStats.serviceTime,
                nodeStats.observationCount
            );
        }

        ComputedNodeStats(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.clientNum = in.readInt();
            this.queueSize = in.readInt();
            this.responseTime = in.readDouble();
            this.serviceTime = in.readDouble();
            this.observationCount = 0L;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.nodeId);
            out.writeInt(this.clientNum);
            out.writeInt(this.queueSize);
            out.writeDouble(this.responseTime);
            out.writeDouble(this.serviceTime);
        }

        /**
         * Rank this copy of the data, according to the adaptive replica selection formula from the C3 paper
         * https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf
         */
        private double innerRank(long outstandingRequests) {
            // the concurrency compensation is defined as the number of
            // outstanding requests from the client to the node times the number
            // of clients in the system
            double concurrencyCompensation = outstandingRequests * clientNum;

            // Cubic queue adjustment factor. The paper chose 3 though we could
            // potentially make this configurable if desired.
            int queueAdjustmentFactor = 3;

            // EWMA of queue size
            double qBar = queueSize;
            double qHatS = 1 + concurrencyCompensation + qBar;

            // EWMA of response time
            double rS = responseTime / FACTOR;
            // EWMA of service time. We match the paper's notation, which
            // defines service time as the inverse of service rate (muBarS).
            double muBarSInverse = serviceTime / FACTOR;

            double innerRank = Math.pow(qHatS, queueAdjustmentFactor) * muBarSInverse;
            // When the feature flag is enabled, the rS - muBarSInverse term is dropped.
            if (ARS_FORMULA_ADJUSTMENT_FEATURE_FLAG.isEnabled() == false) {
                innerRank += rS - muBarSInverse;
            }
            return innerRank;
        }

        public double rank(long outstandingRequests) {
            if (cachedRank == 0) {
                cachedRank = innerRank(outstandingRequests);
            }
            return cachedRank;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ComputedNodeStats[");
            sb.append(nodeId).append("](");
            sb.append("nodes: ").append(clientNum);
            sb.append(", queue: ").append(queueSize);
            sb.append(", response time: ").append(String.format(Locale.ROOT, "%.1f", responseTime));
            sb.append(", service time: ").append(String.format(Locale.ROOT, "%.1f", serviceTime));
            sb.append(", rank: ").append(String.format(Locale.ROOT, "%.1f", rank(1)));
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Class encapsulating a node's exponentially weighted queue size, response
     * time, and service time, however, this class is private and intended only
     * to be used for the internal accounting of {@code ResponseCollectorService}.
     */
    private static class NodeStatistics {
        final String nodeId;
        final ExponentiallyWeightedMovingAverage queueSize;
        final ExponentiallyWeightedMovingAverage responseTime;
        double serviceTime;
        long observationCount;

        NodeStatistics(
            String nodeId,
            ExponentiallyWeightedMovingAverage queueSizeEWMA,
            ExponentiallyWeightedMovingAverage responseTimeEWMA,
            double serviceTimeEWMA
        ) {
            this.nodeId = nodeId;
            this.queueSize = queueSizeEWMA;
            this.responseTime = responseTimeEWMA;
            this.serviceTime = serviceTimeEWMA;
            this.observationCount = 1L;
        }
    }
}
