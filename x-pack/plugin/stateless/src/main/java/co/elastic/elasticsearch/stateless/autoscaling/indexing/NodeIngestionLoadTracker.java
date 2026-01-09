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
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.RawAndAdjustedNodeIngestLoadSnapshots;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestionLoad.NodeIngestionLoad;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

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

    /**
     * The master tracks a tier-wide average task execution time for the write threadpools. When this setting is enabled, this
     * tier-wide value is used to re-estimate the queued work for the WRITE threadpool. Each node reports a stable average
     * task execution time (if it has one) along with its ingestion load. The reported value is for all the different write threadpools,
     * although we only use the one for WRITE. A node considers its latest averageTaskExecutionEWMA in the executor stats stable if:
     * <p>
     * {@link IngestLoadProbe#INITIAL_INTERVAL_TO_CONSIDER_NODE_AVG_TASK_EXEC_TIME_UNSTABLE} has passed since the node's first
     * shard assignment (essentially since the node has started, but to make this more accurate).
     * <p>
     * If a scaling event is in progress (based on existence of shutdown markers),
     * {@link #INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE} has passed from the start of the scaling event.
     * <p>
     * Both of these are by default set to 5 minutes. The idea is to NOT rely on the actual averageTaskExecutionEWMA within the first
     * 5 min for a new node, and within the first 5 min of a scaling event for the entire tier. There are cases where these two overlap.
     *
     * The reason is that during these periods there are often very high/unpredictable jumps in the average task execution time for
     * the WRITE threadpool which causes very high estimates for the queued work. We've seen this jump for both new nodes and existing
     * nodes during scaling.
     */
    public static final Setting<Boolean> USE_TIER_WIDE_AVG_TASK_EXEC_TIME_DURING_SCALING = Setting.boolSetting(
        "serverless.autoscaling.ingest_metrics.use_tier_wide_avg_task_exec_time_during_scaling",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE = Setting.timeSetting(
        "serverless.autoscaling.ingest_metrics.initial_scaling_window_to_consider_avg_task_exec_times_unstable",
        TimeValue.timeValueMinutes(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(NodeIngestionLoadTracker.class);
    public static final String TIER_WIDE_WRITE_THREADPOOL_AVG_TASK_EXEC_TIME_METRIC_NAME =
        "es.autoscaling.indexing.tier.write.average_task_execution_time.current";

    private volatile TimeValue accurateLoadWindow;
    private volatile TimeValue staleLoadWindow;
    // The NodeIngestionLoadTracker needs these settings to be able to re-calculate ingestion loads
    // TODO: refactor to shared this with IngestLoadProbe
    private volatile TimeValue maxTimeToClearQueue;
    private volatile TimeValue maxManageableQueuedWork;
    private volatile Float maxQueueContributionFactor;
    private volatile boolean includeWriteCoordinationExecutors;
    private volatile boolean useTierWideAvgTaskExecTimeDuringScaling;
    private volatile TimeValue initialScalingWindowToConsiderAvgTaskExecTimesUnstable;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final Map<String, Entry> nodesIngestLoad = ConcurrentCollections.newConcurrentMap();
    // For now, we track average task execution times per node to use only for the WRITE threadpool.
    private final Map<String, Double> writeThreadpoolAvgTaskExecTimePerNode = ConcurrentCollections.newConcurrentMap();
    private volatile OptionalLong lastScalingWindowStartTimeNanos = OptionalLong.empty();
    private final AtomicReference<Double> tierWideWriteThreadpoolAvgTaskExecTimeMetricRef = new AtomicReference<>(null);

    public NodeIngestionLoadTracker(
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeInNanosSupplier,
        MeterRegistry meterRegistry
    ) {
        clusterSettings.initializeAndWatch(ACCURATE_LOAD_WINDOW, value -> this.accurateLoadWindow = value);
        clusterSettings.initializeAndWatch(STALE_LOAD_WINDOW, value -> this.staleLoadWindow = value);
        clusterSettings.initializeAndWatch(IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE, value -> this.maxTimeToClearQueue = value);
        clusterSettings.initializeAndWatch(IngestLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR, value -> this.maxQueueContributionFactor = value);
        clusterSettings.initializeAndWatch(
            IngestLoadProbe.INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED,
            value -> this.includeWriteCoordinationExecutors = value
        );
        clusterSettings.initializeAndWatch(IngestLoadProbe.MAX_MANAGEABLE_QUEUED_WORK, value -> this.maxManageableQueuedWork = value);
        clusterSettings.initializeAndWatch(
            USE_TIER_WIDE_AVG_TASK_EXEC_TIME_DURING_SCALING,
            value -> this.useTierWideAvgTaskExecTimeDuringScaling = value
        );
        clusterSettings.initializeAndWatch(
            INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE,
            value -> this.initialScalingWindowToConsiderAvgTaskExecTimesUnstable = value
        );
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        meterRegistry.registerDoublesGauge(
            TIER_WIDE_WRITE_THREADPOOL_AVG_TASK_EXEC_TIME_METRIC_NAME,
            "The tier-wide average task execution time for the WRITE threadpool across all indexing nodes",
            "nanoseconds",
            () -> {
                Double currentValue = tierWideWriteThreadpoolAvgTaskExecTimeMetricRef.get();
                return currentValue == null ? List.of() : List.of(new DoubleWithAttributes(currentValue));
            }
        );
    }

    Map<String, Entry> getCurrentNodesIngestLoad() {
        return nodesIngestLoad;
    }

    public void currentNodeNoLongerMaster() {
        nodesIngestLoad.clear();
        lastScalingWindowStartTimeNanos = OptionalLong.empty();
        writeThreadpoolAvgTaskExecTimePerNode.clear();
        tierWideWriteThreadpoolAvgTaskExecTimeMetricRef.set(null);
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
                writeThreadpoolAvgTaskExecTimePerNode.remove(removedNodeId);
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

    public void trackNodeIngestLoad(ClusterState state, String nodeId, String nodeName, long metricSeqNo, NodeIngestionLoad newIngestLoad) {
        // Drop a (delayed) metric publication from a planned removal that finished successfully (i.e. left no unassigned shards
        // behind). However, if the metric arrives after the node is gone and there is no shutdown metadata, we're treating it as we
        // do for nodes that disappear w/o any shutdown marker, i.e., we assume this is a node that temporarily dropped
        // out (or very recently joined) and to be safe, we keep reporting its ingestion load with a MINIMUM quality. The recorded
        // ingestion load gets removed once there are no unassigned entries from this node.
        if (successfulPlannedNodeRemoval(state, nodeId)) {
            logger.debug("dropping ingestion load metric received from removed node {} which left no shards unassigned", nodeId);
            return;
        }

        Double writeThreadPoolLastStableAvgTaskExecTime = newIngestLoad.lastStableAvgTaskExecutionTimes().get(ThreadPool.Names.WRITE);
        if (writeThreadPoolLastStableAvgTaskExecTime != null) {
            // We record even 0.0 since that at least signals that the node should be considered stable now, i.e.,
            // it is past its initial minutes.
            writeThreadpoolAvgTaskExecTimePerNode.put(nodeId, writeThreadPoolLastStableAvgTaskExecTime);
        }

        var nodeIngestStats = nodesIngestLoad.computeIfAbsent(nodeId, unused -> new Entry(nodeId, nodeName));
        // We track ingestion loads from nodes that left unassigned shards with a MINIMUM quality, to avoid scale down.
        var quality = state.nodes().get(nodeId) != null ? MetricQuality.EXACT : MetricQuality.MINIMUM;
        nodeIngestStats.setLatestReadingTo(newIngestLoad, metricSeqNo, quality);
    }

    // Visible for testing
    Optional<Double> getTierWideAverageWriteThreadpoolTaskExecutionTime() {
        var nonZeros = writeThreadpoolAvgTaskExecTimePerNode.values().stream().filter(v -> v > 0.0).toList();
        double sum = 0.0;
        for (double value : nonZeros) {
            sum += value;
        }
        if (sum == 0.0) {
            return Optional.empty();
        }
        return Optional.of(sum / nonZeros.size());
    }

    /**
     * Returns the current ingestion load snapshots for all nodes being tracked. If we adjust the ingestion load using tier-wide
     * write average task execution time, it returns the adjusted values as well.
     */
    public RawAndAdjustedNodeIngestLoadSnapshots getIngestLoadSnapshots(ClusterState clusterState) {
        final var nodeLoadIterator = nodesIngestLoad.entrySet().iterator();
        final List<NodeIngestLoadSnapshot> rawIngestLoads = new ArrayList<>(nodesIngestLoad.size());
        final List<NodeIngestLoadSnapshot> adjustedIngestLoads = new ArrayList<>();
        final var initialScalingWindow = withinInitialScalingWindow();
        Optional<Double> tierWriteAvgTaskExecTime = getTierWideAverageWriteThreadpoolTaskExecutionTime();
        tierWideWriteThreadpoolAvgTaskExecTimeMetricRef.set(tierWriteAvgTaskExecTime.orElse(null));
        while (nodeLoadIterator.hasNext()) {
            final var nodeIngestStatsEntry = nodeLoadIterator.next();
            final var nodeIngestLoad = nodeIngestStatsEntry.getValue();
            if (shouldRemoveIngestLoadEntry(clusterState, nodeIngestStatsEntry.getKey(), nodeIngestLoad)) {
                nodeLoadIterator.remove();
                writeThreadpoolAvgTaskExecTimePerNode.remove(nodeIngestLoad.nodeId);
            } else {
                if (nodeIngestLoad.isWithinAccurateWindow() == false) {
                    logger.warn(
                        "reported node ingest load is older than {} seconds (accurate_load_window) for node ID [{}}]",
                        accurateLoadWindow.getSeconds(),
                        nodeIngestStatsEntry.getKey()
                    );
                }
                final var rawIngestLoad = nodeIngestLoad.getIngestLoadSnapshot();
                rawIngestLoads.add(rawIngestLoad);
                // If available, use the min of tier-wide average and last stable per-node average task execution times
                Optional<Double> lowerAlternativeAvgTaskExecTime = tierWriteAvgTaskExecTime;
                Double lastStableAvgTaskExecTime = writeThreadpoolAvgTaskExecTimePerNode.get(nodeIngestLoad.nodeId);
                if (lastStableAvgTaskExecTime != null
                    && lastStableAvgTaskExecTime > 0.0
                    && (lowerAlternativeAvgTaskExecTime.isEmpty() || lowerAlternativeAvgTaskExecTime.get() > lastStableAvgTaskExecTime)) {
                    lowerAlternativeAvgTaskExecTime = Optional.of(lastStableAvgTaskExecTime);
                }
                // If we are within {@link INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE} of scaling, or
                // this is an ingestion load for a recently added node, use the tier-wide average for all indexing nodes if
                // tier wide value is available.
                // As long as the node reports a stable avg task exec time, we consider it not new.
                boolean nodeIsNew = writeThreadpoolAvgTaskExecTimePerNode.get(nodeIngestLoad.nodeId) == null;
                if (useTierWideAvgTaskExecTimeDuringScaling  // enabled
                    && (initialScalingWindow || nodeIsNew)  // within a scaling event
                    && nodeIngestLoad.queueContributionFromWriteThreadPool() > 0.0  // there is queueing to re-estimate
                    && lowerAlternativeAvgTaskExecTime.isPresent()  // We have an alternative value to use
                ) {
                    final var adjustedIngestLoad = getIngestLoadSnapshotUsingTierAvgTaskExecTimeForWrite(
                        nodeIngestLoad,
                        lowerAlternativeAvgTaskExecTime.get()
                    );
                    // Use it if it's less
                    if (adjustedIngestLoad.load() < rawIngestLoad.load()) {
                        adjustedIngestLoads.add(adjustedIngestLoad);
                        if (logger.isDebugEnabled()) {
                            final var reason = nodeIsNew
                                ? "node is recently added and has not reported a stable average task execution time"
                                : Strings.format(
                                    "index tier is within a scaling window (%s = %s)",
                                    INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE.getKey(),
                                    initialScalingWindowToConsiderAvgTaskExecTimesUnstable
                                );
                            final var exeTimeMillis = TimeValue.nsecToMSec(tierWriteAvgTaskExecTime.get().longValue());
                            logger.debug(
                                "re-estimated ingestion load for node [{}] "
                                    + "using average WRITE task execution time of {}ms "
                                    + "because [{}] (original reported load: {}, adjusted load: {})",
                                nodeIngestLoad.nodeName,
                                exeTimeMillis,
                                reason,
                                nodeIngestLoad,
                                adjustedIngestLoad
                            );
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug(
                                "not adjusting ingestion load for node [{}] because "
                                    + "adjusted load {} is not less than original reported load {}",
                                nodeIngestLoad.nodeName,
                                adjustedIngestLoad,
                                rawIngestLoad
                            );
                        }
                    }
                }
            }
        }
        if (adjustedIngestLoads.isEmpty() == false) {
            // add the non-adjusted values as well
            final var adjustedIds = adjustedIngestLoads.stream().map(NodeIngestLoadSnapshot::nodeId).collect(Collectors.toSet());
            rawIngestLoads.forEach(rawIngestLoad -> {
                if (adjustedIds.contains(rawIngestLoad.nodeId()) == false) {
                    adjustedIngestLoads.add(rawIngestLoad);
                }
            });
        }
        return new RawAndAdjustedNodeIngestLoadSnapshots(rawIngestLoads, adjustedIngestLoads.isEmpty() ? null : adjustedIngestLoads);
    }

    private NodeIngestLoadSnapshot getIngestLoadSnapshotUsingTierAvgTaskExecTimeForWrite(
        Entry nodeIngestLoad,
        double tierWideAvgTaskExecTime
    ) {
        // recalculate the total load by replacing the WRITE threadpools ingestion load with one
        // calculated using the tier-wide average task execution time
        var executorIngestionLoads = nodeIngestLoad.ingestLoad.executorIngestionLoads();
        var writeExecutorStats = nodeIngestLoad.ingestLoad.executorStats().get(ThreadPool.Names.WRITE);
        var writeExecutorIngestionLoad = IngestLoadProbe.calculateIngestionLoadForExecutor(
            ThreadPool.Names.WRITE,
            writeExecutorStats.averageLoad(),
            tierWideAvgTaskExecTime,
            writeExecutorStats.averageQueueSize(),
            writeExecutorStats.maxThreads(),
            maxTimeToClearQueue,
            maxManageableQueuedWork,
            maxQueueContributionFactor * writeExecutorStats.maxThreads()
        );
        var total = 0.0;
        for (var executor : AverageWriteLoadSampler.WRITE_EXECUTORS) {
            total += switch (executor) {
                case ThreadPool.Names.WRITE -> writeExecutorIngestionLoad.total();
                case ThreadPool.Names.WRITE_COORDINATION, ThreadPool.Names.SYSTEM_WRITE_COORDINATION -> includeWriteCoordinationExecutors
                    ? executorIngestionLoads.get(executor).total()
                    : 0.0;
                default -> executorIngestionLoads.get(executor).total();
            };
        }
        return new NodeIngestLoadSnapshot(nodeIngestLoad.nodeId, nodeIngestLoad.nodeName, total, MetricQuality.MINIMUM);
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

    public void scalingEventStarted() {
        lastScalingWindowStartTimeNanos = OptionalLong.of(relativeTimeInNanos());
    }

    /**
     * Whether we are within the first {@link #initialScalingWindowToConsiderAvgTaskExecTimesUnstable} of a scaling event.
     */
    boolean withinInitialScalingWindow() {
        final var settingIntervalNanos = initialScalingWindowToConsiderAvgTaskExecTimesUnstable.nanos();
        if (lastScalingWindowStartTimeNanos.isEmpty() || settingIntervalNanos == 0) {
            return false;
        }
        return relativeTimeInNanos() - lastScalingWindowStartTimeNanos.getAsLong() < settingIntervalNanos;
    }

    // Package-private for testing
    class Entry {
        private final String nodeId;
        private final String nodeName;
        private NodeIngestionLoad ingestLoad;
        private long latestSampleTimeInNanos = relativeTimeInNanos();
        private long maxSeqNo = Long.MIN_VALUE;
        private MetricQuality quality = MetricQuality.MISSING;

        Entry(String nodeId, String nodeName) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.ingestLoad = NodeIngestionLoad.EMPTY;
        }

        synchronized void setLatestReadingTo(NodeIngestionLoad ingestLoad, long metricSeqNo, MetricQuality quality) {
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
        NodeIngestionLoad getIngestLoad() {
            return ingestLoad;
        }

        public double queueContributionFromWriteThreadPool() {
            final var writeExecutorIngestionLoad = ingestLoad.executorIngestionLoads().get(ThreadPool.Names.WRITE);
            return writeExecutorIngestionLoad == null ? 0.0 : writeExecutorIngestionLoad.queueThreadsNeeded();
        }
    }
}
