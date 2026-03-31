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

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.ShardHeapUsageEstimates;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.xpack.stateless.MetricQuality;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.elasticsearch.xpack.stateless.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;

/**
 * Core memory metrics tracking service for stateless clusters. Tracks per-shard memory metrics
 * and provides per-node and per-shard heap usage estimates. This class lives in the self-managed
 * stateless module (primarily to support the heap-based allocation via the StatelessHeapUsageCollector) and is used by both self-managed
 * and serverless stateless deployments.
 */
public class StatelessMemoryMetricsService implements ClusterStateListener {

    public static final TimeValue DEFAULT_INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY = TimeValue.timeValueMinutes(2);
    public static final Setting<TimeValue> INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING = Setting.timeSetting(
        "serverless.autoscaling.memory_metrics.indexing_operations_memory_requirements.validity",
        // Ensure that we give enough time to the controller to react to memory requirements for rejected operations
        // but at the same time avoid scaling up for too long due to a single operation that was rejected.
        DEFAULT_INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Boolean> INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING = Setting.boolSetting(
        "serverless.autoscaling.memory_metrics.indexing_operations_memory_requirements.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    // For the adaptive method, default to add an additional overhead of 50% of the estimate
    public static final Setting<RatioValue> ADAPTIVE_EXTRA_OVERHEAD_SETTING = new Setting<>(
        "serverless.autoscaling.memory_metrics.adaptive_extra_overhead",
        "50%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * This feature enables the calculation of a minimum threshold for adaptive shard memory estimation,
     * derived from StatelessMemoryMetricsService#MAX_HEAP_SIZE and ShardLimitValidator#SETTING_CLUSTER_MAX_SHARDS_PER_NODE.
     * As the shard count increases, this threshold drives automatic cluster sizing to accommodate the additional memory requirements.
     */
    public static final Setting<Boolean> ADAPTIVE_SHARD_MEMORY_ESTIMATION_MIN_THRESHOLD_ENABLED_SETTING = Setting.boolSetting(
        "serverless.autoscaling.memory_metrics.adaptive_min_threshold.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Two methods to estimate the memory usage of IndexShard instances:
     * 1. Fixed Method: Assigns a default value of 6MB per shard. This method may overestimate memory usage for shards with minimal data
     * and underestimate it for shards with many segments and fields.
     * 2. Adaptive Method: Estimates memory usage based on the actual number of segments and fields in segments. While generally more
     * accurate, this method can still occasionally overestimate or underestimate memory usage.
     * <p>
     * By default, the Fixed Method is used. To switch to the Adaptive Method, explicitly set
     * the `serverless.autoscaling.memory_metrics.shard_memory_overhead` setting to -1.
     * <p>
     */
    public static final ByteSizeValue FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT = ByteSizeValue.ofMb(6);
    public static final Setting<ByteSizeValue> FIXED_SHARD_MEMORY_OVERHEAD_SETTING = Setting.byteSizeSetting(
        "serverless.autoscaling.memory_metrics.shard_memory_overhead",
        FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    // Enables the use of custom shard memory overhead values reported by nodes when calculating memory usage estimates.
    public static final Setting<Boolean> SELF_REPORTED_SHARD_MEMORY_OVERHEAD_ENABLED_SETTING = Setting.boolSetting(
        "serverless.autoscaling.memory_metrics.self_reported_shard_memory_overhead.enabled",
        false,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> MERGE_MEMORY_ESTIMATE_ENABLED_SETTING = Setting.boolSetting(
        "serverless.autoscaling.memory_metrics.merge_memory_estimate.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // We clean up a node's reported max merge when the node leaves the cluster. In order to ensure a node's reported max merges are
    // consumed in the correct sequence, we rely on keeping a sequence no of the last reported value to be able to reject out of order
    // publications for that node. Instead of removing the last known publication immediately when a node leaves the cluster, we wait for
    // the interval specified below and remove it if the node doesn't come back.
    public static final TimeValue DEFAULT_REMOVED_NODE_MERGE_MEMORY_ESTIMATION_VALIDITY = TimeValue.timeValueMinutes(1);

    // Based on {@code org.elasticsearch.server.cli.MachineDependentHeap}
    public static final long MAX_HEAP_SIZE = ByteSizeUnit.GB.toBytes(31);

    private volatile ByteSizeValue fixedShardMemoryOverhead;
    private final boolean selfReportedShardMemoryOverheadEnabled;
    // The memory overhead of each IndexShard instance used in the adaptive estimate
    public static final ByteSizeValue ADAPTIVE_SHARD_MEMORY_OVERHEAD = ByteSizeValue.ofKb(75);
    // The memory overhead of each Lucene segment, including maps for postings, doc_values, and stored_fields producers
    public static final ByteSizeValue ADAPTIVE_SEGMENT_MEMORY_OVERHEAD = ByteSizeValue.ofKb(55);
    // The memory overhead of each field found in Lucene segments
    public static final ByteSizeValue ADAPTIVE_FIELD_MEMORY_OVERHEAD = ByteSizeValue.ofBytes(1024);

    private static final Logger logger = LogManager.getLogger(StatelessMemoryMetricsService.class);
    // visible for testing
    public static final long INDEX_MEMORY_OVERHEAD = ByteSizeValue.ofKb(350).getBytes();

    /**
     * See:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/size-your-shards.html#_consider_additional_heap_overheads
     * for the origin of this.
     */
    public static final long WORKLOAD_MEMORY_OVERHEAD = ByteSizeValue.ofMb(500).getBytes();
    // Mutable copy used by setWorkloadMemoryOverheadOverrideForTesting; equal to WORKLOAD_MEMORY_OVERHEAD at runtime.
    private volatile long workloadMemoryOverhead = WORKLOAD_MEMORY_OVERHEAD;

    private volatile boolean initialized = false;
    private final Map<ShardId, ShardMemoryMetrics> shardMemoryMetrics = new ConcurrentHashMap<>();
    private volatile int totalIndices;
    private final AtomicReference<IndexingOperationsMemoryRequirements> indexingOperationsHeapMemoryRequirementsRef =
        new AtomicReference<>();

    private final LongSupplier relativeTimeInNanosSupplier;
    private volatile boolean indexingOperationsMemoryMetricsEnabled;
    private volatile TimeValue indexingOperationsMemoryMetricsValidityDuration;

    /** Whenever {@link #getPerNodeMemoryMetrics} is called, this value is updated. */
    private volatile long lastMaxTotalPostingsInMemoryBytes = 0;

    private volatile long clusterStateVersion = ClusterState.UNKNOWN_VERSION;
    private volatile boolean mergeMemoryEstimateEnabled;
    private volatile double adaptiveExtraOverheadRatio;
    private final Map<String, ShardMergeMemoryEstimatePublication> maxShardMergeMemoryEstimatePerNode = new ConcurrentHashMap<>();
    /** Populated with the per-node heap estimate results from the last {@link #getPerNodeMemoryMetrics} call, consumed when the gauge is
     * read. Metric depends on that method being called regularly. */
    private final AtomicReference<List<NodeHeapEstimateSnapshot>> lastPerNodeHeapSnapshots = new AtomicReference<>(List.of());
    protected volatile int shardLimitPerNode;
    protected volatile boolean adaptiveShardMemoryEstimationMinThresholdEnabled;

    @SuppressWarnings("this-escape")
    public StatelessMemoryMetricsService(LongSupplier relativeTimeInNanosSupplier, ClusterSettings clusterSettings) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.selfReportedShardMemoryOverheadEnabled = clusterSettings.get(SELF_REPORTED_SHARD_MEMORY_OVERHEAD_ENABLED_SETTING);
        clusterSettings.initializeAndWatch(
            INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING,
            value -> indexingOperationsMemoryMetricsEnabled = value
        );
        clusterSettings.initializeAndWatch(
            INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING,
            value -> indexingOperationsMemoryMetricsValidityDuration = value
        );

        clusterSettings.initializeAndWatch(FIXED_SHARD_MEMORY_OVERHEAD_SETTING, value -> fixedShardMemoryOverhead = value);
        clusterSettings.initializeAndWatch(MERGE_MEMORY_ESTIMATE_ENABLED_SETTING, value -> mergeMemoryEstimateEnabled = value);
        clusterSettings.initializeAndWatch(ADAPTIVE_EXTRA_OVERHEAD_SETTING, value -> adaptiveExtraOverheadRatio = value.getAsRatio());
        clusterSettings.initializeAndWatch(SETTING_CLUSTER_MAX_SHARDS_PER_NODE, value -> shardLimitPerNode = value);
        clusterSettings.initializeAndWatch(
            ADAPTIVE_SHARD_MEMORY_ESTIMATION_MIN_THRESHOLD_ENABLED_SETTING,
            value -> adaptiveShardMemoryEstimationMinThresholdEnabled = value
        );
    }

    /**
     * Estimate the heap usage for each indexing node in the cluster
     * <ul>
     *  <li>Nodes are attributed the heap usage of all shards currently allocated to them in the
     *      {@link org.elasticsearch.cluster.routing.RoutingNodes}</li>
     *  <li>Relocating shards are accounted for on the source and target node</li>
     *  <li>Any shard missing {@link ShardMemoryMetrics} is given a default (non-zero) estimate</li>
     * </ul>
     *
     * @param clusterState The cluster state used to determine which shard is where
     * @return A map of node id to heap usage estimate in bytes
     */
    public Map<String, Long> getPerNodeMemoryMetrics(ClusterState clusterState) {
        final DiscoveryNodes discoveryNodes = clusterState.nodes();
        final long nodeBaseHeapEstimateInBytes = getNodeBaseHeapEstimateInBytes();
        final long mergeMemoryEstimate = mergeMemoryEstimation();
        final long minimumRequiredHeapForHandlingLargeIndexingOps = minimumRequiredHeapForAcceptingLargeIndexingOps();
        final Map<String, EstimatedHeapUsageBuilder> heapUsageBuilders = new HashMap<>();
        final long nowNanos = relativeTimeInNanos();
        final ShardMemoryMetrics defaultEstimate = newUninitialisedShardMemoryMetrics(nowNanos);
        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            final String nodeId = routingNode.nodeId();
            final DiscoveryNode discoveryNode = discoveryNodes.get(nodeId);
            assert discoveryNode != null : "The routing nodes is from the cluster state so DiscoveryNodes should be consistent";
            // We only provide estimates for indexing nodes
            if (discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
                continue;
            }
            final EstimatedHeapUsageBuilder builderForNode = new EstimatedHeapUsageBuilder(
                nodeBaseHeapEstimateInBytes,
                minimumRequiredHeapForHandlingLargeIndexingOps,
                mergeMemoryEstimate
            );
            for (ShardRouting shard : routingNode) {
                // Only include active shards in our node-level estimates, the simulator will complete
                // any ongoing recoveries, which adds the heap consumption to node and deducts it from
                // the source if it's relocating
                if (shard.active() == false) {
                    continue;
                }
                final ShardId shardId = shard.shardId();
                builderForNode.add(shardId, shardMemoryMetrics.getOrDefault(shardId, defaultEstimate));
            }
            heapUsageBuilders.put(nodeId, builderForNode);
        }
        // Take the max postings memory across all nodes and apply that to all nodes.
        final long maxTotalPostingsInMemoryBytes = heapUsageBuilders.values()
            .stream()
            .mapToLong(builder -> builder.totalPostingsInMemoryBytes)
            .max()
            .orElse(0L);
        lastMaxTotalPostingsInMemoryBytes = maxTotalPostingsInMemoryBytes; // Tracked for testing purposes
        heapUsageBuilders.values().forEach(builder -> builder.totalPostingsInMemoryBytes = maxTotalPostingsInMemoryBytes);
        final Map<String, Long> nodeIdToHeapUsage = Maps.transformValues(
            heapUsageBuilders,
            EstimatedHeapUsageBuilder::getHeapUsageEstimate
        );
        lastPerNodeHeapSnapshots.set(nodeIdToHeapUsage.entrySet().stream().filter(e -> discoveryNodes.get(e.getKey()) != null).map(e -> {
            final DiscoveryNode node = discoveryNodes.get(e.getKey());
            return new NodeHeapEstimateSnapshot(e.getKey(), node.getName(), e.getValue());
        }).toList());
        return nodeIdToHeapUsage;
    }

    public long getNodeBaseHeapEstimateInBytes() {
        return INDEX_MEMORY_OVERHEAD * totalIndices + workloadMemoryOverhead;
    }

    // Visible for testing
    public void setWorkloadMemoryOverheadOverrideForTesting(long val) {
        workloadMemoryOverhead = val;
    }

    public long minimumRequiredHeapForAcceptingLargeIndexingOps() {
        if (indexingOperationsMemoryMetricsEnabled == false) {
            return 0;
        }
        var indexingHeapMemoryRequirements = indexingOperationsHeapMemoryRequirementsRef.get();
        if (indexingHeapMemoryRequirements == null || indexingHeapMemoryRequirements.validUntil() < relativeTimeInNanos()) {
            // Try to clear the memory once the results are stale
            indexingOperationsHeapMemoryRequirementsRef.compareAndSet(indexingHeapMemoryRequirements, null);
            return 0;
        }
        return indexingHeapMemoryRequirements.minimumRequiredHeapInBytes();
    }

    public long mergeMemoryEstimation() {
        if (mergeMemoryEstimateEnabled == false) {
            return 0;
        }
        // Clean up
        var publicationIterator = maxShardMergeMemoryEstimatePerNode.entrySet().iterator();
        var validityNanos = DEFAULT_REMOVED_NODE_MERGE_MEMORY_ESTIMATION_VALIDITY.nanos();
        long now = relativeTimeInNanos();
        while (publicationIterator.hasNext()) {
            var publication = publicationIterator.next();
            if (publication.getValue().hasNodeLeft()) {
                var nodeLeftTimestamp = publication.getValue().nodeLeftTimestampNanos();
                if (now >= nodeLeftTimestamp + validityNanos) {
                    publicationIterator.remove();
                }
            }
        }
        return maxShardMergeMemoryEstimatePerNode.values().stream().mapToLong(e -> e.estimate.estimateInBytes()).max().orElse(0L);
    }

    // Accessible for testing.
    ByteSizeValue getFixedShardMemoryOverhead() {
        return fixedShardMemoryOverhead;
    }

    // Accessible for testing.
    long getLastMaxTotalPostingsInMemoryBytes() {
        return lastMaxTotalPostingsInMemoryBytes;
    }

    public boolean isSelfReportedShardMemoryOverheadEnabled() {
        return selfReportedShardMemoryOverheadEnabled;
    }

    private record NodeHeapEstimateSnapshot(String nodeId, String nodeName, long heapBytes) {}

    /**
     * Estimates the heap usage for a single shard, based on: segment, number of fields and live doc byte counts.
     */
    public long estimateShardMemoryUsageInBytes(ShardMemoryMetrics metrics) {
        final var fixedShardOverhead = this.fixedShardMemoryOverhead;
        if (fixedShardOverhead.getBytes() > 0) {
            return fixedShardOverhead.getBytes();
        }
        long estimateBytes = ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + metrics.numSegments * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD.getBytes()
            + metrics.totalFields * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes() + metrics.liveDocsBytes;
        long extraBytes = (long) (estimateBytes * adaptiveExtraOverheadRatio);

        if (this.adaptiveShardMemoryEstimationMinThresholdEnabled) {
            return Math.max(getAdaptiveShardMemoryEstimationMinThreshold(), estimateBytes + extraBytes);
        }

        return estimateBytes + extraBytes;
    }

    public long getAdaptiveShardMemoryEstimationMinThreshold() {
        return (MAX_HEAP_SIZE - getNodeBaseHeapEstimateInBytes()) / this.shardLimitPerNode;
    }

    // visible for testing
    public Map<ShardId, ShardMemoryMetrics> getShardMemoryMetrics() {
        return shardMemoryMetrics;
    }

    public List<LongWithAttributes> getPerNodeHeapAndReset() {
        final List<NodeHeapEstimateSnapshot> snapshot = lastPerNodeHeapSnapshots.getAndSet(List.of());
        return snapshot.stream()
            .map(s -> new LongWithAttributes(s.heapBytes(), Map.of("es_node_id", s.nodeId(), "es_node_name", s.nodeName())))
            .toList();
    }

    /**
     * Apply the received {@link HeapMemoryUsage}
     *
     * NOTE: We may be concurrently applying a cluster state (see comment on {@link #clusterChanged(ClusterChangedEvent)})
     * so we perform the should-retry-on-conflict check before attempting to update any of the metrics. This is so the
     * stale-ness check is conservatively performed using the last cluster state version applied before we start processing
     * the updates.
     *
     * This prevents the following sequence of events:
     * <ol>
     *     <li>Received metrics for cluster state version 5 are applied, conflicts are detected because local state is
     *          representative of version 4 (and a shard primary changed)</li>
     *     <li>Cluster state 5 is applied, version is bumped</li>
     *     <li>Retry check performed, indicates we should silently ignore conflicts because local state version >= update version</li>
     *     <li>Valid updates silently ignored</li>
     * </ol>
     */
    public void updateShardsMappingSize(final HeapMemoryUsage heapMemoryUsage) {
        final boolean shouldRequestRetryOnConflict = shouldRequestRetryForHeapMemoryMetrics(heapMemoryUsage);
        List<ShardId> missedUpdates = new ArrayList<>();
        for (var entry : heapMemoryUsage.shardMappingSizes().entrySet()) {
            ShardId shardId = entry.getKey();
            ShardMappingSize shardMappingSize = entry.getValue();
            boolean applied = false;
            ShardMemoryMetrics shardMemoryMetrics = this.shardMemoryMetrics.get(shardId);
            if (shardMemoryMetrics != null) {
                applied = shardMemoryMetrics.update(
                    shardMappingSize.mappingSizeInBytes(),
                    shardMappingSize.numSegments(),
                    shardMappingSize.totalFields(),
                    shardMappingSize.postingsInMemoryBytes(),
                    shardMappingSize.liveDocsBytes(),
                    shardMappingSize.shardMemoryOverheadBytes(),
                    heapMemoryUsage.publicationSeqNo(),
                    shardMappingSize.nodeId(),
                    relativeTimeInNanos()
                );
            }
            if (applied == false) {
                missedUpdates.add(shardId);
            }
        }
        if (missedUpdates.size() > 0 && shouldRequestRetryOnConflict) {
            throw new AutoscalingMissedIndicesUpdateException(
                "Failed to fully apply " + heapMemoryUsage + " due to missed shards: " + missedUpdates
            );
        }
    }

    public void updateIndexingOperationsHeapMemoryRequirements(long indexingHeapMemoryRequirements) {
        long nowInNanos = relativeTimeInNanosSupplier.getAsLong();
        indexingOperationsHeapMemoryRequirementsRef.accumulateAndGet(
            new IndexingOperationsMemoryRequirements(
                indexingHeapMemoryRequirements,
                nowInNanos + indexingOperationsMemoryMetricsValidityDuration.nanos()
            ),
            (existing, proposed) -> {
                if (existing == null || existing.validUntil() < nowInNanos) {
                    return proposed;
                }
                return existing.compareTo(proposed) >= 0 ? existing : proposed;
            }
        );
    }

    public void updateMergeMemoryEstimate(ShardMergeMemoryEstimatePublication receivedPublication) {
        if (mergeMemoryEstimateEnabled == false) {
            return;
        }
        maxShardMergeMemoryEstimatePerNode.compute(receivedPublication.nodeEphemeralId, (nodeId, lastPublication) -> {
            if (lastPublication == null) {
                return receivedPublication;
            }
            if (receivedPublication.seqNo <= lastPublication.seqNo) {
                return lastPublication;
            }
            // It is possible to receive a publication after the node is gone. Keep the nodeLeftTimestampNanos so if the node doesn't come
            // back we remove its entry
            return receivedPublication.withNodeLeftTimestamp(lastPublication.nodeLeftTimestampNanos);
        });
    }

    /**
     * Update our map of {@link ShardMemoryMetrics} to represent the new cluster state. This includes:
     *
     * <ul>
     *     <li>Creating instances for new shards</li>
     *     <li>Removing instances for deleted shards</li>
     *     <li>Updating {@link ShardMemoryMetrics#getMetricShardNodeId()} to reflect changes in the location of the primary</li>
     *     <li>Setting quality to {@link MetricQuality#MINIMUM} when mapping changes are applied</li>
     *     <li>Populating the whole map on a new master</li>
     *     <li>Clearing the map on a demoted master</li>
     * </ul>
     *
     * We update the {@link #clusterStateVersion} only once the cluster state has been fully processed. This means it always reflects
     * the last cluster state version fully reflected in the state of the map (i.e. not a version that we might be currently processing).
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            shardMemoryMetrics.clear();
            indexingOperationsHeapMemoryRequirementsRef.set(null);
            maxShardMergeMemoryEstimatePerNode.clear();
            initialized = false;
            // Set the cluster state to unknown so we don't ignore valid updates that arrive during our next promotion
            clusterStateVersion = ClusterState.UNKNOWN_VERSION;
            return;
        }
        this.totalIndices = event.state().metadata().getTotalNumberOfIndices();

        // new master use case: no indices exist in internal map
        if (event.nodesDelta().masterNodeChanged() || initialized == false) {
            for (IndexMetadata indexMetadata : event.state().metadata().indicesAllProjects()) {
                for (int id = 0; id < indexMetadata.getNumberOfShards(); id++) {
                    var shardMemoryMetrics = newUninitialisedShardMemoryMetrics(relativeTimeInNanosSupplier.getAsLong());
                    // new master should track the current assigned primary shard node in order to accept updates from such nodes
                    ShardId shardId = new ShardId(indexMetadata.getIndex(), id);
                    ShardRouting shardRouting = event.state().routingTable().shardRoutingTable(shardId).primaryShard();
                    if (shardRouting.assignedToNode()) {
                        shardMemoryMetrics.update(shardRouting.currentNodeId(), relativeTimeInNanos());
                    }
                    this.shardMemoryMetrics.put(shardId, shardMemoryMetrics);
                }
            }
            initialized = true;
            clusterStateVersion = event.state().version();
            return;
        }

        if (event.metadataChanged() || event.routingTableChanged()) {

            // index delete use case
            for (Index deletedIndex : event.indicesDeleted()) {
                int numberOfShards = event.previousState().metadata().indexMetadata(deletedIndex).getNumberOfShards();
                for (int id = 0; id < numberOfShards; id++) {
                    shardMemoryMetrics.remove(new ShardId(deletedIndex, id));
                }
            }

            for (IndexMetadata indexMetadata : event.state().metadata().indicesAllProjects()) {
                final Index index = indexMetadata.getIndex();
                for (int id = 0; id < indexMetadata.getNumberOfShards(); id++) {
                    // index created use case, EXACT values will be sent by index node
                    shardMemoryMetrics.putIfAbsent(
                        new ShardId(index, id),
                        newUninitialisedShardMemoryMetrics(relativeTimeInNanosSupplier.getAsLong())
                    );
                }
                // index mapping update use case
                final IndexMetadata oldIndexMetadata = event.previousState().metadata().findIndex(index).orElse(null);
                final IndexMetadata newIndexMetadata = event.state().metadata().findIndex(index).orElse(null);

                if (oldIndexMetadata != null
                    && newIndexMetadata != null
                    && ClusterChangedEvent.indexMetadataChanged(oldIndexMetadata, newIndexMetadata)) {
                    if (oldIndexMetadata.getMappingVersion() < newIndexMetadata.getMappingVersion()) {
                        // set last known value as MINIMUM, EXACT value will be sent by index node
                        for (int id = 0; id < indexMetadata.getNumberOfShards(); id++) {
                            shardMemoryMetrics.get(new ShardId(index, id)).update(MetricQuality.MINIMUM, relativeTimeInNanos());
                        }
                    }
                }

                // moving shards use case
                if (event.indexRoutingTableChanged(index)) {
                    for (int id = 0; id < indexMetadata.getNumberOfShards(); id++) {
                        ShardId shardId = new ShardId(indexMetadata.getIndex(), id);
                        ShardRouting newShardRouting = event.state().routingTable().shardRoutingTable(shardId).primaryShard();
                        final ShardMemoryMetrics shardMemoryMetrics = this.shardMemoryMetrics.get(shardId);
                        if (newShardRouting.assignedToNode()) {
                            assert shardMemoryMetrics != null;
                            final String lastKnownShardNodeId = shardMemoryMetrics.getMetricShardNodeId();
                            final String newShardNodeId = newShardRouting.currentNodeId();
                            if (newShardNodeId.equals(lastKnownShardNodeId) == false) {
                                // preserve last-known value, since shard has moved and no mapping change happened
                                shardMemoryMetrics.update(newShardRouting.currentNodeId(), relativeTimeInNanos());
                            }
                        } else {
                            shardMemoryMetrics.update(MetricQuality.MINIMUM, relativeTimeInNanos());
                        }
                    }
                }
            }
        }

        if (event.nodesChanged()) {
            for (var node : event.nodesDelta().removedNodes()) {
                maxShardMergeMemoryEstimatePerNode.computeIfPresent(
                    node.getEphemeralId(),
                    (nodeEphemeralId, current) -> current.withNodeLeftTimestamp(relativeTimeInNanos())
                );
            }
            for (var node : event.nodesDelta().addedNodes()) {
                maxShardMergeMemoryEstimatePerNode.computeIfPresent(
                    node.getEphemeralId(),
                    (nodeEphemeralId, current) -> current.withoutNodeLeftTimestamp()
                );
            }
        }

        clusterStateVersion = event.state().version();
    }

    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Returns the current indexing operations heap memory requirements if they are still valid (not expired),
     * or {@code null} if they are expired or not set.
     */
    public IndexingOperationsMemoryRequirements getValidIndexingOperationsHeapMemoryRequirements() {
        var reqs = indexingOperationsHeapMemoryRequirementsRef.get();
        if (reqs == null || reqs.validUntil() < relativeTimeInNanos()) {
            return null;
        }
        return reqs;
    }

    /**
     * Should we request a retry for a {@link HeapMemoryUsage} that failed to fully process?
     *
     * @param heapMemoryUsage The heapMemoryUsage
     * @return true if it may be correctly applied on retry, false otherwise
     */
    private boolean shouldRequestRetryForHeapMemoryMetrics(HeapMemoryUsage heapMemoryUsage) {
        return heapMemoryUsage.clusterStateVersion() == ClusterState.UNKNOWN_VERSION
            || clusterStateVersion == ClusterState.UNKNOWN_VERSION
            || clusterStateVersion < heapMemoryUsage.clusterStateVersion();
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    public static class ShardMemoryMetrics {
        private long mappingSizeInBytes;
        private int numSegments = 0;
        private int totalFields = 0;
        private long postingsInMemoryBytes = 0;
        private long liveDocsBytes = 0;
        private long shardMemoryOverheadBytes;
        private long seqNo;
        private MetricQuality metricQuality;
        private String metricShardNodeId;   // node id which hosts sending primary 0-shard
        private long updateTimestampNanos;

        public ShardMemoryMetrics(
            long mappingSizeInBytes,
            int numSegments,
            int totalFields,
            long postingsInMemoryBytes,
            long liveDocsBytes,
            long shardMemoryOverheadBytes,
            long seqNo,
            MetricQuality metricQuality,
            String metricShardNodeId,
            long updateTimestampNanos
        ) {
            this.mappingSizeInBytes = mappingSizeInBytes;
            this.numSegments = numSegments;
            this.totalFields = totalFields;
            this.postingsInMemoryBytes = postingsInMemoryBytes;
            this.liveDocsBytes = liveDocsBytes;
            this.shardMemoryOverheadBytes = shardMemoryOverheadBytes;
            this.seqNo = seqNo;
            this.metricQuality = metricQuality;
            this.metricShardNodeId = metricShardNodeId;
            this.updateTimestampNanos = updateTimestampNanos;
        }

        public synchronized boolean update(
            long mappingSizeInBytes,
            int numSegments,
            int totalFields,
            long postingsInMemoryBytes,
            long liveDocsBytes,
            long shardMemoryOverheadBytes,
            long seqNo,
            String metricShardNodeId,
            long updateTime
        ) {
            assert metricShardNodeId != null;
            // drop messages which were sent from unknown nodes
            // e.g. message late arrival from previously allocated node (where primary 0-shard used to be)
            if (Objects.equals(this.metricShardNodeId, metricShardNodeId)) {
                if (this.seqNo < seqNo) { // handle reordered messages which were sent from the same node
                    this.seqNo = seqNo;
                    this.mappingSizeInBytes = mappingSizeInBytes;
                    this.numSegments = numSegments;
                    this.totalFields = totalFields;
                    this.postingsInMemoryBytes = postingsInMemoryBytes;
                    this.liveDocsBytes = liveDocsBytes;
                    this.shardMemoryOverheadBytes = shardMemoryOverheadBytes;
                    this.metricQuality = MetricQuality.EXACT;
                    this.metricShardNodeId = metricShardNodeId;
                    this.updateTimestampNanos = updateTime;
                }
                return true;
            }
            return false;
        }

        private synchronized void update(MetricQuality metricQuality, long updateTime) {
            this.metricQuality = metricQuality;
            this.updateTimestampNanos = updateTime;
        }

        private synchronized void update(String metricShardNodeId, long updateTime) {
            this.metricShardNodeId = metricShardNodeId;
            this.seqNo = 0;
            this.updateTimestampNanos = updateTime;
        }

        public synchronized long getMappingSizeInBytes() {
            return mappingSizeInBytes;
        }

        // visible for testing
        public synchronized MetricQuality getMetricQuality() {
            return metricQuality;
        }

        // visible for testing
        public synchronized String getMetricShardNodeId() {
            return metricShardNodeId;
        }

        // visible for testing
        public synchronized long getSeqNo() {
            return seqNo;
        }

        public synchronized int getNumSegments() {
            return numSegments;
        }

        public synchronized int getTotalFields() {
            return totalFields;
        }

        public synchronized long getPostingsInMemoryBytes() {
            return postingsInMemoryBytes;
        }

        public synchronized long getLiveDocsBytes() {
            return liveDocsBytes;
        }

        // visible for testing
        public synchronized long getShardMemoryOverheadBytes() {
            return shardMemoryOverheadBytes;
        }

        public synchronized long getUpdateTimestampNanos() {
            return updateTimestampNanos;
        }

        @Override
        public String toString() {
            return Strings.format(
                "ShardMemoryMetrics{mappingSizeInBytes=%d, numSegments=%d, totalFields=%d, "
                    + "postingsInMemoryBytes=%d, liveDocsBytes=%d, shardMemoryOverheadBytes=%d, seqNo=%d, "
                    + "metricQuality=%s, metricShardNodeId='%s', updateTimestampNanos='%d'}",
                mappingSizeInBytes,
                numSegments,
                totalFields,
                postingsInMemoryBytes,
                liveDocsBytes,
                shardMemoryOverheadBytes,
                seqNo,
                metricQuality,
                metricShardNodeId,
                updateTimestampNanos
            );
        }
    }

    /**
     * The last shard merge memory estimate received for a node. We accept the publications only if they have a
     * higher seq. no. than the previous value for the node. When a node leaves we update its {@code nodeLeftTimestampNanos}
     * but keep the publication for {@code DEFAULT_REMOVED_NODE_MERGE_MEMORY_ESTIMATION_VALIDITY}.
     */
    public record ShardMergeMemoryEstimatePublication(
        long seqNo,
        String nodeEphemeralId,
        ShardMergeMemoryEstimate estimate,
        long nodeLeftTimestampNanos
    ) {
        public ShardMergeMemoryEstimatePublication(long seqNo, String nodeEphemeralId, ShardMergeMemoryEstimate estimate) {
            this(seqNo, nodeEphemeralId, estimate, -1);
        }

        public ShardMergeMemoryEstimatePublication withNodeLeftTimestamp(long timestamp) {
            return new ShardMergeMemoryEstimatePublication(seqNo, nodeEphemeralId, estimate, timestamp);
        }

        public ShardMergeMemoryEstimatePublication withoutNodeLeftTimestamp() {
            return new ShardMergeMemoryEstimatePublication(seqNo, nodeEphemeralId, estimate);
        }

        public boolean hasNodeLeft() {
            return nodeLeftTimestampNanos > -1;
        }
    }

    // Visible for testing
    public Map<String, ShardMergeMemoryEstimatePublication> getMaxShardMergeMemoryEstimatePerNode() {
        return Map.copyOf(maxShardMergeMemoryEstimatePerNode);
    }

    private class EstimatedHeapUsageBuilder {
        private final long nodeBaseHeapEstimateInBytes;
        private final long minimumRequiredHeapForAcceptingLargeIndexingOps;
        private final long shardMergeMemoryEstimate;
        private final Set<String> seenIndices = new HashSet<>();
        private long mappingSizeInBytes;
        private long totalPostingsInMemoryBytes;
        private long shardMemoryUsageInBytes;
        private long totalShardMemoryOverheadBytes;
        private int totalShards;
        private int totalShardsWithSelfReportedOverhead;

        EstimatedHeapUsageBuilder(
            long nodeBaseHeapEstimateInBytes,
            long minimumRequiredHeapForAcceptingLargeIndexingOps,
            long shardMergeMemoryEstimate
        ) {
            this.nodeBaseHeapEstimateInBytes = nodeBaseHeapEstimateInBytes;
            this.minimumRequiredHeapForAcceptingLargeIndexingOps = minimumRequiredHeapForAcceptingLargeIndexingOps;
            this.shardMergeMemoryEstimate = shardMergeMemoryEstimate;
        }

        void add(ShardId shardId, ShardMemoryMetrics shardMemoryMetrics) {
            if (seenIndices.add(shardId.getIndexName())) {
                mappingSizeInBytes += shardMemoryMetrics.getMappingSizeInBytes();
            }
            if (isSelfReportedShardMemoryOverheadAvailable(shardMemoryMetrics)) {
                totalShardMemoryOverheadBytes += shardMemoryMetrics.getShardMemoryOverheadBytes();
                totalShardsWithSelfReportedOverhead++;
            } else {
                shardMemoryUsageInBytes += estimateShardMemoryUsageInBytes(shardMemoryMetrics);
                totalPostingsInMemoryBytes += shardMemoryMetrics.getPostingsInMemoryBytes();
            }
            totalShards++;
        }

        long getHeapUsageEstimate() {
            assert totalShards >= totalShardsWithSelfReportedOverhead;
            return totalShardMemoryOverheadBytes + shardMemoryUsageInBytes + mappingSizeInBytes + shardMergeMemoryEstimate
                + nodeBaseHeapEstimateInBytes + minimumRequiredHeapForAcceptingLargeIndexingOps + totalPostingsInMemoryBytes;
        }
    }

    private boolean isSelfReportedShardMemoryOverheadAvailable(ShardMemoryMetrics shardMemoryMetrics) {
        return selfReportedShardMemoryOverheadEnabled
            && shardMemoryMetrics.getShardMemoryOverheadBytes() != UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
    }

    /**
     * Create a new {@link ShardMemoryMetrics} for a shard that we have not yet received memory metrics for
     * <p>
     * Also used as a default when a shard with no metrics is encountered
     */
    ShardMemoryMetrics newUninitialisedShardMemoryMetrics(long updateTimestampNanos) {
        return new ShardMemoryMetrics(
            0L,
            0,
            0,
            0L,
            0L,
            UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
            0L,
            MetricQuality.MISSING,
            null,
            updateTimestampNanos
        );
    }

    /**
     * Per-shard heap usage from tracked metrics, plus {@link ShardHeapUsageEstimates#defaultForShardsWithoutMetrics()} derived from
     * {@link #newUninitialisedShardMemoryMetrics} so it matches uninitialized shards in {@link #shardMemoryMetrics}.
     */
    public ShardHeapUsageEstimates getShardHeapUsageEstimates() {
        final Map<ShardId, ShardAndIndexHeapUsage> heapUsagePerShard = new HashMap<>();
        for (Map.Entry<ShardId, ShardMemoryMetrics> entry : shardMemoryMetrics.entrySet()) {
            heapUsagePerShard.put(
                entry.getKey(),
                new ShardAndIndexHeapUsage(computeShardHeapUsage(entry.getValue()), computeIndexHeapUsage(entry.getValue()))
            );
        }
        ShardMemoryMetrics uninitialised = newUninitialisedShardMemoryMetrics(relativeTimeInNanos());
        ShardAndIndexHeapUsage defaultForShardsWithoutMetrics = new ShardAndIndexHeapUsage(
            computeShardHeapUsage(uninitialised),
            computeIndexHeapUsage(uninitialised)
        );
        return new ShardHeapUsageEstimates(heapUsagePerShard, defaultForShardsWithoutMetrics);
    }

    /**
     * Goes through the shard memory metrics and returns a map of shards to their shard-level and index-level heap usage components.
     */
    // visible for testing
    public Map<ShardId, ShardAndIndexHeapUsage> getShardHeapUsages() {
        return getShardHeapUsageEstimates().perShard();
    }

    /**
     * Computes the shard-level heap usage.
     * Ignores index-level heap usage, {@link #computeIndexHeapUsage} should be called for that.
     * Same computation as {@link EstimatedHeapUsageBuilder#add}, except excludes index and node level overheads.
     */
    // Visible for testing.
    public long computeShardHeapUsage(ShardMemoryMetrics shardMemoryMetrics) {
        if (isSelfReportedShardMemoryOverheadAvailable(shardMemoryMetrics)) {
            return shardMemoryMetrics.getShardMemoryOverheadBytes();
        }
        return estimateShardMemoryUsageInBytes(shardMemoryMetrics) + shardMemoryMetrics.getPostingsInMemoryBytes();
    }

    /**
     * Computes the index-level heap usage for a shard. {@link #INDEX_MEMORY_OVERHEAD} is not included because all nodes include an overhead
     * for all indices regardless of shard assignments: see {@link #getNodeBaseHeapEstimateInBytes()}.
     *
     * Same computation as {@link EstimatedHeapUsageBuilder#add}, except excludes shard and node level overheads.
     */
    // Visible for testing.
    public long computeIndexHeapUsage(ShardMemoryMetrics shardMemoryMetrics) {
        return shardMemoryMetrics.getMappingSizeInBytes();
    }

    public record ShardMergeMemoryEstimate(String mergeId, long estimateInBytes) implements Writeable {
        public static final ShardMergeMemoryEstimate NO_MERGES = new ShardMergeMemoryEstimate("", 0);

        public static final Comparator<ShardMergeMemoryEstimate> COMPARATOR = Comparator.comparingLong(
            ShardMergeMemoryEstimate::estimateInBytes
        ).thenComparing(ShardMergeMemoryEstimate::mergeId);

        public ShardMergeMemoryEstimate(StreamInput in) throws IOException {
            this(in.readString(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(mergeId);
            out.writeVLong(estimateInBytes);
        }
    }
}
