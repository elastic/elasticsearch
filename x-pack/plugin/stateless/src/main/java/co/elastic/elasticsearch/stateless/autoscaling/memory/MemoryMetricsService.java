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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MergeMemoryEstimateCollector.ShardMergeMemoryEstimate;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

public class MemoryMetricsService implements ClusterStateListener {
    public static final Setting<TimeValue> STALE_METRICS_CHECK_DURATION_SETTING = Setting.timeSetting(
        "serverless.autoscaling.memory_metrics.indices_mapping_size.stale_metrics_check.duration",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> STALE_METRICS_CHECK_INTERVAL_SETTING = Setting.timeSetting(
        "serverless.autoscaling.memory_metrics.indices_mapping_size.stale_metrics_check.interval",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    static final TimeValue DEFAULT_INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY = TimeValue.timeValueMinutes(2);
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
     * Two methods to estimate the memory usage of IndexShard instances:
     * 1. Fixed Method: Assigns a default value of 6MB per shard. This method may overestimate memory usage for shards with minimal data
     * and underestimate it for shards with many segments and fields.
     * 2. Adaptive Method: Estimates memory usage based on the actual number of segments and fields in segments. While generally more
     * accurate, this method can still occasionally overestimate or underestimate memory usage.
     * <p>
     * By default, the Fixed Method is used. To switch to the Adaptive Method, explicitly set
     * the `serverless.autoscaling.memory_metrics.shard_memory_overhead` setting to -1.
     */
    public static final ByteSizeValue FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT = ByteSizeValue.ofMb(6);
    public static final Setting<ByteSizeValue> FIXED_SHARD_MEMORY_OVERHEAD_SETTING = Setting.byteSizeSetting(
        "serverless.autoscaling.memory_metrics.shard_memory_overhead",
        FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final String INDEXING_MEMORY_MINIMUM_HEAP_REQUIRED_TO_ACCEPT_LARGE_OPERATIONS_METRIC_NAME =
        "es.autoscaling.indexing.memory.heap_required_large_operations.current";

    // We clean up a node's reported max merge when the node leaves the cluster. In order to ensure a node's reported max merges are
    // consumed in the correct sequence, we rely on keeping a sequence no of the last reported value to be able to reject out of order
    // publications for that node. Instead of removing the last known publication immediately when a node leaves the cluster, we wait for
    // the interval specified below and remove it if the node doesn't come back.
    public static final TimeValue DEFAULT_REMOVED_NODE_MERGE_MEMORY_ESTIMATION_VALIDITY = TimeValue.timeValueMinutes(1);

    volatile ByteSizeValue fixedShardMemoryOverhead;
    // The memory overhead of each IndexShard instance used in the adaptive estimate
    static final ByteSizeValue ADAPTIVE_SHARD_MEMORY_OVERHEAD = ByteSizeValue.ofKb(75);
    // The memory overhead of each Lucene segment, including maps for postings, doc_values, and stored_fields producers
    static final ByteSizeValue ADAPTIVE_SEGMENT_MEMORY_OVERHEAD = ByteSizeValue.ofKb(55);
    // The memory overhead of each field found in Lucene segments
    static final ByteSizeValue ADAPTIVE_FIELD_MEMORY_OVERHEAD = ByteSizeValue.ofBytes(1024);

    private static final Logger logger = LogManager.getLogger(MemoryMetricsService.class);
    // visible for testing
    static final long INDEX_MEMORY_OVERHEAD = ByteSizeValue.ofKb(350).getBytes();

    /**
     * See:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/size-your-shards.html#_consider_additional_heap_overheads
     * for the origin of this.
     */
    static final long WORKLOAD_MEMORY_OVERHEAD = ByteSizeValue.ofMb(500).getBytes();
    // cap node mem request to 48Gb. All current instance types have max size beyond that and none have a size 2x that.
    private static final long MAX_NODE_MEMORY = ByteSizeValue.ofGb(48).getBytes();

    public static final Setting<Boolean> MERGE_MEMORY_ESTIMATE_ENABLED_SETTING = Setting.boolSetting(
        "serverless.autoscaling.memory_metrics.merge_memory_estimate.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean initialized = false;
    private final Map<ShardId, ShardMemoryMetrics> shardMemoryMetrics = new ConcurrentHashMap<>();
    private volatile int totalIndices;
    private final AtomicReference<IndexingOperationsMemoryRequirements> indexingOperationsHeapMemoryRequirementsRef =
        new AtomicReference<>();

    private final LongSupplier relativeTimeInNanosSupplier;
    private final ProjectType projectType;
    private volatile TimeValue staleMetricsCheckDuration;
    private volatile boolean indexingOperationsMemoryMetricsEnabled;
    private volatile TimeValue indexingOperationsMemoryMetricsValidityDuration;

    private volatile long lastStaleMetricsCheckTimeNs = Long.MIN_VALUE;
    private volatile TimeValue staleMetricsCheckInterval;
    private volatile long clusterStateVersion = ClusterState.UNKNOWN_VERSION;
    private volatile boolean mergeMemoryEstimateEnabled;
    private volatile double adaptiveExtraOverheadRatio;
    private final Map<String, ShardMergeMemoryEstimatePublication> maxShardMergeMemoryEstimatePerNode = new ConcurrentHashMap<>();

    public MemoryMetricsService(
        LongSupplier relativeTimeInNanosSupplier,
        ClusterSettings clusterSettings,
        ProjectType projectType,
        MeterRegistry meterRegistry
    ) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.projectType = projectType;
        clusterSettings.initializeAndWatch(
            INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING,
            value -> indexingOperationsMemoryMetricsEnabled = value
        );
        clusterSettings.initializeAndWatch(
            INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING,
            value -> indexingOperationsMemoryMetricsValidityDuration = value
        );
        clusterSettings.initializeAndWatch(STALE_METRICS_CHECK_DURATION_SETTING, value -> staleMetricsCheckDuration = value);
        clusterSettings.initializeAndWatch(STALE_METRICS_CHECK_INTERVAL_SETTING, value -> staleMetricsCheckInterval = value);
        clusterSettings.initializeAndWatch(FIXED_SHARD_MEMORY_OVERHEAD_SETTING, value -> fixedShardMemoryOverhead = value);
        clusterSettings.initializeAndWatch(MERGE_MEMORY_ESTIMATE_ENABLED_SETTING, value -> mergeMemoryEstimateEnabled = value);
        clusterSettings.initializeAndWatch(ADAPTIVE_EXTRA_OVERHEAD_SETTING, value -> adaptiveExtraOverheadRatio = value.getAsRatio());
        setupMetrics(meterRegistry);
    }

    private void setupMetrics(MeterRegistry meterRegistry) {
        meterRegistry.registerLongsGauge(
            INDEXING_MEMORY_MINIMUM_HEAP_REQUIRED_TO_ACCEPT_LARGE_OPERATIONS_METRIC_NAME,
            "Minimum heap required to accept large indexing operations",
            "bytes",
            () -> {
                var latestIndexingOperationsMemoryRequirements = indexingOperationsHeapMemoryRequirementsRef.get();
                if (latestIndexingOperationsMemoryRequirements == null
                    || latestIndexingOperationsMemoryRequirements.validUntil() < relativeTimeInNanos()) {
                    return List.of();
                }
                return List.of(new LongWithAttributes(latestIndexingOperationsMemoryRequirements.minimumRequiredHeapInBytes()));
            }
        );
    }

    public MemoryMetrics getSearchTierMemoryMetrics() {
        return getMemoryMetrics(getNodeBaseHeapEstimateInBytes());
    }

    public MemoryMetrics getIndexingTierMemoryMetrics() {
        var nodeHeapEstimateInBytes = getNodeBaseHeapEstimateInBytes() + minimumRequiredHeapForAcceptingLargeIndexingOps()
            + mergeMemoryEstimation();
        return getMemoryMetrics(nodeHeapEstimateInBytes);
    }

    public Map<String, Long> getPerNodeMemoryMetrics(DiscoveryNodes discoveryNodes) {
        final long nodeBaseHeapEstimateInBytes = getNodeBaseHeapEstimateInBytes();
        final long mergeMemoryEstimate = mergeMemoryEstimation();
        final long minimumRequiredHeapForHandlingLargeIndexingOps = minimumRequiredHeapForAcceptingLargeIndexingOps();
        final Map<String, EstimatedHeapUsageBuilder> heapUsageBuilders = new HashMap<>();
        for (Map.Entry<ShardId, ShardMemoryMetrics> entry : shardMemoryMetrics.entrySet()) {
            final String shardNodeId = entry.getValue().getMetricShardNodeId();
            if (shardNodeId == null) { // nodeId is null for unassigned shard
                continue;
            }
            final EstimatedHeapUsageBuilder builderForNode = heapUsageBuilders.computeIfAbsent(shardNodeId, id -> {
                // Pass the DiscoveryNode if available, this allows us to determine the ephemeral ID and look for current merge memory
                // estimates
                final DiscoveryNode discoveryNode = discoveryNodes.get(id);
                // We only provide estimates for indexing nodes
                if (discoveryNode != null && discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
                    return null;
                }
                return new EstimatedHeapUsageBuilder(
                    discoveryNode,
                    nodeBaseHeapEstimateInBytes,
                    minimumRequiredHeapForHandlingLargeIndexingOps,
                    mergeMemoryEstimate
                );
            });
            if (builderForNode != null) {
                builderForNode.add(entry.getKey(), entry.getValue());
            }
        }
        return Maps.transformValues(heapUsageBuilders, EstimatedHeapUsageBuilder::getHeapUsageEstimate);
    }

    private MemoryMetrics getMemoryMetrics(long nodeHeapEstimateInBytes) {
        final long nodeMemoryInBytes = Math.min(HeapToSystemMemory.dataNode(nodeHeapEstimateInBytes, projectType), MAX_NODE_MEMORY);

        // Note that the autoscaling controller adds the node memory multiplied by the number of search nodes to the following tier memory
        // calculation (tierMemoryInBytes) to come up with the actual minimum total tier size.
        // https://github.com/elastic/elasticsearch-autoscaler/blob/c5124d6d94ff79960590a118bf44e72b58e6821d/internal/autoscaler/
        // elasticsearch/autoscaling/mapper/step_function.go#L296-L311
        //
        // That indirectly adds the necessary total indices and workload memory due to being included in the node memory above.
        //
        // The following tier memory calculation only accounts for the memory requirements that is NOT considered in the base node memory.
        // This value is used by both indexing and search metrics and assumes that each shard has one replica per tier.
        // The controller ensures that the search tier also considers the max known shard replicas in the search tier memory calculation to
        // correct the total tier memory requirement in cases where there are more than one search replica.
        //
        // https://github.com/elastic/elasticsearch-autoscaler/blob/72ac2692f900dc8fe5220b53b1ab20b88008ef7e/internal/autoscaler/
        // elasticsearch/autoscaling/recommender/search.go#L142
        final var estimateMemoryUsage = estimateTierMemoryUsage();
        final long tierMemoryInBytes = HeapToSystemMemory.tier(estimateMemoryUsage.totalBytes, projectType);
        return new MemoryMetrics(nodeMemoryInBytes, tierMemoryInBytes, estimateMemoryUsage.metricQuality);
    }

    private long getNodeBaseHeapEstimateInBytes() {
        return INDEX_MEMORY_OVERHEAD * totalIndices + WORKLOAD_MEMORY_OVERHEAD;
    }

    private long minimumRequiredHeapForAcceptingLargeIndexingOps() {
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

    // Visible for testing
    long mergeMemoryEstimation() {
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

    // Estimate of total mapping size of all known indices and IndexShard instances
    record TierEstimateMemoryUsage(long totalBytes, MetricQuality metricQuality) {}

    // Calculates sum & metric quality of all known indices and shards, if any of qualities is not `EXACT` report the whole batch as such.
    TierEstimateMemoryUsage estimateTierMemoryUsage() {
        MetricQuality metricQuality = MetricQuality.EXACT;
        // Can't control the frequency in which getTotalIndicesMappingSize is called, so we need to make sure we run the stale check
        // at a regular interval to avoid flooding logs with stale index warnings
        boolean checkStaleMetrics = relativeTimeInNanos() - staleMetricsCheckInterval.nanos() > lastStaleMetricsCheckTimeNs;
        if (checkStaleMetrics) {
            lastStaleMetricsCheckTimeNs = relativeTimeInNanos();
        }
        long mappingSizeInBytes = 0;
        long totalSegments = 0;
        long totalFields = 0;
        long totalPostingsInMemoryBytes = 0;
        long totalLiveDocsBytes = 0;
        for (var entry : shardMemoryMetrics.entrySet()) {
            var metric = entry.getValue();
            // once per index
            if (entry.getKey().id() == 0) {
                mappingSizeInBytes += metric.mappingSizeInBytes;
            }
            totalSegments += metric.getNumSegments();
            totalFields += metric.getTotalFields();
            totalPostingsInMemoryBytes += metric.getPostingsInMemoryBytes();
            totalLiveDocsBytes += metric.getLiveDocsBytes();
            metricQuality = metric.getMetricQuality() == MetricQuality.EXACT ? metricQuality : metric.getMetricQuality();
            if (checkStaleMetrics
                && metric.getMetricQuality() != MetricQuality.EXACT
                && relativeTimeInNanos() - staleMetricsCheckDuration.nanos() > metric.getUpdateTimestampNanos()) {
                logger.warn("Memory metrics are stale for shard {}", entry);
            }
        }
        final long shardMemoryInBytes = estimateShardMemoryUsageInBytes(
            shardMemoryMetrics.size(),
            totalSegments,
            totalFields,
            totalPostingsInMemoryBytes,
            totalLiveDocsBytes
        );
        return new TierEstimateMemoryUsage(mappingSizeInBytes + shardMemoryInBytes, metricQuality);
    }

    long estimateShardMemoryUsageInBytes(
        int numShards,
        long numSegments,
        long numFields,
        long postingsInMemoryBytes,
        long totalLiveDocsBytes
    ) {
        final var fixedShardOverhead = this.fixedShardMemoryOverhead;
        if (fixedShardOverhead.getBytes() > 0) {
            return fixedShardOverhead.getBytes() * numShards;
        }
        long estimateBytes = numShards * ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + numSegments * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD
            .getBytes() + numFields * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes() + postingsInMemoryBytes + totalLiveDocsBytes;
        long extraBytes = (long) (estimateBytes * adaptiveExtraOverheadRatio);
        return estimateBytes + extraBytes;
    }

    // visible for testing
    Map<ShardId, ShardMemoryMetrics> getShardMemoryMetrics() {
        return shardMemoryMetrics;
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
    void updateShardsMappingSize(final HeapMemoryUsage heapMemoryUsage) {
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

    void updateIndexingOperationsHeapMemoryRequirements(long indexingHeapMemoryRequirements) {
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

    void updateMergeMemoryEstimate(ShardMergeMemoryEstimatePublication receivedPublication) {
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
            // back
            // we remove its entry
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
                    var shardMemoryMetrics = new ShardMemoryMetrics(
                        0L,
                        0,
                        0,
                        0L,
                        0L,
                        0L,
                        MetricQuality.MISSING,
                        null,
                        relativeTimeInNanosSupplier.getAsLong()
                    );
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
                        new ShardMemoryMetrics(0L, 0, 0, 0L, 0L, 0L, MetricQuality.MISSING, null, relativeTimeInNanosSupplier.getAsLong())
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

    static class ShardMemoryMetrics {
        private long mappingSizeInBytes;
        private int numSegments = 0;
        private int totalFields = 0;
        private long postingsInMemoryBytes = 0;
        private long liveDocsBytes = 0;
        private long seqNo;
        private MetricQuality metricQuality;
        private String metricShardNodeId;   // node id which hosts sending primary 0-shard
        private long updateTimestampNanos;

        ShardMemoryMetrics(
            long mappingSizeInBytes,
            int numSegments,
            int totalFields,
            long postingsInMemoryBytes,
            long liveDocsBytes,
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
            this.seqNo = seqNo;
            this.metricQuality = metricQuality;
            this.metricShardNodeId = metricShardNodeId;
            this.updateTimestampNanos = updateTimestampNanos;
        }

        synchronized boolean update(
            long mappingSizeInBytes,
            int numSegments,
            int totalFields,
            long postingsInMemoryBytes,
            long liveDocsBytes,
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

        // visible for testing
        synchronized long getMappingSizeInBytes() {
            return mappingSizeInBytes;
        }

        // visible for testing
        synchronized MetricQuality getMetricQuality() {
            return metricQuality;
        }

        // visible for testing
        synchronized String getMetricShardNodeId() {
            return metricShardNodeId;
        }

        // visible for testing
        synchronized long getSeqNo() {
            return seqNo;
        }

        synchronized int getNumSegments() {
            return numSegments;
        }

        synchronized int getTotalFields() {
            return totalFields;
        }

        synchronized long getPostingsInMemoryBytes() {
            return postingsInMemoryBytes;
        }

        synchronized long getLiveDocsBytes() {
            return liveDocsBytes;
        }

        synchronized long getUpdateTimestampNanos() {
            return updateTimestampNanos;
        }

        @Override
        public String toString() {
            return Strings.format(
                "ShardMemoryMetrics{mappingSizeInBytes=%d, numSegments=%d, totalFields=%d, seqNo=%d, "
                    + "metricQuality=%s, metricShardNodeId='%s', updateTimestampNanos='%d'}",
                mappingSizeInBytes,
                numSegments,
                totalFields,
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
        @Nullable
        private final DiscoveryNode discoveryNode;
        private final long nodeBaseHeapEstimateInBytes;
        private final long minimumRequiredHeapForAcceptingLargeIndexingOps;
        private final long shardMergeMemoryEstimate;
        private final Set<String> seenIndices = new HashSet<>();
        private long mappingSizeInBytes;
        private long totalSegments;
        private long totalFields;
        private long totalPostingsInMemoryBytes;
        private long totalLiveDocsBytes;
        private int totalShards;

        EstimatedHeapUsageBuilder(
            @Nullable DiscoveryNode discoveryNode,
            long nodeBaseHeapEstimateInBytes,
            long minimumRequiredHeapForAcceptingLargeIndexingOps,
            long shardMergeMemoryEstimate
        ) {
            this.discoveryNode = discoveryNode;
            this.nodeBaseHeapEstimateInBytes = nodeBaseHeapEstimateInBytes;
            this.minimumRequiredHeapForAcceptingLargeIndexingOps = minimumRequiredHeapForAcceptingLargeIndexingOps;
            this.shardMergeMemoryEstimate = shardMergeMemoryEstimate;
        }

        void add(ShardId shardId, ShardMemoryMetrics shardMemoryMetrics) {
            if (seenIndices.add(shardId.getIndexName())) {
                mappingSizeInBytes += shardMemoryMetrics.getMappingSizeInBytes();
            }
            totalSegments += shardMemoryMetrics.getNumSegments();
            totalFields += shardMemoryMetrics.getTotalFields();
            totalPostingsInMemoryBytes += shardMemoryMetrics.getPostingsInMemoryBytes();
            totalLiveDocsBytes += shardMemoryMetrics.getLiveDocsBytes();
            totalShards++;
        }

        long getHeapUsageEstimate() {
            final long shardMemoryUsageInBytes = estimateShardMemoryUsageInBytes(
                totalShards,
                totalSegments,
                totalFields,
                totalPostingsInMemoryBytes,
                totalLiveDocsBytes
            );
            return shardMemoryUsageInBytes + mappingSizeInBytes + shardMergeMemoryEstimate + nodeBaseHeapEstimateInBytes
                + minimumRequiredHeapForAcceptingLargeIndexingOps;
        }
    }
}
