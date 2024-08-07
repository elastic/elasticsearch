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

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
    // let each shard use 6MB by default, which matches what we see in heap dumps (with a bit of margin).
    public static final ByteSizeValue SHARD_MEMORY_OVERHEAD_DEFAULT = ByteSizeValue.ofMb(6);
    public static final Setting<ByteSizeValue> SHARD_MEMORY_OVERHEAD_SETTING = Setting.byteSizeSetting(
        "serverless.autoscaling.memory_metrics.shard_memory_overhead",
        SHARD_MEMORY_OVERHEAD_DEFAULT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private static final Logger logger = LogManager.getLogger(MemoryMetricsService.class);
    // visible for testing
    static final long INDEX_MEMORY_OVERHEAD = ByteSizeValue.ofKb(350).getBytes();
    // visible for testing
    /**
     * See:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/size-your-shards.html#_consider_additional_heap_overheads
     * for the origin of this.
     */
    static final long WORKLOAD_MEMORY_OVERHEAD = ByteSizeValue.ofMb(500).getBytes();
    // cap node mem request to 48Gb. All current instance types have max size beyond that and none have a size 2x that.
    private static final long MAX_NODE_MEMORY = ByteSizeValue.ofGb(48).getBytes();
    private volatile boolean initialized = false;
    private final Map<ShardId, ShardMemoryMetrics> shardMemoryMetrics = new ConcurrentHashMap<>();
    private volatile int totalIndices;

    private final LongSupplier relativeTimeInNanosSupplier;
    private volatile TimeValue staleMetricsCheckDuration;

    private volatile long lastStaleMetricsCheckTimeNs = Long.MIN_VALUE;
    private volatile TimeValue staleMetricsCheckInterval;
    // visible for testing
    volatile ByteSizeValue shardMemoryOverhead;

    public MemoryMetricsService(LongSupplier relativeTimeInNanosSupplier, ClusterSettings clusterSettings) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        clusterSettings.initializeAndWatch(STALE_METRICS_CHECK_DURATION_SETTING, value -> staleMetricsCheckDuration = value);
        clusterSettings.initializeAndWatch(STALE_METRICS_CHECK_INTERVAL_SETTING, value -> staleMetricsCheckInterval = value);
        clusterSettings.initializeAndWatch(SHARD_MEMORY_OVERHEAD_SETTING, value -> shardMemoryOverhead = value);
    }

    public MemoryMetrics getMemoryMetrics() {
        final var totalIndicesMappingSize = calculateTotalIndicesMappingSize();

        final long nodeMemoryInBytes = Math.min(
            HeapToSystemMemory.dataNode(INDEX_MEMORY_OVERHEAD * totalIndices + WORKLOAD_MEMORY_OVERHEAD),
            MAX_NODE_MEMORY
        );

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
        final long tierMemoryInBytes = HeapToSystemMemory.tier(
            totalIndicesMappingSize.sizeInBytes + shardMemoryOverhead.getBytes() * shardMemoryMetrics.size()
        );

        return new MemoryMetrics(nodeMemoryInBytes, tierMemoryInBytes, totalIndicesMappingSize.metricQuality);
    }

    // Total mapping size of all known indices, and whether it is exact or not.
    record TotalIndicesMappingsSize(long sizeInBytes, MetricQuality metricQuality) {}

    // Calculates rolling sum & metric quality of all known indices, if any of qualities is not `EXACT` report the whole batch as such.
    TotalIndicesMappingsSize calculateTotalIndicesMappingSize() {
        long sizeInBytes = 0;
        MetricQuality metricQuality = MetricQuality.EXACT;
        // Can't control the frequency in which getTotalIndicesMappingSize is called, so we need to make sure we run the stale check
        // at a regular interval to avoid flooding logs with stale index warnings
        boolean checkStaleMetrics = relativeTimeInNanos() - staleMetricsCheckInterval.nanos() > lastStaleMetricsCheckTimeNs;
        if (checkStaleMetrics) {
            lastStaleMetricsCheckTimeNs = relativeTimeInNanos();
        }
        for (var entry : shardMemoryMetrics.entrySet()) {
            // TODO: Use mapping_size_in_bytes from any shard metric once the cluster is upgraded. Consider using the highest
            // quality (exact) metric of each index to improve metric quality and minimize excessive warning logs from stale metrics.
            if (entry.getKey().id() != 0) {
                continue;
            }
            var memoryMetrics = entry.getValue();
            sizeInBytes += memoryMetrics.mappingSizeInBytes;
            metricQuality = memoryMetrics.getMetricQuality() == MetricQuality.EXACT ? metricQuality : memoryMetrics.getMetricQuality();
            if (checkStaleMetrics
                && memoryMetrics.getMetricQuality() != MetricQuality.EXACT
                && relativeTimeInNanos() - staleMetricsCheckDuration.nanos() > memoryMetrics.getUpdateTimestampNanos()) {
                logger.warn("Memory metrics are stale for shard {}", entry);
            }
        }
        return new TotalIndicesMappingsSize(sizeInBytes, metricQuality);
    }

    // visible for testing
    Map<ShardId, ShardMemoryMetrics> getShardMemoryMetrics() {
        return shardMemoryMetrics;
    }

    void updateShardsMappingSize(final HeapMemoryUsage heapMemoryUsage) {
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
                    heapMemoryUsage.publicationSeqNo(),
                    shardMappingSize.nodeId(),
                    relativeTimeInNanos()
                );
            }
            if (applied == false) {
                missedUpdates.add(shardId);
            }
        }
        if (missedUpdates.size() > 0) {
            throw new AutoscalingMissedIndicesUpdateException(
                "Failed to fully apply " + heapMemoryUsage + " due to missed shards: " + missedUpdates
            );
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            shardMemoryMetrics.clear();
            initialized = false;
            return;
        }
        this.totalIndices = event.state().metadata().indices().size();

        // new master use case: no indices exist in internal map
        if (event.nodesDelta().masterNodeChanged() || initialized == false) {
            for (IndexMetadata indexMetadata : event.state().metadata()) {
                for (int id = 0; id < indexMetadata.getNumberOfShards(); id++) {
                    var shardMemoryMetrics = new ShardMemoryMetrics(
                        0L,
                        0,
                        0,
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
            return;
        }

        if (event.metadataChanged() || event.routingTableChanged()) {

            // index delete use case
            for (Index deletedIndex : event.indicesDeleted()) {
                int numberOfShards = event.previousState().metadata().index(deletedIndex).getNumberOfShards();
                for (int id = 0; id < numberOfShards; id++) {
                    shardMemoryMetrics.remove(new ShardId(deletedIndex, id));
                }
            }

            for (IndexMetadata indexMetadata : event.state().metadata()) {
                final Index index = indexMetadata.getIndex();
                for (int id = 0; id < indexMetadata.getNumberOfShards(); id++) {
                    // index created use case, EXACT values will be sent by index node
                    shardMemoryMetrics.putIfAbsent(
                        new ShardId(index, id),
                        new ShardMemoryMetrics(0L, 0, 0, 0L, MetricQuality.MISSING, null, relativeTimeInNanosSupplier.getAsLong())
                    );
                }
                // index mapping update use case
                final IndexMetadata oldIndexMetadata = event.previousState().metadata().index(index);
                final IndexMetadata newIndexMetadata = event.state().metadata().index(index);

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
                if (event.indexRoutingTableChanged(index.getName())) {
                    for (int id = 0; id < indexMetadata.getNumberOfShards(); id++) {
                        ShardId shardId = new ShardId(indexMetadata.getIndex(), id);
                        ShardRouting newShardRouting = event.state().routingTable().shardRoutingTable(shardId).primaryShard();
                        if (newShardRouting.assignedToNode()) {
                            final ShardMemoryMetrics shardMemoryMetrics = this.shardMemoryMetrics.get(shardId);
                            assert shardMemoryMetrics != null;
                            final String lastKnownShardNodeId = shardMemoryMetrics.getMetricShardNodeId();
                            final String newShardNodeId = newShardRouting.currentNodeId();
                            if (newShardNodeId.equals(lastKnownShardNodeId) == false) {
                                // preserve last-known value, since shard has moved and no mapping change happened
                                shardMemoryMetrics.update(newShardRouting.currentNodeId(), relativeTimeInNanos());
                            }
                        }
                    }
                }
            }
        }
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    static class ShardMemoryMetrics {
        private long mappingSizeInBytes;
        private int numSegments = 0;
        private int totalFields = 0;
        private long seqNo;
        private MetricQuality metricQuality;
        private String metricShardNodeId;   // node id which hosts sending primary 0-shard
        private long updateTimestampNanos;

        ShardMemoryMetrics(
            long mappingSizeInBytes,
            int numSegments,
            int totalFields,
            long seqNo,
            MetricQuality metricQuality,
            String metricShardNodeId,
            long updateTimestampNanos
        ) {
            this.mappingSizeInBytes = mappingSizeInBytes;
            this.numSegments = numSegments;
            this.totalFields = totalFields;
            this.seqNo = seqNo;
            this.metricQuality = metricQuality;
            this.metricShardNodeId = metricShardNodeId;
            this.updateTimestampNanos = updateTimestampNanos;
        }

        synchronized boolean update(
            long mappingSizeInBytes,
            int numSegments,
            int totalFields,
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

}
