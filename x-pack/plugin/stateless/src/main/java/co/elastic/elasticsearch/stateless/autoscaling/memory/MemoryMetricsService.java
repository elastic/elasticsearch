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
    private static final int SENDING_PRIMARY_SHARD_ID = 0;
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
    private final Map<Index, IndexMemoryMetrics> indicesMemoryMetrics = new ConcurrentHashMap<>();

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
            HeapToSystemMemory.dataNode(INDEX_MEMORY_OVERHEAD * indicesCount() + WORKLOAD_MEMORY_OVERHEAD),
            MAX_NODE_MEMORY
        );

        // notice that autoscaling controller adds the node memory multiplied by replicas to the tier memory:
        // https://github.com/elastic/elasticsearch-serverless/pull/1372/files#r1467399624
        // That indirectly adds the necessary total indices and workload memory due to being included in the node memory above.
        // We assume 1 shard per index for now.
        final long tierMemoryInBytes = HeapToSystemMemory.dataNode(
            totalIndicesMappingSize.sizeInBytes + shardMemoryOverhead.getBytes() * indicesCount()
        );

        return new MemoryMetrics(nodeMemoryInBytes, tierMemoryInBytes, totalIndicesMappingSize.metricQuality);
    }

    private int indicesCount() {
        return indicesMemoryMetrics.size(); // since mapping is Index -> IndexMemoryMetrics
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
        for (var indexMemoryMetrics : indicesMemoryMetrics.entrySet()) {
            var memoryMetrics = indexMemoryMetrics.getValue();
            sizeInBytes += memoryMetrics.sizeInBytes;
            metricQuality = memoryMetrics.getMetricQuality() == MetricQuality.EXACT ? metricQuality : memoryMetrics.getMetricQuality();
            if (checkStaleMetrics
                && memoryMetrics.getMetricQuality() != MetricQuality.EXACT
                && relativeTimeInNanos() - staleMetricsCheckDuration.nanos() > memoryMetrics.getUpdateTimestampNanos()) {
                logger.warn("Memory metrics are stale for index {}", indexMemoryMetrics);
            }
        }
        return new TotalIndicesMappingsSize(sizeInBytes, metricQuality);
    }

    // visible for testing
    Map<Index, IndexMemoryMetrics> getIndicesMemoryMetrics() {
        return indicesMemoryMetrics;
    }

    void updateIndicesMappingSize(final HeapMemoryUsage heapMemoryUsage) {
        List<Index> missedUpdates = new ArrayList<>();
        for (var entry : heapMemoryUsage.indicesMappingSize().entrySet()) {
            Index index = entry.getKey();
            IndexMappingSize indexMappingSize = entry.getValue();
            boolean applied = false;
            IndexMemoryMetrics indexMemoryMetrics = indicesMemoryMetrics.get(index);
            if (indexMemoryMetrics != null) {
                applied = indexMemoryMetrics.update(
                    indexMappingSize.sizeInBytes(),
                    heapMemoryUsage.publicationSeqNo(),
                    indexMappingSize.metricShardNodeId(),
                    relativeTimeInNanos()
                );
            }
            if (applied == false) {
                missedUpdates.add(index);
            }
        }
        if (missedUpdates.size() > 0) {
            throw new AutoscalingMissedIndicesUpdateException(
                "Failed to fully apply " + heapMemoryUsage + " due to missed indices: " + missedUpdates
            );
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            indicesMemoryMetrics.clear();
            initialized = false;
            return;
        }

        // new master use case: no indices exist in internal map
        if (event.nodesDelta().masterNodeChanged() || initialized == false) {
            for (IndexMetadata indexMetadata : event.state().metadata()) {
                IndexMemoryMetrics indexMemoryMetrics = new IndexMemoryMetrics(
                    0,
                    MetricQuality.MISSING,
                    relativeTimeInNanosSupplier.getAsLong()
                );

                // new master should track the current assigned primary shard node in order to accept updates from such nodes
                var shardRouting = getPrimaryShardRouting(event, indexMetadata.getIndex());
                if (shardRouting.assignedToNode()) {
                    indexMemoryMetrics.update(shardRouting.currentNodeId(), relativeTimeInNanos());
                }
                indicesMemoryMetrics.put(indexMetadata.getIndex(), indexMemoryMetrics);
            }
            initialized = true;
            return;
        }

        if (event.metadataChanged() || event.routingTableChanged()) {

            // index delete use case
            for (Index deletedIndex : event.indicesDeleted()) {
                indicesMemoryMetrics.remove(deletedIndex);
            }

            for (IndexMetadata indexMetadata : event.state().metadata()) {

                final Index index = indexMetadata.getIndex();

                // index created use case, EXACT values will be sent by index node
                indicesMemoryMetrics.putIfAbsent(
                    index,
                    new IndexMemoryMetrics(0, MetricQuality.MISSING, relativeTimeInNanosSupplier.getAsLong())
                );

                // index mapping update use case
                final IndexMetadata oldIndexMetadata = event.previousState().metadata().index(index);
                final IndexMetadata newIndexMetadata = event.state().metadata().index(index);

                if (oldIndexMetadata != null
                    && newIndexMetadata != null
                    && ClusterChangedEvent.indexMetadataChanged(oldIndexMetadata, newIndexMetadata)) {
                    if (oldIndexMetadata.getMappingVersion() < newIndexMetadata.getMappingVersion()) {
                        // set last known value as MINIMUM, EXACT value will be sent by index node
                        IndexMemoryMetrics indexMemoryMetrics = indicesMemoryMetrics.get(index);
                        assert indexMemoryMetrics != null;
                        indexMemoryMetrics.update(MetricQuality.MINIMUM, relativeTimeInNanos());
                    }
                }

                // moving shard use case
                if (event.indexRoutingTableChanged(index.getName())) {
                    final ShardRouting newShardRouting = getPrimaryShardRouting(event, index);
                    if (newShardRouting.assignedToNode()) {
                        final IndexMemoryMetrics indexMemoryMetrics = indicesMemoryMetrics.get(index);
                        assert indexMemoryMetrics != null;
                        final String lastKnownShardNodeId = indexMemoryMetrics.getMetricShardNodeId();
                        final String newShardNodeId = newShardRouting.currentNodeId();
                        if (newShardNodeId.equals(lastKnownShardNodeId) == false) {
                            // preserve last-known value, since shard has moved and no mapping change happened
                            indexMemoryMetrics.update(newShardRouting.currentNodeId(), relativeTimeInNanos());
                        }
                    }
                }
            }
        }
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    private static ShardRouting getPrimaryShardRouting(ClusterChangedEvent event, Index indexMetadata) {
        return event.state().routingTable().shardRoutingTable(indexMetadata.getName(), SENDING_PRIMARY_SHARD_ID).primaryShard();
    }

    static class IndexMemoryMetrics {
        private long sizeInBytes;
        private long seqNo;
        private MetricQuality metricQuality;
        private String metricShardNodeId;   // node id which hosts sending primary 0-shard
        private long updateTimestampNanos;

        IndexMemoryMetrics(long sizeInBytes, MetricQuality metricQuality, long updateTimestampNanos) {
            this(sizeInBytes, 0, metricQuality, null, updateTimestampNanos);
        }

        IndexMemoryMetrics(long sizeInBytes, long seqNo, MetricQuality metricQuality, String metricShardNodeId, long updateTimestampNanos) {
            this.sizeInBytes = sizeInBytes;
            this.seqNo = seqNo;
            this.metricQuality = metricQuality;
            this.metricShardNodeId = metricShardNodeId;
            this.updateTimestampNanos = updateTimestampNanos;
        }

        synchronized boolean update(long sizeInBytes, long seqNo, String metricShardNodeId, long updateTime) {
            assert metricShardNodeId != null;
            // drop messages which were sent from unknown nodes
            // e.g. message late arrival from previously allocated node (where primary 0-shard used to be)
            if (Objects.equals(this.metricShardNodeId, metricShardNodeId)) {
                if (this.seqNo < seqNo) { // handle reordered messages which were sent from the same node
                    this.seqNo = seqNo;
                    this.sizeInBytes = sizeInBytes;
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
        synchronized long getSizeInBytes() {
            return sizeInBytes;
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

        synchronized long getUpdateTimestampNanos() {
            return updateTimestampNanos;
        }

        @Override
        public String toString() {
            return Strings.format(
                "IndexMemoryMetrics{sizeInBytes=%d, seqNo=%d, metricQuality=%s, metricShardNodeId='%s', updateTimestampNanos='%d'}",
                sizeInBytes,
                seqNo,
                metricQuality,
                metricShardNodeId,
                updateTimestampNanos
            );
        }
    }

}
