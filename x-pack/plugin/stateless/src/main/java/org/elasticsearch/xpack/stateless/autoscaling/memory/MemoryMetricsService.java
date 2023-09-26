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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryMetricsService implements ClusterStateListener {
    private static final int SENDING_PRIMARY_SHARD_ID = 0;
    private static final long INDEX_MEMORY_OVERHEAD = ByteSizeValue.ofKb(350).getBytes();
    private static final long WORKLOAD_MEMORY_OVERHEAD = ByteSizeValue.ofMb(500).getBytes();
    private volatile boolean initialized = false;
    private final Map<Index, IndexMemoryMetrics> indicesMemoryMetrics = new ConcurrentHashMap<>();

    public MemoryMetricsService() {}

    public MemoryMetrics getMemoryMetrics() {
        final IndexMemoryMetrics totalIndicesMappingSize = getTotalIndicesMappingSize();

        final long nodeMemoryInBytes = HeapToSystemMemory.dataNode(INDEX_MEMORY_OVERHEAD * indicesCount() + WORKLOAD_MEMORY_OVERHEAD);
        final long tierMemoryInBytes = HeapToSystemMemory.dataNode(totalIndicesMappingSize.sizeInBytes);

        return new MemoryMetrics(nodeMemoryInBytes, tierMemoryInBytes, totalIndicesMappingSize.metricQuality);
    }

    private int indicesCount() {
        return indicesMemoryMetrics.size(); // since mapping is Index -> IndexMemoryMetrics
    }

    IndexMemoryMetrics getTotalIndicesMappingSize() {
        return indicesMemoryMetrics.values()
            .stream()
            .reduce(
                new IndexMemoryMetrics(0, MetricQuality.EXACT),
                // rolling sum & metric quality, if any of qualities is not `EXACT` report the whole batch as such
                (acc, curr) -> new IndexMemoryMetrics(
                    acc.sizeInBytes + curr.sizeInBytes,
                    MetricQuality.EXACT == curr.metricQuality ? acc.metricQuality : curr.metricQuality
                )
            );
    }

    // visible for testing
    Map<Index, IndexMemoryMetrics> getIndicesMemoryMetrics() {
        return indicesMemoryMetrics;
    }

    void updateIndicesMappingSize(final HeapMemoryUsage heapMemoryUsage) {
        heapMemoryUsage.indicesMappingSize().forEach((index, indexMappingSize) -> {
            // drop update for unknown index
            // scenario: index created but CS applier thread is lagging behind
            // eventually it converges to correct state since index nodes re-sending updates
            if (indicesMemoryMetrics.containsKey(index)) {
                IndexMemoryMetrics indexMemoryMetrics = indicesMemoryMetrics.get(index);
                indexMemoryMetrics.update(
                    indexMappingSize.sizeInBytes(),
                    heapMemoryUsage.publicationSeqNo(),
                    indexMappingSize.metricShardNodeId()
                );
            }
        });
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
                IndexMemoryMetrics indexMemoryMetrics = new IndexMemoryMetrics(0, MetricQuality.MISSING);

                var shardRouting = getPrimaryShardRouting(event, indexMetadata.getIndex());
                if (shardRouting.assignedToNode()) {
                    indexMemoryMetrics.update(shardRouting.currentNodeId());
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

                // index created
                if (event.indicesCreated().contains(index.getName())) {
                    // EXACT values will be sent by index node
                    indicesMemoryMetrics.put(index, new IndexMemoryMetrics(0, MetricQuality.MISSING));
                }

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
                        indexMemoryMetrics.update(MetricQuality.MINIMUM);
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
                            indexMemoryMetrics.update(newShardRouting.currentNodeId());
                        }
                    }
                }
            }
        }
    }

    private static ShardRouting getPrimaryShardRouting(ClusterChangedEvent event, Index indexMetadata) {
        return event.state().routingTable().shardRoutingTable(indexMetadata.getName(), SENDING_PRIMARY_SHARD_ID).primaryShard();
    }

    static class IndexMemoryMetrics {
        private long sizeInBytes;
        private long seqNo;
        private MetricQuality metricQuality;
        private String metricShardNodeId;   // node id which hosts sending primary 0-shard

        IndexMemoryMetrics(long sizeInBytes, MetricQuality metricQuality) {
            this(sizeInBytes, 0, metricQuality, null);
        }

        IndexMemoryMetrics(long sizeInBytes, long seqNo, MetricQuality metricQuality, String metricShardNodeId) {
            this.sizeInBytes = sizeInBytes;
            this.seqNo = seqNo;
            this.metricQuality = metricQuality;
            this.metricShardNodeId = metricShardNodeId;
        }

        synchronized void update(long sizeInBytes, long seqNo, String metricShardNodeId) {
            assert metricShardNodeId != null;
            // drop messages which were sent from unknown nodes
            // e.g. message late arrival from previously allocated node (where primary 0-shard used to be)
            if (Objects.equals(this.metricShardNodeId, metricShardNodeId)) {
                if (this.seqNo < seqNo) { // handle reordered messages which were sent from the same node
                    this.seqNo = seqNo;
                    this.sizeInBytes = sizeInBytes;
                    this.metricQuality = MetricQuality.EXACT;
                    this.metricShardNodeId = metricShardNodeId;
                }
            }
        }

        private synchronized void update(MetricQuality metricQuality) {
            this.metricQuality = metricQuality;
        }

        private synchronized void update(String metricShardNodeId) {
            this.metricShardNodeId = metricShardNodeId;
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
    }
}
