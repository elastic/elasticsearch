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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * This service is responsible for receiving raw shard sizes notification from the search nodes
 * and aggregating them into search tier autoscaling metrics.
 */
public class SearchMetricsService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchMetricsService.class);

    public static final Setting<TimeValue> ACCURATE_METRICS_WINDOW_SETTING = Setting.timeSetting(
        "serverless.autoscaling.search_metrics.accurate_window",
        TimeValue.timeValueSeconds(90),
        TimeValue.timeValueSeconds(3),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final LongSupplier relativeTimeInNanosSupplier;
    private final MemoryMetricsService memoryMetricsService;

    private volatile boolean initialized = false;
    private final ConcurrentMap<Index, IndexShardsSettings> indices = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NodeMetrics> nodeMetrics = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShardId, ShardMetrics> shardMetrics = new ConcurrentHashMap<>();

    private volatile long metricTimeoutNanos;

    public static SearchMetricsService create(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ClusterService clusterService,
        MemoryMetricsService memoryMetricsService
    ) {
        var service = new SearchMetricsService(clusterSettings, threadPool, memoryMetricsService);
        clusterService.addListener(service);
        return service;
    }

    public SearchMetricsService(ClusterSettings clusterSettings, ThreadPool threadPool, MemoryMetricsService memoryMetricsService) {
        this(clusterSettings, threadPool::relativeTimeInNanos, memoryMetricsService);
    }

    public SearchMetricsService(
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeInNanosSupplier,
        MemoryMetricsService memoryMetricsService
    ) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.memoryMetricsService = memoryMetricsService;
        clusterSettings.initializeAndWatch(ACCURATE_METRICS_WINDOW_SETTING, value -> this.metricTimeoutNanos = value.nanos());
    }

    public void processShardSizesRequest(PublishShardSizesRequest request) {
        var currentTimestampNanos = relativeTimeInNanos();
        var nodeMetrics = this.nodeMetrics.computeIfAbsent(request.getNodeId(), unused -> new NodeMetrics());
        nodeMetrics.update(currentTimestampNanos, request.getSeqNo());
        for (var entry : request.getShardSizes().entrySet()) {
            shardMetrics.computeIfAbsent(entry.getKey(), unused -> new ShardMetrics())
                .update(request.getSeqNo(), nodeMetrics, entry.getValue());
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            initialized = false;
            indices.clear();
            nodeMetrics.clear();
            shardMetrics.clear();
            return;
        }
        if (event.metadataChanged() || initialized == false) {
            var state = event.state();
            indices.keySet().removeIf(index -> state.metadata().hasIndex(index) == false);
            shardMetrics.keySet().removeIf(shardId -> state.metadata().hasIndex(shardId.getIndex()) == false);

            Function<ShardId, ShardMetrics> newShardMetricFactory = initialized
                ? newIndexShardMetricFactory()
                : shardId -> new ShardMetrics();

            for (IndexMetadata metadata : state.metadata().indices().values()) {
                // using min_replicas when auto-expand is used to not prevent possible scale downs
                var shardCopies = metadata.getAutoExpandReplicas() != null && metadata.getAutoExpandReplicas().enabled()
                    ? metadata.getAutoExpandReplicas().minReplicas()
                    : metadata.getNumberOfReplicas();
                indices.put(metadata.getIndex(), new IndexShardsSettings(metadata.getNumberOfShards(), shardCopies));
                for (int i = 0; i < metadata.getNumberOfShards(); i++) {
                    shardMetrics.computeIfAbsent(new ShardId(metadata.getIndex(), i), newShardMetricFactory);
                }
            }
        }
        if (event.nodesChanged() || initialized == false) {
            var searchNodes = new HashSet<String>();
            for (DiscoveryNode node : event.state().nodes()) {
                if (node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE)) {
                    searchNodes.add(node.getId());
                }
            }
            nodeMetrics.keySet().retainAll(searchNodes);
            for (String searchNode : searchNodes) {
                nodeMetrics.computeIfAbsent(searchNode, unused -> new NodeMetrics());
            }
        }
        initialized = true;
    }

    /**
     * Creates a metric factory for a newly created index.
     * It is initialized with a fake node so that metric quality stays exact when new/empty index is created
     */
    private Function<ShardId, ShardMetrics> newIndexShardMetricFactory() {
        var nodeMetrics = new NodeMetrics();
        nodeMetrics.timestamp = relativeTimeInNanos();
        return shardId -> {
            var shardMetrics = new ShardMetrics();
            shardMetrics.sourceNode = nodeMetrics;
            shardMetrics.shardSize = new ShardSize(0, 0);
            return shardMetrics;
        };
    }

    public SearchTierMetrics getSearchTierMetrics() {
        var currentTimestampNanos = relativeTimeInNanos();

        boolean shardCopiesExact = initialized;
        int maxShardCopies = 0;
        boolean dataSizeExact = initialized;
        long maxInteractiveDataSizeInBytes = 0;
        long totalInteractiveDataSizeInBytes = 0;
        long totalDataSizeInBytes = 0;

        for (var entry : indices.entrySet()) {
            var index = entry.getKey();
            var settings = entry.getValue();
            maxShardCopies = Math.max(maxShardCopies, settings.replicas);
            for (int i = 0; i < settings.shards; i++) {
                var metrics = shardMetrics.get(new ShardId(index, i));
                assert metrics != null : "Metric should be initialized by this point";
                synchronized (metrics) {
                    dataSizeExact &= metrics.shardSize != null
                        && metrics.sourceNode != null
                        && metrics.sourceNode.isOutdated(currentTimestampNanos) == false;
                    if (metrics.shardSize != null) {
                        maxInteractiveDataSizeInBytes = Math.max(
                            maxInteractiveDataSizeInBytes,
                            metrics.shardSize.interactiveSizeInBytes() * settings.replicas
                        );
                        totalInteractiveDataSizeInBytes += metrics.shardSize.interactiveSizeInBytes() * settings.replicas;
                        totalDataSizeInBytes += metrics.shardSize.totalSizeInBytes() * settings.replicas;
                    }
                }
            }
        }

        return new SearchTierMetrics(
            memoryMetricsService.getMemoryMetrics(),
            new MaxShardCopies(maxShardCopies, shardCopiesExact ? MetricQuality.EXACT : MetricQuality.MINIMUM),
            new StorageMetrics(
                maxInteractiveDataSizeInBytes,
                totalInteractiveDataSizeInBytes,
                totalDataSizeInBytes,
                dataSizeExact ? MetricQuality.EXACT : MetricQuality.MINIMUM
            )
        );
    }

    private class NodeMetrics {
        private long seqNo = Long.MIN_VALUE;
        private long timestamp = relativeTimeInNanos() - metricTimeoutNanos - 1;

        private synchronized void update(long currentTimestamp, long seqNo) {
            if (this.seqNo < seqNo) {
                this.seqNo = seqNo;
                this.timestamp = currentTimestamp;
            }
        }

        private synchronized boolean isOutdated(long currentTimestampNanos) {
            return currentTimestampNanos - timestamp >= metricTimeoutNanos;
        }
    }

    private class ShardMetrics {
        private long seqNo = Long.MIN_VALUE;
        private NodeMetrics sourceNode = null;
        private ShardSize shardSize = null;

        private synchronized void update(long seqNo, NodeMetrics sourceNode, ShardSize shardSize) {
            if (this.seqNo < seqNo || this.sourceNode != sourceNode) {
                this.seqNo = seqNo;
                this.sourceNode = sourceNode;
                this.shardSize = shardSize;
            }
        }
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    private record IndexShardsSettings(int shards, int replicas) {}

    // visible for testing
    ConcurrentMap<Index, IndexShardsSettings> getIndices() {
        return indices;
    }

    ConcurrentMap<String, NodeMetrics> getNodeMetrics() {
        return nodeMetrics;
    }

    ConcurrentMap<ShardId, ShardMetrics> getShardMetrics() {
        return shardMetrics;
    }
}
