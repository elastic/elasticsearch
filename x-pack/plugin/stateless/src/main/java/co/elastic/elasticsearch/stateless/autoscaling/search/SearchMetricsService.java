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
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
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
    public static final Setting<TimeValue> STALE_METRICS_CHECK_INTERVAL_SETTING = Setting.timeSetting(
        "serverless.autoscaling.search_metrics.stale_check_interval",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private static final ShardSize ZERO_SHARD_SIZE = new ShardSize(0, 0, PrimaryTermAndGeneration.ZERO);

    private final LongSupplier relativeTimeInNanosSupplier;
    private final MemoryMetricsService memoryMetricsService;

    private volatile boolean initialized = false;
    private final ConcurrentMap<Index, IndexShardsSettings> indices = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NodeMetrics> nodeMetrics = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShardId, ShardMetrics> shardMetrics = new ConcurrentHashMap<>();

    private volatile long metricTimeoutNanos;

    private volatile long lastStaleMetricsCheckTimeNs = Long.MIN_VALUE;
    private volatile TimeValue staleMetricsCheckInterval;

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
        clusterSettings.initializeAndWatch(STALE_METRICS_CHECK_INTERVAL_SETTING, value -> this.staleMetricsCheckInterval = value);
    }

    public void processShardSizesRequest(PublishShardSizesRequest request) {
        var currentTimestampNanos = relativeTimeInNanos();
        logger.debug("Received shard sizes {}", request.getShardSizes());
        var nodeMetrics = this.nodeMetrics.computeIfAbsent(request.getNodeId(), unused -> new NodeMetrics());
        nodeMetrics.update(currentTimestampNanos);
        for (var entry : request.getShardSizes().entrySet()) {
            shardMetrics.computeIfAbsent(entry.getKey(), unused -> new ShardMetrics()).update(nodeMetrics, entry.getValue());
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            initialized = false;
            indices.clear();
            nodeMetrics.clear();
            shardMetrics.clear();
            return;
        }
        if (event.metadataChanged() || initialized == false) {
            var state = event.state();
            indices.keySet().removeIf(index -> state.metadata().hasIndex(index) == false || hasZeroReplicas(index, state));
            shardMetrics.keySet()
                .removeIf(shardId -> state.metadata().hasIndex(shardId.getIndex()) == false || hasZeroReplicas(shardId.getIndex(), state));

            Function<ShardId, ShardMetrics> newShardMetricFactory = initialized
                ? newIndexShardMetricFactory()
                : shardId -> new ShardMetrics();

            for (IndexMetadata metadata : state.metadata().indices().values()) {
                int shardCopies = getNumberOfReplicas(metadata);
                if (shardCopies == 0) {
                    continue;
                }

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
            shardMetrics.shardSize = ZERO_SHARD_SIZE;
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
        boolean checkStaleMetrics = currentTimestampNanos - staleMetricsCheckInterval.nanos() > lastStaleMetricsCheckTimeNs;
        if (checkStaleMetrics) {
            lastStaleMetricsCheckTimeNs = relativeTimeInNanos();
        }

        for (var entry : indices.entrySet()) {
            var index = entry.getKey();
            var settings = entry.getValue();
            maxShardCopies = Math.max(maxShardCopies, settings.replicas);
            for (int i = 0; i < settings.shards; i++) {
                var shardId = new ShardId(index, i);
                var metrics = shardMetrics.get(shardId);
                assert metrics != null : "Metric should be initialized by this point";
                synchronized (metrics) {
                    boolean isOutdated = metrics.sourceNode != null && metrics.sourceNode.isOutdated(currentTimestampNanos);
                    if (checkStaleMetrics && isOutdated) {
                        logger.warn("Storage metrics are stale for shard: {}, {}", shardId, metrics);
                    }
                    dataSizeExact &= metrics.sourceNode != null && isOutdated == false;
                    maxInteractiveDataSizeInBytes = Math.max(
                        maxInteractiveDataSizeInBytes,
                        metrics.shardSize.interactiveSizeInBytes() * settings.replicas
                    );
                    totalInteractiveDataSizeInBytes += metrics.shardSize.interactiveSizeInBytes() * settings.replicas;
                    totalDataSizeInBytes += metrics.shardSize.totalSizeInBytes() * settings.replicas;
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
        private long timestamp = relativeTimeInNanos() - metricTimeoutNanos - 1;

        private synchronized void update(long currentTimestamp) {
            this.timestamp = currentTimestamp;
        }

        private synchronized boolean isOutdated(long currentTimestampNanos) {
            return currentTimestampNanos - timestamp >= metricTimeoutNanos;
        }
    }

    private class ShardMetrics {
        private NodeMetrics sourceNode = null;
        private ShardSize shardSize = ZERO_SHARD_SIZE;

        private synchronized void update(NodeMetrics sourceNode, ShardSize shardSize) {
            if (this.shardSize.primaryTermGeneration().compareTo(shardSize.primaryTermGeneration()) <= 0) {
                this.sourceNode = sourceNode;
                this.shardSize = shardSize;
            }
        }

        @Override
        public String toString() {
            return Strings.format("ShardMetrics{timestamp=%d, shardSize=%s}", sourceNode != null ? sourceNode.timestamp : 0, shardSize);
        }
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    private record IndexShardsSettings(int shards, int replicas) {}

    private static boolean hasZeroReplicas(Index index, ClusterState state) {
        return getNumberOfReplicas(state.metadata().index(index)) == 0;
    }

    private static int getNumberOfReplicas(IndexMetadata metadata) {
        return metadata.getAutoExpandReplicas() != null && metadata.getAutoExpandReplicas().enabled()
            // For indices with auto-expand we expect that at least 1 replica can be allocated
            ? Math.max(metadata.getAutoExpandReplicas().minReplicas(), 1)
            : metadata.getNumberOfReplicas();
    }

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
