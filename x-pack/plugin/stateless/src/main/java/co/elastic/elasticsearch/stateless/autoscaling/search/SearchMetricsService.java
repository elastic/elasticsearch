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

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.NodeSearchLoadSnapshot;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.PublishNodeSearchLoadRequest;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.getRankedIndicesBelowThreshold;

/**
 * This service is responsible for receiving raw shard sizes notification from the search nodes
 * and aggregating them into search tier autoscaling metrics.
 */
public class SearchMetricsService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchMetricsService.class);

    /**
     * Samples older than this value will not be considered exact.
     */
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

    /**
     * Search power setting below which we only have 1 replica for all interactive indices
     */
    public static final int SEARCH_POWER_MIN_NO_REPLICATION = 100;

    /**
     * Search power setting at which we want to replicate all interactive indices with a factor of 2
     */
    public static final int SEARCH_POWER_MIN_FULL_REPLICATION = 250;

    private final LongSupplier relativeTimeInNanosSupplier;
    private final MemoryMetricsService memoryMetricsService;

    private volatile boolean shardMetricsInitialized = false;
    private volatile boolean loadMetricsInitialized = false;

    private final ConcurrentMap<Index, IndexProperties> indices = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NodeTimingForShardMetrics> nodeTimingForShardMetrics = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShardId, ShardMetrics> shardMetrics = new ConcurrentHashMap<>();
    private final Map<String, NodeSearchLoad> nodeSearchLoads = new ConcurrentHashMap<>();

    private volatile long lastStaleMetricsCheckTimeNs = Long.MIN_VALUE;
    private volatile long accurateMetricWindowNs;
    private volatile long staleMetricsCheckIntervalNs;
    private volatile int searchPowerMinSetting;
    private volatile int searchPowerMaxSetting;

    public static SearchMetricsService create(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ClusterService clusterService,
        MemoryMetricsService memoryMetricsService
    ) {
        SearchMetricsService service = new SearchMetricsService(clusterSettings, threadPool::relativeTimeInNanos, memoryMetricsService);
        clusterService.addListener(service);
        return service;
    }

    SearchMetricsService(
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeInNanosSupplier,
        MemoryMetricsService memoryMetricsService
    ) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.memoryMetricsService = memoryMetricsService;
        clusterSettings.initializeAndWatch(ACCURATE_METRICS_WINDOW_SETTING, value -> this.accurateMetricWindowNs = value.getNanos());
        clusterSettings.initializeAndWatch(
            STALE_METRICS_CHECK_INTERVAL_SETTING,
            value -> this.staleMetricsCheckIntervalNs = value.getNanos()
        );
        this.searchPowerMinSetting = clusterSettings.get(SEARCH_POWER_MIN_SETTING);
        this.searchPowerMaxSetting = clusterSettings.get(SEARCH_POWER_MAX_SETTING);
        clusterSettings.initializeAndWatch(SEARCH_POWER_SETTING, this::updateSearchPower);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MIN_SETTING, this::updateSearchPowerMin);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MAX_SETTING, this::updateSearchPowerMax);
    }

    void updateSearchPowerMin(Integer spMin) {
        this.searchPowerMinSetting = spMin;
    }

    void updateSearchPowerMax(Integer spMax) {
        this.searchPowerMaxSetting = spMax;
    }

    void updateSearchPower(Integer sp) {
        if (this.searchPowerMinSetting == this.searchPowerMaxSetting) {
            this.searchPowerMinSetting = sp;
            this.searchPowerMaxSetting = sp;
        } else {
            throw new IllegalArgumentException(
                "Updating "
                    + ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey()
                    + " ["
                    + sp
                    + "] while "
                    + ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                    + " ["
                    + this.searchPowerMinSetting
                    + "] and "
                    + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                    + " ["
                    + this.searchPowerMaxSetting
                    + "] are not equal."
            );
        }
    }

    // visible for testing
    public int getSearchPowerMinSetting() {
        return searchPowerMinSetting;
    }

    // visible for testing
    public int getSearchPowerMaxSetting() {
        return searchPowerMaxSetting;
    }

    void processShardSizesRequest(PublishShardSizesRequest request) {
        var currentTimestampNanos = relativeTimeInNanos();
        logger.debug("Received shard sizes {}", request.getShardSizes());
        var nodeMetrics = this.nodeTimingForShardMetrics.computeIfAbsent(request.getNodeId(), unused -> new NodeTimingForShardMetrics());
        nodeMetrics.update(currentTimestampNanos);
        for (var entry : request.getShardSizes().entrySet()) {
            shardMetrics.computeIfAbsent(entry.getKey(), unused -> new ShardMetrics()).update(nodeMetrics, entry.getValue());
        }
    }

    public void processSearchLoadRequest(PublishNodeSearchLoadRequest request) {
        var nodeSearchLoad = nodeSearchLoads.computeIfAbsent(request.getNodeId(), unused -> new NodeSearchLoad(request.getNodeId()));
        nodeSearchLoad.setLatestReadingTo(request.getSearchLoad(), request.getSeqNo());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            shardMetricsInitialized = false;
            loadMetricsInitialized = false;
            indices.clear();
            nodeTimingForShardMetrics.clear();
            shardMetrics.clear();
            nodeSearchLoads.clear();

        } else {
            if (event.metadataChanged() || shardMetricsInitialized == false) {
                var state = event.state();
                indices.keySet().removeIf(index -> state.metadata().hasIndex(index) == false || hasZeroReplicas(index, state));
                shardMetrics.keySet()
                    .removeIf(
                        shardId -> state.metadata().hasIndex(shardId.getIndex()) == false || hasZeroReplicas(shardId.getIndex(), state)
                    );

                Function<ShardId, ShardMetrics> newShardMetricFactory = shardMetricsInitialized
                    ? newIndexShardMetricFactory()
                    : shardId -> new ShardMetrics();

                SortedMap<String, IndexAbstraction> indicesLookup = state.metadata().getIndicesLookup();
                for (IndexMetadata metadata : state.metadata().indices().values()) {
                    int shardCopies = getNumberOfReplicas(metadata);
                    if (shardCopies == 0) {
                        continue;
                    }

                    for (int i = 0; i < metadata.getNumberOfShards(); i++) {
                        shardMetrics.computeIfAbsent(new ShardId(metadata.getIndex(), i), newShardMetricFactory);
                    }

                    IndexAbstraction indexAbstraction = indicesLookup.get(metadata.getIndex().getName());
                    assert indexAbstraction.getType() != IndexAbstraction.Type.ALIAS : "we shouldn't see aliases here";
                    long indexRecency = getRecency(metadata, indexAbstraction);
                    indices.put(
                        metadata.getIndex(),
                        new IndexProperties(
                            metadata.getIndex().getName(),
                            metadata.getNumberOfShards(),
                            shardCopies,
                            metadata.isSystem(),
                            indexAbstraction.getParentDataStream() != null,
                            indexRecency
                        )
                    );
                }
            }
            if (event.nodesChanged() || shardMetricsInitialized == false) {
                var searchNodes = new HashSet<String>();
                for (DiscoveryNode node : event.state().nodes()) {
                    if (node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE)) {
                        searchNodes.add(node.getId());
                    }
                }
                nodeTimingForShardMetrics.keySet().retainAll(searchNodes);
                for (String searchNode : searchNodes) {
                    nodeTimingForShardMetrics.computeIfAbsent(searchNode, unused -> new NodeTimingForShardMetrics());
                }
            }
            shardMetricsInitialized = true;

            if (event.nodesDelta().masterNodeChanged() || loadMetricsInitialized == false) {
                for (DiscoveryNode node : event.state().nodes()) {
                    if (isSearchNode(node)) {
                        nodeSearchLoads.computeIfAbsent(node.getId(), unused -> new NodeSearchLoad(node.getId()));
                    }
                }
                loadMetricsInitialized = true;
            }
            if (event.nodesChanged()) {
                for (DiscoveryNode node : event.nodesDelta().addedNodes()) {
                    if (isSearchNode(node)) {
                        nodeSearchLoads.computeIfAbsent(node.getId(), unused -> new NodeSearchLoad(node.getId()));
                    }
                }
                for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                    if (isSearchNode(removedNode)) {
                        var removedNodeSearchLoad = nodeSearchLoads.get(removedNode.getId());
                        if (removedNodeSearchLoad != null) {
                            removedNodeSearchLoad.setQualityToMinimum();
                        }
                    }
                }
            }
        }
    }

    /**
     * Use the creation date of an index as its recency by default.
     * Check if this is a data stream write index, in this case use {@link Long#MAX_VALUE} so we always order
     * write indices first.
     */
    private static long getRecency(IndexMetadata metadata, IndexAbstraction indexAbstraction) {
        DataStream parentDataStream = indexAbstraction.getParentDataStream();
        if (parentDataStream != null && metadata.getIndex().equals(parentDataStream.getWriteIndex())) {
            return Long.MAX_VALUE;
        }
        return metadata.getCreationDate();
    }

    /**
     * Creates a metric factory for a newly created index.
     * It is initialized with a fake node so that metric quality stays exact when new/empty index is created
     */
    private Function<ShardId, ShardMetrics> newIndexShardMetricFactory() {
        var nodeMetrics = new NodeTimingForShardMetrics();
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

        boolean shardCopiesExact = shardMetricsInitialized;
        int maxShardCopies = 0;
        boolean dataSizeExact = shardMetricsInitialized;
        long maxInteractiveDataSizeInBytes = 0;
        long totalInteractiveDataSizeInBytes = 0;
        long totalDataSizeInBytes = 0;
        boolean checkStaleMetrics = currentTimestampNanos - staleMetricsCheckIntervalNs > lastStaleMetricsCheckTimeNs;
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
                    maxInteractiveDataSizeInBytes = Math.max(maxInteractiveDataSizeInBytes, metrics.shardSize.interactiveSizeInBytes());
                    totalInteractiveDataSizeInBytes += metrics.shardSize.interactiveSizeInBytes();
                    totalDataSizeInBytes += metrics.shardSize.totalSizeInBytes();
                }
            }
        }

        var nodeLoadIterator = nodeSearchLoads.entrySet().iterator();
        var searchLoads = new ArrayList<NodeSearchLoadSnapshot>();
        while (nodeLoadIterator.hasNext()) {
            var nodeSearchStatsEntry = nodeLoadIterator.next();
            var value = nodeSearchStatsEntry.getValue();
            if (value.isStale()) {
                nodeLoadIterator.remove();
            } else {
                searchLoads.add(value.getSearchLoadSnapshot());
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
            ),
            Collections.unmodifiableList(searchLoads)
        );
    }

    /**
     * Stores timeStamp information used to determine metric quality for ShardMetrics.
     */
    private class NodeTimingForShardMetrics {
        private long timestamp = relativeTimeInNanos() - accurateMetricWindowNs - 1;

        private synchronized void update(long currentTimestamp) {
            this.timestamp = currentTimestamp;
        }

        private synchronized boolean isOutdated(long currentTimestampNanos) {
            return currentTimestampNanos - timestamp >= accurateMetricWindowNs;
        }
    }

    private class ShardMetrics {
        private NodeTimingForShardMetrics sourceNode = null;
        private ShardSize shardSize = ZERO_SHARD_SIZE;

        private synchronized void update(NodeTimingForShardMetrics sourceNode, ShardSize shardSize) {
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

    class NodeSearchLoad {
        private final String nodeId;
        private double searchLoad;
        private long latestSampleTimeInNanos;
        private long maxSeqNo = Long.MIN_VALUE;
        private MetricQuality quality = MetricQuality.MISSING;

        NodeSearchLoad(String nodeId) {
            this.nodeId = nodeId;
        }

        synchronized void setLatestReadingTo(double searchLoad, long metricSeqNo) {
            if (metricSeqNo > maxSeqNo) {
                this.searchLoad = searchLoad;
                this.quality = MetricQuality.EXACT;
                this.latestSampleTimeInNanos = relativeTimeInNanos();
                this.maxSeqNo = metricSeqNo;
            }
        }

        synchronized void setQualityToMinimum() {
            this.quality = MetricQuality.MINIMUM;
        }

        synchronized NodeSearchLoadSnapshot getSearchLoadSnapshot() {
            if (quality == MetricQuality.EXACT && timeSinceLastSampleInNanos() >= accurateMetricWindowNs) {
                quality = MetricQuality.MINIMUM;
            }
            return new NodeSearchLoadSnapshot(nodeId, searchLoad, quality);
        }

        synchronized boolean isStale() {
            return timeSinceLastSampleInNanos() >= staleMetricsCheckIntervalNs;
        }

        private long timeSinceLastSampleInNanos() {
            return relativeTimeInNanos() - latestSampleTimeInNanos;
        }
    }

    private long relativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    record IndexProperties(String name, int shards, int replicas, boolean isSystem, boolean isDataStream, long recency) {}

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
    ConcurrentMap<Index, IndexProperties> getIndices() {
        return indices;
    }

    ConcurrentMap<String, NodeTimingForShardMetrics> getNodeTimingForShardMetrics() {
        return nodeTimingForShardMetrics;
    }

    ConcurrentMap<ShardId, ShardMetrics> getShardMetrics() {
        return shardMetrics;
    }

    private static boolean isSearchNode(DiscoveryNode node) {
        return node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE);
    }

    record IndexRankingProperties(IndexProperties indexProperties, long interactiveSize) {}

    Map<Integer, List<String>> getNumberOfReplicaChanges() {
        Map<Integer, List<String>> numReplicaChanges = new HashMap<>(2);
        if (searchPowerMinSetting < SEARCH_POWER_MIN_NO_REPLICATION) {
            for (Map.Entry<Index, IndexProperties> entry : indices.entrySet()) {
                Index index = entry.getKey();
                IndexProperties settings = entry.getValue();
                if (settings.replicas != 1) {
                    setNumReplicasForIndex(index.getName(), 1, numReplicaChanges);
                }
            }
        } else if (searchPowerMinSetting >= SEARCH_POWER_MIN_FULL_REPLICATION) {
            for (Map.Entry<Index, IndexProperties> entry : indices.entrySet()) {
                Index index = entry.getKey();
                IndexProperties indexProperties = entry.getValue();
                boolean indexInteractive = false;
                for (int i = 0; i < indexProperties.shards; i++) {
                    ShardMetrics shardMetrics = this.shardMetrics.get(new ShardId(index, i));
                    if (shardMetrics == null) {
                        // move to the next index if shard metrics for the current index are not found: they could have been removed
                        // because the index has been removed, or because it has now zero replicas.
                        break;
                    }
                    if (shardMetrics.shardSize.interactiveSizeInBytes() > 0) {
                        indexInteractive = true;
                        break;
                    }
                }
                if (indexInteractive) {
                    if (indexProperties.replicas != 2) {
                        setNumReplicasForIndex(index.getName(), 2, numReplicaChanges);
                    }
                } else {
                    if (indexProperties.replicas != 1) {
                        setNumReplicasForIndex(index.getName(), 1, numReplicaChanges);
                    }
                }
            }
        } else {
            // search power should be between 100 and 250 here
            long allIndicesInteractiveSize = 0;
            List<IndexRankingProperties> rankingProperties = new ArrayList<>();
            for (Map.Entry<Index, IndexProperties> entry : indices.entrySet()) {
                Index index = entry.getKey();
                IndexProperties indexProperties = entry.getValue();
                long totalIndexInteractiveSize = 0;
                boolean indexSizeValid = true;
                for (int i = 0; i < indexProperties.shards; i++) {
                    ShardMetrics shardMetrics = this.shardMetrics.get(new ShardId(index, i));
                    if (shardMetrics == null) {
                        // move to the next index if shard metrics for the current index are not found: they could have been removed
                        // because the index has been removed, or because it has now zero replicas.
                        indexSizeValid = false;
                        break;
                    }
                    totalIndexInteractiveSize += shardMetrics.shardSize.interactiveSizeInBytes();
                }
                if (indexSizeValid) {
                    rankingProperties.add(new IndexRankingProperties(indexProperties, totalIndexInteractiveSize));
                    allIndicesInteractiveSize += totalIndexInteractiveSize;
                } else {
                    // TODO if we detect an index with invalid shard metrics, should we reset its replica setting to 1 or
                    // trust that it will get removed from state and this.indices map in an upcoming cluster state update?
                }
            }
            final long threshold = allIndicesInteractiveSize * (searchPowerMinSetting - SEARCH_POWER_MIN_NO_REPLICATION)
                / (SEARCH_POWER_MIN_FULL_REPLICATION - SEARCH_POWER_MIN_NO_REPLICATION);
            Set<String> twoReplicaEligibleIndices = getRankedIndicesBelowThreshold(rankingProperties, threshold);
            for (var rankedIndex : rankingProperties) {
                String indexName = rankedIndex.indexProperties.name;
                int replicas = rankedIndex.indexProperties.replicas;
                if (rankedIndex.interactiveSize > 0 && twoReplicaEligibleIndices.contains(indexName)) {
                    if (replicas != 2) {
                        setNumReplicasForIndex(indexName, 2, numReplicaChanges);
                    }
                } else {
                    if (replicas != 1) {
                        setNumReplicasForIndex(indexName, 1, numReplicaChanges);
                    }
                }
            }
        }
        return numReplicaChanges;
    }

    private static void setNumReplicasForIndex(String index, int numReplicas, Map<Integer, List<String>> numReplicaChanges) {
        numReplicaChanges.compute(numReplicas, (integer, strings) -> {
            if (strings == null) {
                strings = new ArrayList<>();
            }
            strings.add(index);
            return strings;
        });
    }
}
