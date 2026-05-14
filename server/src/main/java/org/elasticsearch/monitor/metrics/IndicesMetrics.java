/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.store.FieldInfoCachingDirectory;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.getTotalUserIndices;
import static org.elasticsearch.common.component.Lifecycle.State.STARTED;

/**
 * {@link IndicesMetrics} monitors index statistics on an Elasticsearch node and exposes them as metrics
 * through the provided {@link MeterRegistry}. It tracks the current total number of indices, document count, and
 * store size (in bytes) for each index mode.
 */
public class IndicesMetrics extends AbstractLifecycleComponent {
    public static final String USER_INDEX_TOTAL_METRIC_NAME = "es.indices.users.total";
    public static final String FIELD_INFOS_CACHED_CURRENT_METRIC_NAME = "es.indices.field_infos.cached.current";
    public static final String FIELD_INFOS_CURRENT_METRIC_NAME = "es.indices.field_infos.current";
    public static final String MAPPING_FIELDS_CURRENT_METRIC_NAME = "es.indices.mapping.fields.current";
    private final Logger logger = LogManager.getLogger(IndicesMetrics.class);
    private final MeterRegistry registry;
    private final List<AutoCloseable> metrics = new ArrayList<>();
    private final IndicesStatsCache stateCache;
    private final ClusterService clusterService;
    private final SystemIndices systemIndices;

    public IndicesMetrics(
        MeterRegistry meterRegistry,
        IndicesService indicesService,
        TimeValue metricsInterval,
        ClusterService clusterService,
        SystemIndices systemIndices
    ) {
        this.registry = meterRegistry;
        // Use half of the update interval to ensure that results aren't cached across updates,
        // while preventing the cache from expiring when reading different gauges within the same update.
        var cacheExpiry = new TimeValue(metricsInterval.getMillis() / 2);
        this.stateCache = new IndicesStatsCache(indicesService, cacheExpiry);
        this.clusterService = clusterService;
        this.systemIndices = systemIndices;
    }

    @FixForMultiProject(description = "When multi-project arrives we should add project ID to the USER_INDEX_TOTAL_METRIC_NAME.")
    private static List<AutoCloseable> registerAsyncMetrics(
        MeterRegistry registry,
        IndicesStatsCache cache,
        ClusterService clusterService,
        SystemIndices systemIndices
    ) {
        final int TOTAL_METRICS = 95;
        List<AutoCloseable> metrics = new ArrayList<>(TOTAL_METRICS);
        for (IndexMode indexMode : IndexMode.values()) {
            String name = indexMode.getName();
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".total",
                    "total number of " + name + " indices",
                    "unit",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).numIndices)
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".docs.total",
                    "total documents of " + name + " indices",
                    "unit",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).numDocs)
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".size",
                    "total size in bytes of " + name + " indices",
                    "bytes",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).numBytes)
                )
            );
            // query (count, took, failures) - use gauges as shards can be removed
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".query.total",
                    "current queries of " + name + " indices",
                    "unit",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).search.getQueryCount())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".query.time",
                    "current query time of " + name + " indices",
                    "ms",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).search.getQueryTimeInMillis())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".query.failure.total",
                    "current query failures of " + name + " indices",
                    "unit",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).search.getQueryFailure())
                )
            );
            // fetch (count, took, failures) - use gauges as shards can be removed
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".fetch.total",
                    "current fetches of " + name + " indices",
                    "unit",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).search.getFetchCount())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".fetch.time",
                    "current fetch time of " + name + " indices",
                    "ms",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).search.getFetchTimeInMillis())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".fetch.failure.total",
                    "current fetch failures of " + name + " indices",
                    "unit",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).search.getFetchFailure())
                )
            );
            // indexing
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".indexing.total",
                    "current indexing operations of " + name + " indices",
                    "unit",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).indexing.getIndexCount())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".indexing.time",
                    "current indexing time of " + name + " indices",
                    "ms",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).indexing.getIndexTime().millis())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".indexing.failure.total",
                    "current indexing failures of " + name + " indices",
                    "unit",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).indexing.getIndexFailedCount())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".indexing.failure.version_conflict.total",
                    "current indexing failures due to version conflict of " + name + " indices",
                    "unit",
                    diffGauge(() -> cache.getOrRefresh().get(indexMode).indexing.getIndexFailedDueToVersionConflictCount())
                )
            );
        }
        metrics.add(
            registry.registerLongGauge(
                FIELD_INFOS_CACHED_CURRENT_METRIC_NAME,
                "Unique FieldInfo instances retained by the per-shard FieldInfo cache across all shards on this node; "
                    + "deduped count of "
                    + FIELD_INFOS_CURRENT_METRIC_NAME
                    + ", which ideally approaches "
                    + MAPPING_FIELDS_CURRENT_METRIC_NAME,
                "unit",
                () -> new LongWithAttributes(getCachedFieldInfoCount(cache.indicesService))
            )
        );
        metrics.add(
            registry.registerLongGauge(
                FIELD_INFOS_CURRENT_METRIC_NAME,
                "Raw count of FieldInfo instances summed across every segment of every shard on this node, before " + "deduplication",
                "unit",
                () -> new LongWithAttributes(getTotalLuceneFieldCount(cache.indicesService))
            )
        );
        metrics.add(
            registry.registerLongGauge(
                MAPPING_FIELDS_CURRENT_METRIC_NAME,
                "Total fields defined in the index mappings of all shards on this node",
                "unit",
                () -> new LongWithAttributes(getTotalMappingFieldCount(cache.indicesService))
            )
        );
        metrics.add(registry.registerLongGauge(USER_INDEX_TOTAL_METRIC_NAME, "Total number of user indices", "index", () -> {
            if (clusterService.lifecycleState() != STARTED) {
                return null;
            }
            final var clusterState = clusterService.state();
            if (clusterState.clusterRecovered() == false || clusterState.nodes().isLocalNodeElectedMaster() == false) {
                return null;
            }
            return new LongWithAttributes(
                getTotalUserIndices(systemIndices, clusterState.getMetadata().projects().values().iterator().next())
            );
        }));
        assert metrics.size() == TOTAL_METRICS : "total number of metrics has changed";
        return metrics;
    }

    static Supplier<LongWithAttributes> diffGauge(Supplier<Long> currentValue) {
        final AtomicLong counter = new AtomicLong();
        return () -> {
            var curr = currentValue.get();
            long prev = counter.getAndUpdate(v -> Math.max(curr, v));
            return new LongWithAttributes(Math.max(0, curr - prev));
        };
    }

    @Override
    protected void doStart() {
        metrics.addAll(registerAsyncMetrics(registry, stateCache, clusterService, systemIndices));
    }

    @Override
    protected void doStop() {
        stateCache.stopRefreshing();
    }

    @Override
    protected void doClose() throws IOException {
        metrics.forEach(metric -> {
            try {
                metric.close();
            } catch (Exception e) {
                logger.warn("metrics close() method should not throw Exception", e);
            }
        });
    }

    static long getTotalMappingFieldCount(IndicesService indicesService) {
        long count = 0;
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                var mapperService = indexShard.mapperService();
                if (mapperService == null) {
                    continue;
                }

                var lookup = mapperService.mappingLookup();
                if (lookup != null) {
                    count += lookup.getTotalFieldsCount();
                }
            }
        }
        return count;
    }

    static long getTotalLuceneFieldCount(IndicesService indicesService) {
        long count = 0;
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                ShardFieldStats stats = indexShard.getShardFieldStats();
                if (stats != null) {
                    count += stats.totalFields();
                }
            }
        }
        return count;
    }

    static long getCachedFieldInfoCount(IndicesService indicesService) {
        long count = 0;
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                try {
                    FieldInfoCachingDirectory cache = FieldInfoCachingDirectory.unwrap(indexShard.store().directory());
                    if (cache != null) {
                        count += cache.fieldInfoCacheSize();
                    }
                } catch (IllegalIndexShardStateException | AlreadyClosedException ignored) {
                    // shard closed or not ready; skip
                }
            }
        }
        return count;
    }

    static Map<IndexMode, IndexStats> getStatsWithoutCache(IndicesService indicesService) {
        Map<IndexMode, IndexStats> stats = new EnumMap<>(IndexMode.class);
        for (IndexMode mode : IndexMode.values()) {
            stats.put(mode, new IndexStats());
        }
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (indexShard.isSystem()) {
                    continue; // skip system indices
                }
                final ShardRouting shardRouting = indexShard.routingEntry();
                final IndexMode indexMode = indexShard.indexSettings().getMode();
                final IndexStats indexStats = stats.get(indexMode);
                try {
                    if (shardRouting.primary() && shardRouting.recoverySource() == null) {
                        if (shardRouting.shardId().id() == 0) {
                            indexStats.numIndices++;
                        }
                        final DocsStats docStats = indexShard.docStats();
                        indexStats.numDocs += docStats.getCount();
                        indexStats.numBytes += docStats.getTotalSizeInBytes();
                        indexStats.indexing.add(indexShard.indexingStats().getTotal());
                    }
                    indexStats.search.add(indexShard.searchStats().getTotal());
                } catch (IllegalIndexShardStateException | AlreadyClosedException ignored) {
                    // ignored
                }
            }
        }
        return stats;
    }

    private static class IndicesStatsCache extends SingleObjectCache<Map<IndexMode, IndexStats>> {
        private static final Map<IndexMode, IndexStats> MISSING_STATS;
        static {
            MISSING_STATS = new EnumMap<>(IndexMode.class);
            for (IndexMode value : IndexMode.values()) {
                MISSING_STATS.put(value, new IndexStats());
            }
        }

        private boolean refresh;
        private final IndicesService indicesService;

        IndicesStatsCache(IndicesService indicesService, TimeValue interval) {
            super(interval, MISSING_STATS);
            this.indicesService = indicesService;
            this.refresh = true;
        }

        @Override
        protected Map<IndexMode, IndexStats> refresh() {
            return refresh ? getStatsWithoutCache(indicesService) : getNoRefresh();
        }

        @Override
        protected boolean needsRefresh() {
            return getNoRefresh() == MISSING_STATS || super.needsRefresh();
        }

        void stopRefreshing() {
            this.refresh = false;
        }
    }
}
