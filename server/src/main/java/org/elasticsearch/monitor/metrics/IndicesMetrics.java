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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * {@link IndicesMetrics} monitors index statistics on an Elasticsearch node and exposes them as metrics
 * through the provided {@link MeterRegistry}. It tracks the current total number of indices, document count, and
 * store size (in bytes) for each index mode.
 */
public class IndicesMetrics extends AbstractLifecycleComponent {
    private final Logger logger = LogManager.getLogger(IndicesMetrics.class);
    private final MeterRegistry registry;
    private final List<AutoCloseable> metrics = new ArrayList<>();
    private final IndicesStatsCache stateCache;

    public IndicesMetrics(MeterRegistry meterRegistry, IndicesService indicesService, TimeValue metricsInterval) {
        this.registry = meterRegistry;
        // Use half of the update interval to ensure that results aren't cached across updates,
        // while preventing the cache from expiring when reading different gauges within the same update.
        var cacheExpiry = new TimeValue(metricsInterval.getMillis() / 2);
        this.stateCache = new IndicesStatsCache(indicesService, cacheExpiry);
    }

    private static List<AutoCloseable> registerAsyncMetrics(MeterRegistry registry, IndicesStatsCache cache) {
        final int TOTAL_METRICS = 36;
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
                    "total queries of " + name + " indices",
                    "unit",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).search.getQueryCount())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".query.time",
                    "total query time of " + name + " indices",
                    "ms",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).search.getQueryTimeInMillis())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".query.failure.total",
                    "total query failures of " + name + " indices",
                    "unit",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).search.getQueryFailure())
                )
            );
            // fetch (count, took, failures) - use gauges as shards can be removed
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".fetch.total",
                    "total fetches of " + name + " indices",
                    "unit",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).search.getFetchCount())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".fetch.time",
                    "total fetch time of " + name + " indices",
                    "ms",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).search.getFetchTimeInMillis())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".fetch.failure.total",
                    "total fetch failures of " + name + " indices",
                    "unit",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).search.getFetchFailure())
                )
            );
            // indexing
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".indexing.total",
                    "total indexing operations of " + name + " indices",
                    "unit",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).indexing.getIndexCount())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".indexing.time",
                    "total indexing time of " + name + " indices",
                    "ms",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).indexing.getIndexTime().millis())
                )
            );
            metrics.add(
                registry.registerLongGauge(
                    "es.indices." + name + ".indexing.failure.total",
                    "total indexing failures of " + name + " indices",
                    "unit",
                    () -> new LongWithAttributes(cache.getOrRefresh().get(indexMode).indexing.getIndexFailedCount())
                )
            );
        }
        assert metrics.size() == TOTAL_METRICS : "total number of metrics has changed";
        return metrics;
    }

    @Override
    protected void doStart() {
        metrics.addAll(registerAsyncMetrics(registry, stateCache));
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

    static class IndexStats {
        int numIndices = 0;
        long numDocs = 0;
        long numBytes = 0;
        SearchStats.Stats search = new SearchStats().getTotal();
        IndexingStats.Stats indexing = new IndexingStats().getTotal();
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

        private Map<IndexMode, IndexStats> internalGetIndicesStats() {
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
                    if (shardRouting.primary() == false) {
                        continue; // count primaries only
                    }
                    if (shardRouting.recoverySource() != null) {
                        continue; // exclude relocating shards
                    }
                    final IndexMode indexMode = indexShard.indexSettings().getMode();
                    final IndexStats indexStats = stats.get(indexMode);
                    if (shardRouting.shardId().id() == 0) {
                        indexStats.numIndices++;
                    }
                    try {
                        indexStats.numDocs += indexShard.commitStats().getNumDocs();
                        indexStats.numBytes += indexShard.storeStats().sizeInBytes();
                        indexStats.search.add(indexShard.searchStats().getTotal());
                        indexStats.indexing.add(indexShard.indexingStats().getTotal());
                    } catch (IllegalIndexShardStateException | AlreadyClosedException ignored) {
                        // ignored
                    }
                }
            }
            return stats;
        }

        @Override
        protected Map<IndexMode, IndexStats> refresh() {
            return refresh ? internalGetIndicesStats() : getNoRefresh();
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
