/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.engine.IndexEngine.LiveVersionMapArchiveType;
import org.elasticsearch.xpack.stateless.engine.StatelessLiveVersionMapArchiveApi.CachedStats;

import java.io.Closeable;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Node level gauges of the size of the live version map archives of the node's indexing shards. Each gauge sums (or takes the max
 * of) the values the archives cached during their last {@code getRamBytesUsed()} walk, which the indexing memory and disk controllers
 * trigger at least every few seconds, so exporting never walks an archive or takes its lock.
 * <p>
 * The metric names carry the archive variant ({@code per_generation} or {@code by_reference}), and only the variant this node is
 * configured with is registered, so the two variants never share a series. The number of maps held is not comparable across variants:
 * the per-generation archive merges the maps of a generation and drops empty ones, the by-reference archive keeps one per hand-off.
 */
public class LiveVersionMapArchiveMetrics implements Closeable {

    private final List<AutoCloseable> gauges;

    public LiveVersionMapArchiveMetrics(MeterRegistry meterRegistry, IndicesService indicesService, LiveVersionMapArchiveType type) {
        final String prefix = "es.live_version_map.archive." + type.name().toLowerCase(Locale.ROOT);
        final String variant = switch (type) {
            case PER_GENERATION -> "per-generation archive: one map per commit generation with data, merged";
            case BY_REFERENCE -> "by-reference archive: one map per refresh hand-off, not merged";
        };
        final Function<IndexShard, CachedStats> shardStats = LiveVersionMapArchiveMetrics::cachedStats;
        this.gauges = List.of(
            meterRegistry.registerLongAsyncGauge(
                prefix + ".maps.current",
                "Live version map lookups held by the archives of all indexing shards on this node, as of the last memory controller "
                    + "poll ("
                    + variant
                    + ")",
                "{map}",
                () -> sumMaps(indicesService, shardStats)
            ),
            meterRegistry.registerLongAsyncGauge(
                prefix + ".usage.current",
                "Bytes accounted to the live version map archives of all indexing shards on this node, as of the last memory controller "
                    + "poll",
                "bytes",
                () -> sumBytes(indicesService, shardStats)
            ),
            meterRegistry.registerLongAsyncGauge(
                prefix + ".maps.shard_max.current",
                "Largest number of live version map lookups held by the archive of a single indexing shard on this node, as of the last "
                    + "memory controller poll ("
                    + variant
                    + ")",
                "{map}",
                () -> maxMaps(indicesService, shardStats)
            )
        );
    }

    /**
     * Reads the cached stats of an indexing shard's archive without blocking: {@code tryWithEngineOrNull} passes {@code null} while an
     * engine reset holds the lock. The engine does not escape the lambda. Shards without an {@link IndexEngine} contribute nothing.
     */
    static CachedStats cachedStats(IndexShard shard) {
        final CachedStats stats = shard.tryWithEngineOrNull(
            engine -> engine instanceof IndexEngine indexEngine ? indexEngine.liveVersionMapArchiveCachedStats() : null
        );
        return stats == null ? CachedStats.ZERO : stats;
    }

    static long sumMaps(Iterable<IndexService> indexServices, Function<IndexShard, CachedStats> shardStats) {
        long sum = 0;
        for (IndexService indexService : indexServices) {
            for (IndexShard shard : indexService) {
                sum += shardStats.apply(shard).mapsHeld();
            }
        }
        return sum;
    }

    static long sumBytes(Iterable<IndexService> indexServices, Function<IndexShard, CachedStats> shardStats) {
        long sum = 0;
        for (IndexService indexService : indexServices) {
            for (IndexShard shard : indexService) {
                sum += shardStats.apply(shard).ramBytesUsed();
            }
        }
        return sum;
    }

    static long maxMaps(Iterable<IndexService> indexServices, Function<IndexShard, CachedStats> shardStats) {
        long max = 0;
        for (IndexService indexService : indexServices) {
            for (IndexShard shard : indexService) {
                max = Math.max(max, shardStats.apply(shard).mapsHeld());
            }
        }
        return max;
    }

    @Override
    public void close() {
        // Closing a gauge does not throw in practice; keep close total.
        for (AutoCloseable gauge : gauges) {
            try {
                gauge.close();
            } catch (Exception e) {
                assert false : e;
            }
        }
    }
}
