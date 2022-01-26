/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.engine.Engine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

/**
 * Internal class that maintains relevant indexing statistics / metrics.
 * @see IndexShard
 */
final class InternalIndexingStats implements IndexingOperationListener {
    private final StatsHolder totalStats = new StatsHolder();
    private volatile Map<String, StatsHolder> typesStats = emptyMap();

    /**
     * Returns the stats, including type specific stats. If the types are null/0 length, then nothing
     * is returned for them. If they are set, then only types provided will be returned, or
     * {@code _all} for all types.
     */
    IndexingStats stats(boolean isThrottled, long currentThrottleInMillis, String... types) {
        IndexingStats.Stats total = totalStats.stats(isThrottled, currentThrottleInMillis);
        Map<String, IndexingStats.Stats> typesSt = null;
        if (types != null && types.length > 0) {
            typesSt = new HashMap<>(typesStats.size());
            if (types.length == 1 && types[0].equals("_all")) {
                for (Map.Entry<String, StatsHolder> entry : typesStats.entrySet()) {
                    typesSt.put(entry.getKey(), entry.getValue().stats(isThrottled, currentThrottleInMillis));
                }
            } else {
                for (Map.Entry<String, StatsHolder> entry : typesStats.entrySet()) {
                    if (Regex.simpleMatch(types, entry.getKey())) {
                        typesSt.put(entry.getKey(), entry.getValue().stats(isThrottled, currentThrottleInMillis));
                    }
                }
            }
        }
        return new IndexingStats(total, typesSt);
    }

    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
        if (operation.origin().isRecovery() == false) {
            totalStats.indexCurrent.inc();
            typeStats(operation.type()).indexCurrent.inc();
        }
        return operation;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        switch (result.getResultType()) {
            case SUCCESS:
                if (index.origin().isRecovery() == false) {
                    long took = result.getTook();
                    totalStats.indexMetric.inc(took);
                    totalStats.indexCurrent.dec();
                    StatsHolder typeStats = typeStats(index.type());
                    typeStats.indexMetric.inc(took);
                    typeStats.indexCurrent.dec();
                }
                break;
            case FAILURE:
                postIndex(shardId, index, result.getFailure());
                break;
            default:
                throw new IllegalArgumentException("unknown result type: " + result.getResultType());
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        if (index.origin().isRecovery() == false) {
            totalStats.indexCurrent.dec();
            typeStats(index.type()).indexCurrent.dec();
            totalStats.indexFailed.inc();
            typeStats(index.type()).indexFailed.inc();
        }
    }

    @Override
    public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
        if (delete.origin().isRecovery() == false) {
            totalStats.deleteCurrent.inc();
            typeStats(delete.type()).deleteCurrent.inc();
        }
        return delete;

    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        switch (result.getResultType()) {
            case SUCCESS:
                if (delete.origin().isRecovery() == false) {
                    long took = result.getTook();
                    totalStats.deleteMetric.inc(took);
                    totalStats.deleteCurrent.dec();
                    StatsHolder typeStats = typeStats(delete.type());
                    typeStats.deleteMetric.inc(took);
                    typeStats.deleteCurrent.dec();
                }
                break;
            case FAILURE:
                postDelete(shardId, delete, result.getFailure());
                break;
            default:
                throw new IllegalArgumentException("unknown result type: " + result.getResultType());
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
        if (delete.origin().isRecovery() == false) {
            totalStats.deleteCurrent.dec();
            typeStats(delete.type()).deleteCurrent.dec();
        }
    }

    public void noopUpdate(String type) {
        totalStats.noopUpdates.inc();
        typeStats(type).noopUpdates.inc();
    }

    private StatsHolder typeStats(String type) {
        StatsHolder stats = typesStats.get(type);
        if (stats == null) {
            synchronized (this) {
                stats = typesStats.get(type);
                if (stats == null) {
                    stats = new StatsHolder();
                    typesStats = MapBuilder.newMapBuilder(typesStats).put(type, stats).immutableMap();
                }
            }
        }
        return stats;
    }

    static class StatsHolder {
        private final MeanMetric indexMetric = new MeanMetric();
        private final MeanMetric deleteMetric = new MeanMetric();
        private final CounterMetric indexCurrent = new CounterMetric();
        private final CounterMetric indexFailed = new CounterMetric();
        private final CounterMetric deleteCurrent = new CounterMetric();
        private final CounterMetric noopUpdates = new CounterMetric();

        IndexingStats.Stats stats(boolean isThrottled, long currentThrottleMillis) {
            return new IndexingStats.Stats(
                indexMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(indexMetric.sum()),
                indexCurrent.count(),
                indexFailed.count(),
                deleteMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(deleteMetric.sum()),
                deleteCurrent.count(),
                noopUpdates.count(),
                isThrottled,
                TimeUnit.MILLISECONDS.toMillis(currentThrottleMillis)
            );
        }
    }
}
