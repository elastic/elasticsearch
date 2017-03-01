/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
     * <tt>_all</tt> for all types.
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
        if (!operation.origin().isRecovery()) {
            totalStats.indexCurrent.inc();
            typeStats(operation.type()).indexCurrent.inc();
        }
        return operation;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (result.hasFailure() == false) {
            if (!index.origin().isRecovery()) {
                long took = result.getTook();
                totalStats.indexMetric.inc(took);
                totalStats.indexCurrent.dec();
                StatsHolder typeStats = typeStats(index.type());
                typeStats.indexMetric.inc(took);
                typeStats.indexCurrent.dec();
            }
        } else {
            postIndex(shardId, index, result.getFailure());
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        if (!index.origin().isRecovery()) {
            totalStats.indexCurrent.dec();
            typeStats(index.type()).indexCurrent.dec();
            totalStats.indexFailed.inc();
            typeStats(index.type()).indexFailed.inc();
        }
    }

    @Override
    public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
        if (!delete.origin().isRecovery()) {
            totalStats.deleteCurrent.inc();
            typeStats(delete.type()).deleteCurrent.inc();
        }
        return delete;

    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        if (result.hasFailure() == false) {
            if (!delete.origin().isRecovery()) {
                long took = result.getTook();
                totalStats.deleteMetric.inc(took);
                totalStats.deleteCurrent.dec();
                StatsHolder typeStats = typeStats(delete.type());
                typeStats.deleteMetric.inc(took);
                typeStats.deleteCurrent.dec();
            }
        } else {
            postDelete(shardId, delete, result.getFailure());
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
        if (!delete.origin().isRecovery()) {
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
                indexMetric.count(), TimeUnit.NANOSECONDS.toMillis(indexMetric.sum()), indexCurrent.count(), indexFailed.count(),
                deleteMetric.count(), TimeUnit.NANOSECONDS.toMillis(deleteMetric.sum()), deleteCurrent.count(),
                noopUpdates.count(), isThrottled, TimeUnit.MILLISECONDS.toMillis(currentThrottleMillis));
        }

        void clear() {
            indexMetric.clear();
            deleteMetric.clear();
        }
    }
}
