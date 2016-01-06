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

package org.elasticsearch.index.indexing;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

/**
 */
public class ShardIndexingService extends AbstractIndexShardComponent {

    private final IndexingSlowLog slowLog;

    private final StatsHolder totalStats = new StatsHolder();

    private final CopyOnWriteArrayList<IndexingOperationListener> listeners = new CopyOnWriteArrayList<>();

    private volatile Map<String, StatsHolder> typesStats = emptyMap();

    public ShardIndexingService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
        this.slowLog = new IndexingSlowLog(this.indexSettings.getSettings());
    }

    /**
     * Returns the stats, including type specific stats. If the types are null/0 length, then nothing
     * is returned for them. If they are set, then only types provided will be returned, or
     * <tt>_all</tt> for all types.
     */
    public IndexingStats stats(boolean isThrottled, long currentThrottleInMillis, String... types) {
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

    public void addListener(IndexingOperationListener listener) {
        listeners.add(listener);
    }

    public void removeListener(IndexingOperationListener listener) {
        listeners.remove(listener);
    }

    public Engine.Index preIndex(Engine.Index operation) {
        totalStats.indexCurrent.inc();
        typeStats(operation.type()).indexCurrent.inc();
        for (IndexingOperationListener listener : listeners) {
            operation = listener.preIndex(operation);
        }
        return operation;
    }

    public void postIndex(Engine.Index index) {
        long took = index.endTime() - index.startTime();
        totalStats.indexMetric.inc(took);
        totalStats.indexCurrent.dec();
        StatsHolder typeStats = typeStats(index.type());
        typeStats.indexMetric.inc(took);
        typeStats.indexCurrent.dec();
        slowLog.postIndex(index, took);
        for (IndexingOperationListener listener : listeners) {
            try {
                listener.postIndex(index);
            } catch (Exception e) {
                logger.warn("postIndex listener [{}] failed", e, listener);
            }
        }
    }

    public void postIndex(Engine.Index index, Throwable ex) {
        totalStats.indexCurrent.dec();
        typeStats(index.type()).indexCurrent.dec();
        totalStats.indexFailed.inc();
        typeStats(index.type()).indexFailed.inc();
        for (IndexingOperationListener listener : listeners) {
            try {
                listener.postIndex(index, ex);
            } catch (Throwable t) {
                logger.warn("postIndex listener [{}] failed", t, listener);
            }
        }
    }

    public Engine.Delete preDelete(Engine.Delete delete) {
        totalStats.deleteCurrent.inc();
        typeStats(delete.type()).deleteCurrent.inc();
        for (IndexingOperationListener listener : listeners) {
            delete = listener.preDelete(delete);
        }
        return delete;
    }


    public void postDelete(Engine.Delete delete) {
        long took = delete.endTime() - delete.startTime();
        totalStats.deleteMetric.inc(took);
        totalStats.deleteCurrent.dec();
        StatsHolder typeStats = typeStats(delete.type());
        typeStats.deleteMetric.inc(took);
        typeStats.deleteCurrent.dec();
        for (IndexingOperationListener listener : listeners) {
            try {
                listener.postDelete(delete);
            } catch (Exception e) {
                logger.warn("postDelete listener [{}] failed", e, listener);
            }
        }
    }

    public void postDelete(Engine.Delete delete, Throwable ex) {
        totalStats.deleteCurrent.dec();
        typeStats(delete.type()).deleteCurrent.dec();
        for (IndexingOperationListener listener : listeners) {
            try {
                listener. postDelete(delete, ex);
            } catch (Throwable t) {
                logger.warn("postDelete listener [{}] failed", t, listener);
            }
        }
    }

    public void noopUpdate(String type) {
        totalStats.noopUpdates.inc();
        typeStats(type).noopUpdates.inc();
    }

    public void clear() {
        totalStats.clear();
        synchronized (this) {
            if (!typesStats.isEmpty()) {
                MapBuilder<String, StatsHolder> typesStatsBuilder = MapBuilder.newMapBuilder();
                for (Map.Entry<String, StatsHolder> typeStats : typesStats.entrySet()) {
                    if (typeStats.getValue().totalCurrent() > 0) {
                        typeStats.getValue().clear();
                        typesStatsBuilder.put(typeStats.getKey(), typeStats.getValue());
                    }
                }
                typesStats = typesStatsBuilder.immutableMap();
            }
        }
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

    public void onRefreshSettings(Settings settings) {
        slowLog.onRefreshSettings(settings);
    }

    static class StatsHolder {
        public final MeanMetric indexMetric = new MeanMetric();
        public final MeanMetric deleteMetric = new MeanMetric();
        public final CounterMetric indexCurrent = new CounterMetric();
        public final CounterMetric indexFailed = new CounterMetric();
        public final CounterMetric deleteCurrent = new CounterMetric();
        public final CounterMetric noopUpdates = new CounterMetric();

        public IndexingStats.Stats stats(boolean isThrottled, long currentThrottleMillis) {
            return new IndexingStats.Stats(
                    indexMetric.count(), TimeUnit.NANOSECONDS.toMillis(indexMetric.sum()), indexCurrent.count(), indexFailed.count(),
                    deleteMetric.count(), TimeUnit.NANOSECONDS.toMillis(deleteMetric.sum()), deleteCurrent.count(),
                    noopUpdates.count(), isThrottled, TimeUnit.MILLISECONDS.toMillis(currentThrottleMillis));
        }

        public long totalCurrent() {
            return indexCurrent.count() + deleteMetric.count();
        }

        public void clear() {
            indexMetric.clear();
            deleteMetric.clear();
        }


    }
}
