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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.indexing.slowlog.ShardSlowLogIndexingService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 */
public class ShardIndexingService extends AbstractIndexShardComponent {

    private final ShardSlowLogIndexingService slowLog;

    private final StatsHolder totalStats = new StatsHolder();

    private final CopyOnWriteArrayList<IndexingOperationListener> listeners = new CopyOnWriteArrayList<>();

    private volatile Map<String, StatsHolder> typesStats = ImmutableMap.of();

    @Inject
    public ShardIndexingService(ShardId shardId, @IndexSettings Settings indexSettings, ShardSlowLogIndexingService slowLog) {
        super(shardId, indexSettings);
        this.slowLog = slowLog;
    }

    /**
     * Returns the stats, including type specific stats. If the types are null/0 length, then nothing
     * is returned for them. If they are set, then only types provided will be returned, or
     * <tt>_all</tt> for all types.
     */
    public IndexingStats stats(String... types) {
        IndexingStats.Stats total = totalStats.stats();
        Map<String, IndexingStats.Stats> typesSt = null;
        if (types != null && types.length > 0) {
            typesSt = new HashMap<>(typesStats.size());
            if (types.length == 1 && types[0].equals("_all")) {
                for (Map.Entry<String, StatsHolder> entry : typesStats.entrySet()) {
                    typesSt.put(entry.getKey(), entry.getValue().stats());
                }
            } else {
                for (Map.Entry<String, StatsHolder> entry : typesStats.entrySet()) {
                    if (Regex.simpleMatch(types, entry.getKey())) {
                        typesSt.put(entry.getKey(), entry.getValue().stats());
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

    public Engine.Create preCreate(Engine.Create create) {
        totalStats.indexCurrent.inc();
        typeStats(create.type()).indexCurrent.inc();
        for (IndexingOperationListener listener : listeners) {
            create = listener.preCreate(create);
        }
        return create;
    }

    public void postCreateUnderLock(Engine.Create create) {
        for (IndexingOperationListener listener : listeners) {
            try {
                listener.postCreateUnderLock(create);
            } catch (Exception e) {
                logger.warn("post listener [{}] failed", e, listener);
            }
        }
    }

    public void throttlingActivated() {
        totalStats.setThrottled(true);
    }

    public void throttlingDeactivated() {
        totalStats.setThrottled(false);
    }

    public void postCreate(Engine.Create create) {
        long took = create.endTime() - create.startTime();
        totalStats.indexMetric.inc(took);
        totalStats.indexCurrent.dec();
        StatsHolder typeStats = typeStats(create.type());
        typeStats.indexMetric.inc(took);
        typeStats.indexCurrent.dec();
        slowLog.postCreate(create, took);
        for (IndexingOperationListener listener : listeners) {
            try {
                listener.postCreate(create);
            } catch (Exception e) {
                logger.warn("post listener [{}] failed", e, listener);
            }
        }
    }

    public void postCreate(Engine.Create create, Throwable ex) {
    }

    public Engine.Index preIndex(Engine.Index index) {
        totalStats.indexCurrent.inc();
        typeStats(index.type()).indexCurrent.inc();
        for (IndexingOperationListener listener : listeners) {
            index = listener.preIndex(index);
        }
        return index;
    }

    public void postIndexUnderLock(Engine.Index index) {
        for (IndexingOperationListener listener : listeners) {
            try {
                listener.postIndexUnderLock(index);
            } catch (Exception e) {
                logger.warn("post listener [{}] failed", e, listener);
            }
        }
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
                logger.warn("post listener [{}] failed", e, listener);
            }
        }
    }

    public void postIndex(Engine.Index index, Throwable ex) {
        totalStats.indexCurrent.dec();
        typeStats(index.type()).indexCurrent.dec();
    }

    public Engine.Delete preDelete(Engine.Delete delete) {
        totalStats.deleteCurrent.inc();
        typeStats(delete.type()).deleteCurrent.inc();
        for (IndexingOperationListener listener : listeners) {
            delete = listener.preDelete(delete);
        }
        return delete;
    }

    public void postDeleteUnderLock(Engine.Delete delete) {
        for (IndexingOperationListener listener : listeners) {
            try {
                listener.postDeleteUnderLock(delete);
            } catch (Exception e) {
                logger.warn("post listener [{}] failed", e, listener);
            }
        }
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
                logger.warn("post listener [{}] failed", e, listener);
            }
        }
    }

    public void postDelete(Engine.Delete delete, Throwable ex) {
        totalStats.deleteCurrent.dec();
        typeStats(delete.type()).deleteCurrent.dec();
    }

    public Engine.DeleteByQuery preDeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
        for (IndexingOperationListener listener : listeners) {
            deleteByQuery = listener.preDeleteByQuery(deleteByQuery);
        }
        return deleteByQuery;
    }

    public void postDeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
        for (IndexingOperationListener listener : listeners) {
            listener.postDeleteByQuery(deleteByQuery);
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

    static class StatsHolder {
        public final MeanMetric indexMetric = new MeanMetric();
        public final MeanMetric deleteMetric = new MeanMetric();
        public final CounterMetric indexCurrent = new CounterMetric();
        public final CounterMetric deleteCurrent = new CounterMetric();
        public final CounterMetric noopUpdates = new CounterMetric();
        public final CounterMetric throttleTimeMillisMetric = new CounterMetric();
        volatile boolean isThrottled = false;
        volatile long startOfThrottleMillis;

        public IndexingStats.Stats stats() {
            long currentThrottleMillis = 0;
            if (isThrottled && startOfThrottleMillis != 0) {
                currentThrottleMillis +=  System.currentTimeMillis() - startOfThrottleMillis;
                if (currentThrottleMillis < 0) {
                    //Timeslip must have happened, have to ignore this value
                    currentThrottleMillis = 0;
                }
            }
            return new IndexingStats.Stats(
                    indexMetric.count(), TimeUnit.NANOSECONDS.toMillis(indexMetric.sum()), indexCurrent.count(),
                    deleteMetric.count(), TimeUnit.NANOSECONDS.toMillis(deleteMetric.sum()), deleteCurrent.count(),
                    noopUpdates.count(), isThrottled, TimeUnit.MILLISECONDS.toMillis(throttleTimeMillisMetric.count() + currentThrottleMillis));
        }


        void setThrottled(boolean isThrottled) {
            if (!this.isThrottled && isThrottled) {
                startOfThrottleMillis = System.currentTimeMillis();
            } else if (this.isThrottled && !isThrottled) {
                assert startOfThrottleMillis > 0 : "Bad state of startOfThrottleMillis";
                long throttleTimeMillis = System.currentTimeMillis() - startOfThrottleMillis;
                if (throttleTimeMillis >= 0) {
                    //A timeslip may have occurred but never want to add a negative number
                    throttleTimeMillisMetric.inc(throttleTimeMillis);
                }
            }
            this.isThrottled = isThrottled;
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
