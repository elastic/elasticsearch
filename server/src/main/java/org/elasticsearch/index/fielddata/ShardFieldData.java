/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsAccounting;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class ShardFieldData implements IndexFieldDataCache.Listener {

    private final CounterMetric evictionsMetric = new CounterMetric();
    private final CounterMetric totalMetric = new CounterMetric();
    private final ConcurrentMap<String, CounterMetric> perFieldTotals = ConcurrentCollections.newConcurrentMap();
    private final CounterMetric buildTime = new CounterMetric();
    private final ConcurrentMap<String, GlobalOrdinalFieldStats> perFieldGlobalOrdinalStats = ConcurrentCollections.newConcurrentMap();

    public FieldDataStats stats(String... fields) {
        Map<String, Long> fieldTotals = null;
        Map<String, FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats> fieldGlobalOrdinalsStats = null;
        if (CollectionUtils.isEmpty(fields) == false) {
            fieldTotals = new HashMap<>();
            for (Map.Entry<String, CounterMetric> entry : perFieldTotals.entrySet()) {
                if (Regex.simpleMatch(fields, entry.getKey())) {
                    fieldTotals.put(entry.getKey(), entry.getValue().count());
                }
            }
            for (var entry : perFieldGlobalOrdinalStats.entrySet()) {
                if (Regex.simpleMatch(fields, entry.getKey())) {
                    if (fieldGlobalOrdinalsStats == null) {
                        fieldGlobalOrdinalsStats = new HashMap<>();
                    }
                    fieldGlobalOrdinalsStats.put(
                        entry.getKey(),
                        new FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats(
                            entry.getValue().totalBuildTime.count(),
                            entry.getValue().valueCount.get()
                        )
                    );
                }
            }
        }
        return new FieldDataStats(
            totalMetric.count(),
            evictionsMetric.count(),
            fieldTotals == null ? null : new FieldMemoryStats(fieldTotals),
            new FieldDataStats.GlobalOrdinalsStats(buildTime.count(), fieldGlobalOrdinalsStats)
        );
    }

    @Override
    public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
        totalMetric.inc(ramUsage.ramBytesUsed());
        CounterMetric total = perFieldTotals.get(fieldName);
        if (total != null) {
            total.inc(ramUsage.ramBytesUsed());
        } else {
            total = new CounterMetric();
            total.inc(ramUsage.ramBytesUsed());
            CounterMetric prev = perFieldTotals.putIfAbsent(fieldName, total);
            if (prev != null) {
                prev.inc(ramUsage.ramBytesUsed());
            }
        }
    }

    @Override
    public void onCache(ShardId shardId, String fieldName, GlobalOrdinalsAccounting info) {
        buildTime.inc(info.getBuildingTime().millis());
        perFieldGlobalOrdinalStats.compute(fieldName, (f, globalOrdinalFieldStats) -> {
            if (globalOrdinalFieldStats == null) {
                globalOrdinalFieldStats = new GlobalOrdinalFieldStats();
            }
            globalOrdinalFieldStats.totalBuildTime.inc(info.getBuildingTime().millis());
            if (globalOrdinalFieldStats.valueCount.get() < info.getValueCount()) {
                globalOrdinalFieldStats.valueCount.set(info.getValueCount());
            }
            return globalOrdinalFieldStats;
        });
    }

    @Override
    public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
        if (wasEvicted) {
            evictionsMetric.inc();
        }
        if (sizeInBytes != -1) {
            totalMetric.dec(sizeInBytes);

            CounterMetric total = perFieldTotals.get(fieldName);
            if (total != null) {
                total.dec(sizeInBytes);
            }
        }
    }

    static class GlobalOrdinalFieldStats {

        private final CounterMetric totalBuildTime;
        private final AtomicLong valueCount;

        GlobalOrdinalFieldStats() {
            this.totalBuildTime = new CounterMetric();
            this.valueCount = new AtomicLong();
        }
    }
}
