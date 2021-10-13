/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.metrics.CounterMetric;

public class ScriptMetrics {
    final CounterMetric compilationsMetric = new CounterMetric();
    final CounterMetric cacheEvictionsMetric = new CounterMetric();
    final CounterMetric compilationLimitTriggered = new CounterMetric();
    final TimeSeriesCounter compilationsHistory = timeSeriesCounter();
    final TimeSeriesCounter cacheEvictionsHistory = timeSeriesCounter();
    final int[] TIME_PERIODS = { 5 * TimeSeriesCounter.MINUTE, 15 * TimeSeriesCounter.MINUTE, 24 * TimeSeriesCounter.HOUR };

    public static TimeSeriesCounter timeSeriesCounter() {
        return TimeSeriesCounter.nestedCounter(24 * 4, 15 * TimeSeriesCounter.MINUTE, 60, 15);
    }

    public void onCompilation() {
        compilationsMetric.inc();
        compilationsHistory.inc();
    }

    public void onCacheEviction() {
        cacheEvictionsMetric.inc();
        cacheEvictionsHistory.inc();
    }

    public void onCompilationLimit() {
        compilationLimitTriggered.inc();
    }

    public ScriptContextStats stats(String context) {
        long timestamp = compilationsHistory.timestamp();
        int[] compilations = compilationsHistory.counts(timestamp, TIME_PERIODS);
        int[] cacheEvictions = cacheEvictionsHistory.counts(timestamp, TIME_PERIODS);
        return new ScriptContextStats(
            context,
            compilationsMetric.count(),
            cacheEvictionsMetric.count(),
            compilationLimitTriggered.count(),
            new ScriptContextStats.TimeSeries(compilations[0], compilations[1], compilations[2]),
            new ScriptContextStats.TimeSeries(cacheEvictions[0], cacheEvictions[1], cacheEvictions[2])
        );
    }}
