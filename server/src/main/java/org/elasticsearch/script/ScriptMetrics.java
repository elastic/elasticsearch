/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.metrics.CounterMetric;

import java.util.function.LongSupplier;

public class ScriptMetrics {
    final CounterMetric compilationLimitTriggered = new CounterMetric();
    final TimeSeriesCounter compilations;
    final TimeSeriesCounter cacheEvictions;

    public ScriptMetrics(LongSupplier timeProvider) {
        compilations = new TimeSeriesCounter(timeProvider);
        cacheEvictions = new TimeSeriesCounter(timeProvider);
    }

    public void onCompilation() {
        compilations.inc();
    }

    public void onCacheEviction() {
        cacheEvictions.inc();
    }

    public void onCompilationLimit() {
        compilationLimitTriggered.inc();
    }

    public ScriptStats stats() {
        TimeSeries compilationsTimeSeries = compilations.timeSeries();
        TimeSeries cacheEvictionsTimeSeries = cacheEvictions.timeSeries();
        return new ScriptStats(
            compilationsTimeSeries.total,
            cacheEvictionsTimeSeries.total,
            compilationLimitTriggered.count(),
            compilationsTimeSeries,
            cacheEvictionsTimeSeries
        );
    }

    public ScriptContextStats stats(String context) {
        TimeSeries compilationsTimeSeries = compilations.timeSeries();
        TimeSeries cacheEvictionsTimeSeries = cacheEvictions.timeSeries();
        return new ScriptContextStats(context, compilationLimitTriggered.count(), compilationsTimeSeries, cacheEvictionsTimeSeries);
    }
}
