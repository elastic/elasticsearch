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
    final TimeSeriesCounter compilations = new TimeSeriesCounter();
    final TimeSeriesCounter cacheEvictions = new TimeSeriesCounter();
    final LongSupplier timeProvider;

    public ScriptMetrics(LongSupplier timeProvider) {
        this.timeProvider = timeProvider;
    }

    public void onCompilation() {
        compilations.inc(now());
    }

    public void onCacheEviction() {
        cacheEvictions.inc(now());
    }

    public void onCompilationLimit() {
        compilationLimitTriggered.inc();
    }

    protected long now() {
        return timeProvider.getAsLong() / 1000;
    }

    public ScriptStats stats() {
        return new ScriptStats(compilationsMetric.count(), cacheEvictionsMetric.count(), compilationLimitTriggered.count());
    }

    public ScriptContextStats stats(String context) {
        long t = now();
        return new ScriptContextStats(
            context,
            compilations.count(),
            cacheEvictions.count(),
            compilationLimitTriggered.count(),
            compilations.timeSeries(t),
            cacheEvictions.timeSeries(t)
        );
    }
}
