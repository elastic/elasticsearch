/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.metrics.CounterMetric;
import static org.elasticsearch.script.TimeSeriesCounter.SECOND;
import static org.elasticsearch.script.TimeSeriesCounter.MINUTE;
import static org.elasticsearch.script.TimeSeriesCounter.HOUR;

import java.util.function.LongSupplier;

public class ScriptMetrics {
    protected static final int FIFTEEN_SECONDS = 15 * SECOND;
    protected static final int THIRTY_MINUTES = 30 * MINUTE;
    protected static final int TWENTY_FOUR_HOURS = 24 * HOUR;

    final CounterMetric compilationLimitTriggered = new CounterMetric();
    final TimeSeriesCounter compilations = timeSeriesCounter();
    final TimeSeriesCounter cacheEvictions = timeSeriesCounter();
    final LongSupplier timeProvider;

    public ScriptMetrics(LongSupplier timeProvider) {
        this.timeProvider = timeProvider;
    }

    TimeSeriesCounter timeSeriesCounter() {
        return new TimeSeriesCounter(TWENTY_FOUR_HOURS, THIRTY_MINUTES, FIFTEEN_SECONDS);
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

    public ScriptStats stats() {
        return new ScriptStats(compilationsMetric.count(), cacheEvictionsMetric.count(), compilationLimitTriggered.count());
    }

    protected long now() {
        return timeProvider.getAsLong() / 1000;
    }

    public ScriptContextStats stats(String context) {
        long t = now();
        return new ScriptContextStats(
            context,
            compilations.total(),
            cacheEvictions.total(),
            compilationLimitTriggered.count(),
            compilations.timeSeries(t),
            cacheEvictions.timeSeries(t)
        );
    }
}
