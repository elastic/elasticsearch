/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.metrics.CounterMetric;
import static org.elasticsearch.script.TimeSeriesCounter.Snapshot;
import static org.elasticsearch.script.TimeSeriesCounter.SECOND;
import static org.elasticsearch.script.TimeSeriesCounter.MINUTE;
import static org.elasticsearch.script.TimeSeriesCounter.HOUR;

import java.util.function.LongSupplier;

public class ScriptMetrics {
    protected static final int FIVE_MINUTES = 5 * MINUTE;
    protected static final int FIFTEEN_MINUTES = 15 * MINUTE;
    protected static final int TWENTY_FOUR_HOURS = 24 * HOUR;

    final CounterMetric compilationLimitTriggered = new CounterMetric();
    final TimeSeriesCounter compilations;
    final TimeSeriesCounter cacheEvictions;

    protected final LongSupplier timeProvider;

    public ScriptMetrics(LongSupplier timeProvider) {
        this.timeProvider = timeProvider;
        this.compilations = timeSeriesCounter();
        this.cacheEvictions = timeSeriesCounter();
    }

    TimeSeriesCounter timeSeriesCounter() {
        return new TimeSeriesCounter(TWENTY_FOUR_HOURS, 30 * MINUTE, 15 * SECOND, timeProvider);
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

    public ScriptContextStats stats(String context) {
        Snapshot compilation = compilations.snapshot(FIVE_MINUTES, FIFTEEN_MINUTES, TWENTY_FOUR_HOURS);
        Snapshot cacheEviction = cacheEvictions.snapshot(compilation);
        return new ScriptContextStats(
            context,
            compilation.total,
            cacheEviction.total,
            compilationLimitTriggered.count(),
            new ScriptContextStats.TimeSeries(compilation.getTime(FIVE_MINUTES), compilation.getTime(FIFTEEN_MINUTES),
                    compilation.getTime(TWENTY_FOUR_HOURS)),
            new ScriptContextStats.TimeSeries(cacheEviction.getTime(FIVE_MINUTES), cacheEviction.getTime(FIFTEEN_MINUTES),
                    cacheEviction.getTime(TWENTY_FOUR_HOURS))
        );
    }}
