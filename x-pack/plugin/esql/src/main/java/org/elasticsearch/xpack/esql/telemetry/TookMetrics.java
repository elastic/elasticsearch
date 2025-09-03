/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

public class TookMetrics {
    public static final long ONE_SECOND = TimeValue.timeValueSeconds(1).millis();
    public static final long TEN_SECONDS = TimeValue.timeValueSeconds(10).millis();
    public static final long ONE_MINUTE = TimeValue.timeValueMinutes(1).millis();
    public static final long TEN_MINUTES = TimeValue.timeValueMinutes(10).millis();
    public static final long ONE_HOUR = TimeValue.timeValueHours(1).millis();
    public static final long TEN_HOURS = TimeValue.timeValueHours(10).millis();
    public static final long ONE_DAY = TimeValue.timeValueDays(1).millis();

    private final CounterMetric lessThan10ms = new CounterMetric();
    private final CounterMetric lessThan100ms = new CounterMetric();
    private final CounterMetric lessThan1s = new CounterMetric();
    private final CounterMetric lessThan10s = new CounterMetric();
    private final CounterMetric lessThan1m = new CounterMetric();
    private final CounterMetric lessThan10m = new CounterMetric();
    private final CounterMetric lessThan1h = new CounterMetric();
    private final CounterMetric lessThan10h = new CounterMetric();
    private final CounterMetric lessThan1d = new CounterMetric();
    private final CounterMetric greaterThan1d = new CounterMetric();

    public void count(long tookMillis) {
        if (tookMillis < 10) {
            lessThan10ms.inc();
            return;
        }
        if (tookMillis < 100) {
            lessThan100ms.inc();
            return;
        }
        if (tookMillis < ONE_SECOND) {
            lessThan1s.inc();
            return;
        }
        if (tookMillis < TEN_SECONDS) {
            lessThan10s.inc();
            return;
        }
        if (tookMillis < ONE_MINUTE) {
            lessThan1m.inc();
            return;
        }
        if (tookMillis < TEN_MINUTES) {
            lessThan10m.inc();
            return;
        }
        if (tookMillis < ONE_HOUR) {
            lessThan1h.inc();
            return;
        }
        if (tookMillis < TEN_HOURS) {
            lessThan10h.inc();
            return;
        }
        if (tookMillis < ONE_DAY) {
            lessThan1d.inc();
            return;
        }
        greaterThan1d.inc();
    }

    public void counters(String prefix, Counters counters) {
        counters.inc(prefix + "lt_10ms", lessThan10ms.count());
        counters.inc(prefix + "lt_100ms", lessThan100ms.count());
        counters.inc(prefix + "lt_1s", lessThan1s.count());
        counters.inc(prefix + "lt_10s", lessThan10s.count());
        counters.inc(prefix + "lt_1m", lessThan1m.count());
        counters.inc(prefix + "lt_10m", lessThan10m.count());
        counters.inc(prefix + "lt_1h", lessThan1h.count());
        counters.inc(prefix + "lt_10h", lessThan1h.count());
        counters.inc(prefix + "lt_1d", lessThan1d.count());
        counters.inc(prefix + "gt_1d", greaterThan1d.count());
    }
}
