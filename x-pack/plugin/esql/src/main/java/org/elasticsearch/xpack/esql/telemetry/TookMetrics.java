/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

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

    private static final long[] THRESHOLDS = { 10, 100, ONE_SECOND, TEN_SECONDS, ONE_MINUTE, TEN_MINUTES, ONE_HOUR, TEN_HOURS, ONE_DAY };
    private static final String[] SUFFIXES = {
        "lt_10ms",
        "lt_100ms",
        "lt_1s",
        "lt_10s",
        "lt_1m",
        "lt_10m",
        "lt_1h",
        "lt_10h",
        "lt_1d",
        "gt_1d" };

    private final ThresholdBucketer bucketer = new ThresholdBucketer(THRESHOLDS, SUFFIXES);

    public void count(long tookMillis) {
        bucketer.count(tookMillis);
    }

    public void counters(String prefix, Counters counters) {
        bucketer.counters(prefix, counters);
    }
}
