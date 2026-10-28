/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

public class TookMetricsTests extends ESTestCase {
    public void testCounters() {
        Counters counters = new Counters();
        TookMetrics metrics = new TookMetrics();
        long[] boundaries = new long[] {
            10,
            100,
            TookMetrics.ONE_SECOND,
            TookMetrics.TEN_SECONDS,
            TookMetrics.ONE_MINUTE,
            TookMetrics.TEN_MINUTES,
            TookMetrics.ONE_HOUR,
            TookMetrics.TEN_HOURS,
            TookMetrics.ONE_DAY };
        for (int i = 0; i <= boundaries.length; i++) {
            long min = i == 0 ? 0 : boundaries[i - 1];
            long max = i == boundaries.length ? Long.MAX_VALUE : boundaries[i];
            for (int count = 0; count < i + 10; count++) {
                metrics.count(randomLongBetween(min, max - 1));
            }
        }
        metrics.counters("took.", counters);
        assertMap(
            counters.toNestedMap(),
            matchesMap().entry(
                "took",
                matchesMap().entry("lt_10ms", 10L)
                    .entry("lt_100ms", 11L)
                    .entry("lt_1s", 12L)
                    .entry("lt_10s", 13L)
                    .entry("lt_1m", 14L)
                    .entry("lt_10m", 15L)
                    .entry("lt_1h", 16L)
                    .entry("lt_10h", 17L)
                    .entry("lt_1d", 18L)
                    .entry("gt_1d", 19L)
            )
        );
    }
}
