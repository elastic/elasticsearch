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
import static org.hamcrest.Matchers.equalTo;

public class ThresholdBucketerTests extends ESTestCase {

    private static final long[] THRESHOLDS = { 10, 100, 1000 };
    private static final String[] SUFFIXES = { "lt_10", "lt_100", "lt_1k", "gt_1k" };

    public void testCountRoutesValueToCorrectBucket() {
        ThresholdBucketer b = new ThresholdBucketer(THRESHOLDS, SUFFIXES);

        b.count(0);    // lt_10
        b.count(9);    // lt_10
        b.count(10);   // lt_100
        b.count(99);   // lt_100
        b.count(100);  // lt_1k
        b.count(999);  // lt_1k
        b.count(1000); // gt_1k
        b.count(Long.MAX_VALUE); // gt_1k

        Counters counters = new Counters();
        b.counters("pfx.", counters);

        assertMap(
            counters.toNestedMap(),
            matchesMap().entry("pfx", matchesMap().entry("lt_10", 2L).entry("lt_100", 2L).entry("lt_1k", 2L).entry("gt_1k", 2L))
        );
    }

    public void testAllZeroWhenNothingCounted() {
        ThresholdBucketer b = new ThresholdBucketer(THRESHOLDS, SUFFIXES);
        Counters counters = new Counters();
        b.counters("z.", counters);
        for (String suffix : SUFFIXES) {
            assertThat(counters.get("z." + suffix), equalTo(0L));
        }
    }

    public void testConstructorRejectsWrongSuffixLength() {
        expectThrows(IllegalArgumentException.class, () -> new ThresholdBucketer(THRESHOLDS, new String[] { "a", "b" }));
    }

    public void testSingleBucketBucketer() {
        ThresholdBucketer b = new ThresholdBucketer(new long[] {}, new String[] { "all" });
        b.count(0);
        b.count(Long.MAX_VALUE);
        Counters counters = new Counters();
        b.counters("", counters);
        assertThat(counters.get("all"), equalTo(2L));
    }

    public void testTimeLadderBehaviourMatchesTookMetrics() {
        // Verify that ThresholdBucketer with the TookMetrics constants produces the same
        // bucket assignments as TookMetrics itself.
        long[] thresholds = {
            10,
            100,
            TookMetrics.ONE_SECOND,
            TookMetrics.TEN_SECONDS,
            TookMetrics.ONE_MINUTE,
            TookMetrics.TEN_MINUTES,
            TookMetrics.ONE_HOUR,
            TookMetrics.TEN_HOURS,
            TookMetrics.ONE_DAY };
        String[] suffixes = { "lt_10ms", "lt_100ms", "lt_1s", "lt_10s", "lt_1m", "lt_10m", "lt_1h", "lt_10h", "lt_1d", "gt_1d" };
        ThresholdBucketer bucketer = new ThresholdBucketer(thresholds, suffixes);
        TookMetrics tookMetrics = new TookMetrics();

        long[] samples = { 0, 5, 9, 10, 50, 99, 100, 500, 999, 1000, 5000, 9999, 10000, 30000, 59999, 60000, 300000, 599999, 600000 };
        for (long sample : samples) {
            bucketer.count(sample);
            tookMetrics.count(sample);
        }

        Counters bCounters = new Counters();
        bucketer.counters("took.", bCounters);
        Counters tookCounters = new Counters();
        tookMetrics.counters("took.", tookCounters);

        for (String suffix : suffixes) {
            assertThat("bucket mismatch for " + suffix, bCounters.get("took." + suffix), equalTo(tookCounters.get("took." + suffix)));
        }
    }
}
