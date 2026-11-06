/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.codec.profiler.Counter;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;

/**
 * Decorator that counts the spans of documents an ordinal range steps over because the skip index shows that no
 * ordinal in them can match. A reader builds it only when it has a recorder and an ordinal skip index. Otherwise the
 * reader uses {@link IntervalSkipObserver#NONE}. The navigation time is measured in {@link OrdinalRangePresence}.
 */
final class ProfiledIntervalSkipObserver implements IntervalSkipObserver {

    static final CounterMetric ORDINAL_SKIP_INTERVALS_REJECTED = CounterMetric.of(
        "ColumNAR",
        "ordinal_skip",
        "intervals_rejected",
        Metric.Unit.COUNT,
        "Spans of documents an ordinal range stepped over because the skip index showed no ordinal in them can match."
    );

    static final CounterMetric ORDINAL_SKIP_DOCID_SPAN = CounterMetric.of(
        "ColumNAR",
        "ordinal_skip",
        "docid_span",
        Metric.Unit.DOC_IDS,
        "Document ids covered by the spans an ordinal range stepped over, whether or not each id holds a value."
    );

    static final TimerMetric ORDINAL_SKIP_NAVIGATION_TIME = TimerMetric.of(
        "ColumNAR",
        "ordinal_skip",
        "navigation_time_nanos",
        TimingPolicy.ADAPTIVE,
        "Elapsed wall-clock time spent stepping over spans of an ordinal range, an estimate from a sample of the calls once there are "
            + "many. It includes the advances of the underlying iterator and any wait for data. It is the cost of the navigation, "
            + "not the time it saved. It is not CPU time."
    );

    private final Counter intervals;
    private final Counter span;

    ProfiledIntervalSkipObserver(MetricRecorder metrics) {
        this.intervals = metrics.counter(ORDINAL_SKIP_INTERVALS_REJECTED);
        this.span = metrics.counter(ORDINAL_SKIP_DOCID_SPAN);
    }

    @Override
    public void rejected(int fromDoc, int toDocExclusive) {
        intervals.add(1);
        span.add((long) toDocExclusive - fromDoc);
    }
}
