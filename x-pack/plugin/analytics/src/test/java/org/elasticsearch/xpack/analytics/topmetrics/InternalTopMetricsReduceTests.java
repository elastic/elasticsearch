/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Some explicit and simple tests for reducing {@link InternalTopMetrics}.
 * All of the randomized testing, including randomized reduce testing is
 * in {@link InternalTopMetricsTests}.
 */
public class InternalTopMetricsReduceTests extends ESTestCase {
    public void testAllEmpty() {
        InternalTopMetrics first = buildEmpty();
        InternalTopMetrics reduced = reduce(first, buildEmpty(), buildEmpty(), buildEmpty());
        assertThat(reduced, sameInstance(first));
    }

    public void testFirstEmpty() {
        InternalTopMetrics first = buildEmpty();
        InternalTopMetrics reduced = reduce(first, buildFilled(1, top(SortValue.from(1), 1.0)));
        assertThat(reduced, sameInstance(first));
    }

    public void testManyToReduce() {
        InternalTopMetrics first = buildFilled(1, top(SortValue.from(2.0), randomDouble()));
        InternalTopMetrics min = buildFilled(2, top(SortValue.from(1.0), randomDouble()));
        InternalTopMetrics max = buildFilled(3, top(SortValue.from(7.0), randomDouble()));
        InternalTopMetrics[] metrics = new InternalTopMetrics[] {
                first, max, min, buildEmpty(), buildEmpty(),
        };
        InternalTopMetrics winner = first.getSortOrder() == SortOrder.ASC ? min : max;
        InternalTopMetrics reduced = reduce(metrics);
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getMetricNames(), equalTo(singletonList("test")));
        assertThat(reduced.getSortOrder(), equalTo(first.getSortOrder()));
        assertThat(reduced.getSize(), equalTo(first.getSize()));
        assertThat(reduced.getTopMetrics(), equalTo(winner.getTopMetrics()));
    }

    public void testNonZeroSize() {
        InternalTopMetrics first = buildFilled(SortOrder.DESC, 3, top(SortValue.from(2.0), 1));
        InternalTopMetrics second = buildFilled(2, top(SortValue.from(3.0), 2), top(SortValue.from(1.0), 2));
        InternalTopMetrics third = buildFilled(3, top(SortValue.from(8.0), 4), top(SortValue.from(7.0), 5));
        InternalTopMetrics[] metrics = new InternalTopMetrics[] {
                first, second, third, buildEmpty(), buildEmpty(),
        };
        InternalTopMetrics reduced = reduce(metrics);
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getMetricNames(), equalTo(singletonList("test")));
        assertThat(reduced.getSortOrder(), equalTo(first.getSortOrder()));
        assertThat(reduced.getSize(), equalTo(first.getSize()));
        assertThat(reduced.getTopMetrics(), equalTo(List.of(
                third.getTopMetrics().get(0), third.getTopMetrics().get(1), second.getTopMetrics().get(0))));
    }

    public void testDifferentTypes() {
        InternalTopMetrics doubleMetrics = buildFilled(1, top(SortValue.from(100.0), randomDouble()));
        InternalTopMetrics longMetrics = buildFilled(1, top(SortValue.from(7), randomDouble()));
        InternalTopMetrics reduced = reduce(doubleMetrics, longMetrics);
        // Doubles sort first.
        InternalTopMetrics winner = doubleMetrics.getSortOrder() == SortOrder.ASC ? doubleMetrics : longMetrics;
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getMetricNames(), equalTo(singletonList("test")));
        assertThat(reduced.getSortOrder(), equalTo(doubleMetrics.getSortOrder()));
        assertThat(reduced.getSize(), equalTo(doubleMetrics.getSize()));
        assertThat(reduced.getTopMetrics(), equalTo(winner.getTopMetrics()));
    }

    private InternalTopMetrics buildEmpty() {
        return InternalTopMetrics.buildEmptyAggregation("test", singletonList("test"), null);
    }

    private InternalTopMetrics buildFilled(int size, InternalTopMetrics.TopMetric... metrics) {
        return buildFilled(randomFrom(SortOrder.values()), size, metrics);
    }

    private InternalTopMetrics buildFilled(SortOrder sortOrder, int size, InternalTopMetrics.TopMetric... metrics) {
        return new InternalTopMetrics("test", sortOrder, singletonList("test"), size, Arrays.asList(metrics), null);
    }

    private InternalTopMetrics.TopMetric top(SortValue sortValue, double metricValue) {
        DocValueFormat sortFormat = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN, DocValueFormat.IP);
        DocValueFormat metricFormat = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN, DocValueFormat.IP);
        InternalTopMetrics.MetricValue realMetricValue = new InternalTopMetrics.MetricValue(metricFormat, SortValue.from(metricValue));
        return new InternalTopMetrics.TopMetric(sortFormat, sortValue, singletonList(realMetricValue));
    }

    private InternalTopMetrics reduce(InternalTopMetrics... results) {
        return results[0].reduce(Arrays.asList(results), null);
    }
}
