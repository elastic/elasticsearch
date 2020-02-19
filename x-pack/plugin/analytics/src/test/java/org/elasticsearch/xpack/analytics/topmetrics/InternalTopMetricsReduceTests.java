/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static java.util.Collections.emptyList;
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
        InternalTopMetrics reduced = reduce(first, buildFilled(SortValue.from(1), 1.0));
        assertThat(reduced, sameInstance(first));
    }

    public void testMany() {
        InternalTopMetrics first = buildFilled(SortValue.from(2.0), randomDouble());
        InternalTopMetrics min = buildFilled(SortValue.from(1.0), randomDouble());
        InternalTopMetrics max = buildFilled(SortValue.from(7.0), randomDouble());
        InternalTopMetrics[] metrics = new InternalTopMetrics[] {
                first, max, min, buildEmpty(), buildEmpty(),
        };
        InternalTopMetrics winner = first.getSortOrder() == SortOrder.ASC ? min : max;
        InternalTopMetrics reduced = reduce(metrics);
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getSortValue(), equalTo(winner.getSortValue()));
        assertThat(reduced.getSortFormat(), equalTo(winner.getSortFormat()));
        assertThat(reduced.getSortOrder(), equalTo(first.getSortOrder()));
        assertThat(reduced.getMetricValue(), equalTo(winner.getMetricValue()));
        assertThat(reduced.getMetricName(), equalTo("test"));
    }

    public void testDifferentTypes() {
        InternalTopMetrics doubleMetrics = buildFilled(SortValue.from(100.0), randomDouble());
        InternalTopMetrics longMetrics = buildFilled(SortValue.from(7), randomDouble());
        InternalTopMetrics reduced = reduce(doubleMetrics, longMetrics);
        // Doubles sort first.
        InternalTopMetrics winner = doubleMetrics.getSortOrder() == SortOrder.ASC ? doubleMetrics : longMetrics; 
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getSortValue(), equalTo(winner.getSortValue()));
        assertThat(reduced.getSortFormat(), equalTo(winner.getSortFormat()));
        assertThat(reduced.getSortOrder(), equalTo(doubleMetrics.getSortOrder()));
        assertThat(reduced.getMetricValue(), equalTo(winner.getMetricValue()));
        assertThat(reduced.getMetricName(), equalTo("test"));
    }

    private InternalTopMetrics buildEmpty() {
        return InternalTopMetrics.buildEmptyAggregation("test", "test", emptyList(), null);
    }

    private InternalTopMetrics buildFilled(SortValue sortValue, double metricValue) {
        DocValueFormat sortFormat = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN, DocValueFormat.IP);
        SortOrder sortOrder = randomFrom(SortOrder.values());
        return new InternalTopMetrics("test", sortFormat, sortOrder, sortValue, "test", metricValue, emptyList(), null);
    }

    private InternalTopMetrics reduce(InternalTopMetrics... results) {
        return results[0].reduce(Arrays.asList(results), null);
    }
}
