/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notANumber;

public class InternalTopMetricsReduceTests extends ESTestCase {
    public void testAllEmpty() {
        InternalTopMetrics first = buildEmpty();
        InternalTopMetrics reduced = reduce(first, buildEmpty(), buildEmpty(), buildEmpty());
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getSortValue(), notANumber());
        assertThat(reduced.getSortFormat(), equalTo(DocValueFormat.RAW));
        assertThat(reduced.getSortOrder(), equalTo(first.getSortOrder()));
        assertThat(reduced.getMetricValue(), notANumber());
        assertThat(reduced.getMetricName(), equalTo("test"));
    }

    public void testFirstEmpty() {
        InternalTopMetrics first = buildEmpty();
        InternalTopMetrics filled = buildFilled(randomDouble(), randomDouble());
        InternalTopMetrics reduced = reduce(first, buildEmpty(), filled, buildEmpty());
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getSortValue(), equalTo(filled.getSortValue()));
        assertThat(reduced.getSortFormat(), equalTo(filled.getSortFormat()));
        assertThat(reduced.getSortOrder(), equalTo(first.getSortOrder()));
        assertThat(reduced.getMetricValue(), equalTo(filled.getMetricValue()));
        assertThat(reduced.getMetricName(), equalTo("test"));
    }

    public void testMany() {
        InternalTopMetrics min = buildFilled(1.0, randomDouble());
        InternalTopMetrics max = buildFilled(7.0, randomDouble());
        InternalTopMetrics[] metrics = new InternalTopMetrics[] {
                min, buildEmpty(), buildFilled(2.0, randomDouble()), buildEmpty(), max
        };
        Collections.shuffle(Arrays.asList(metrics), random());
        InternalTopMetrics winner = metrics[0].getSortOrder() == SortOrder.ASC ? min : max; 
        InternalTopMetrics reduced = reduce(metrics);
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getSortValue(), equalTo(7.0d));
        assertThat(reduced.getSortFormat(), equalTo(winner.getSortFormat()));
        assertThat(reduced.getSortOrder(), equalTo(metrics[0].getSortOrder()));
        assertThat(reduced.getMetricValue(), equalTo(winner.getMetricValue()));
        assertThat(reduced.getMetricName(), equalTo("test"));
    }

    private InternalTopMetrics buildEmpty() {
        DocValueFormat sortFormat = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN, DocValueFormat.IP);
        SortOrder sortOrder = randomFrom(SortOrder.values());
        return InternalTopMetrics.buildEmptyAggregation("test", sortFormat, sortOrder, "test", emptyList(), null);
    }

    private InternalTopMetrics buildFilled(double sortValue, double metricValue) {
        DocValueFormat sortFormat = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN, DocValueFormat.IP);
        SortOrder sortOrder = randomFrom(SortOrder.values());
        return new InternalTopMetrics("test", sortFormat, sortOrder, sortValue, "test", metricValue, emptyList(), null);
    }

    private InternalTopMetrics reduce(InternalTopMetrics... results) {
        return results[0].reduce(Arrays.asList(results), null);
    }
}
