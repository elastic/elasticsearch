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
import java.util.Collections;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notANumber;
import static org.hamcrest.Matchers.nullValue;

public class InternalTopMetricsReduceTests extends ESTestCase {
    public void testAllEmpty() {
        InternalTopMetrics first = buildEmpty();
        InternalTopMetrics reduced = reduce(first, buildEmpty(), buildEmpty(), buildEmpty());
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getSortValue(), nullValue());
        assertThat(reduced.getSortFormat(), equalTo(DocValueFormat.RAW));
        assertThat(reduced.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(reduced.getMetricValue(), notANumber());
        assertThat(reduced.getMetricName(), equalTo("test"));
    }

    public void testFirstEmpty() {
        InternalTopMetrics first = buildEmpty();
        InternalTopMetrics filled = buildFilled(SortValue.from(randomDouble()), randomDouble());
        InternalTopMetrics reduced = reduce(first, buildEmpty(), filled, buildEmpty());
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getSortValue(), equalTo(filled.getSortValue()));
        assertThat(reduced.getSortFormat(), equalTo(filled.getSortFormat()));
        assertThat(reduced.getSortOrder(), equalTo(filled.getSortOrder()));
        assertThat(reduced.getMetricValue(), equalTo(filled.getMetricValue()));
        assertThat(reduced.getMetricName(), equalTo("test"));
    }

    public void testMany() {
        InternalTopMetrics min = buildFilled(SortValue.from(1.0), randomDouble());
        InternalTopMetrics max = buildFilled(SortValue.from(7.0), randomDouble());
        InternalTopMetrics[] metrics = new InternalTopMetrics[] {
                min, buildEmpty(), buildFilled(SortValue.from(2.0), randomDouble()), buildEmpty(), max
        };
        Collections.shuffle(Arrays.asList(metrics), random());
        SortOrder sortOrder = Arrays.stream(metrics).filter(i -> i.getSortValue() != null).findFirst().get().getSortOrder();
        InternalTopMetrics winner = sortOrder == SortOrder.ASC ? min : max; 
        InternalTopMetrics reduced = reduce(metrics);
        assertThat(reduced.getName(), equalTo("test"));
        assertThat(reduced.getSortValue(), equalTo(winner.getSortValue()));
        assertThat(reduced.getSortFormat(), equalTo(winner.getSortFormat()));
        assertThat(reduced.getSortOrder(), equalTo(sortOrder));
        assertThat(reduced.getMetricValue(), equalTo(winner.getMetricValue()));
        assertThat(reduced.getMetricName(), equalTo("test"));
    }

    // NOCOMMIT tests for mixed types

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
