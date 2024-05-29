/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatsAccumulatorTests extends AbstractWireSerializingTestCase<StatsAccumulator> {

    public void testGivenNoValues() {
        StatsAccumulator accumulator = new StatsAccumulator();
        assertThat(accumulator.getMin(), equalTo(0.0));
        assertThat(accumulator.getMax(), equalTo(0.0));
        assertThat(accumulator.getTotal(), equalTo(0.0));
        assertThat(accumulator.getAvg(), equalTo(0.0));
    }

    public void testGivenPositiveValues() {
        StatsAccumulator accumulator = new StatsAccumulator();

        for (int i = 1; i <= 10; i++) {
            accumulator.add(i);
        }

        assertThat(accumulator.getMin(), equalTo(1.0));
        assertThat(accumulator.getMax(), equalTo(10.0));
        assertThat(accumulator.getTotal(), equalTo(55.0));
        assertThat(accumulator.getAvg(), equalTo(5.5));
    }

    public void testGivenNegativeValues() {
        StatsAccumulator accumulator = new StatsAccumulator();

        for (int i = 1; i <= 10; i++) {
            accumulator.add(-1 * i);
        }

        assertThat(accumulator.getMin(), equalTo(-10.0));
        assertThat(accumulator.getMax(), equalTo(-1.0));
        assertThat(accumulator.getTotal(), equalTo(-55.0));
        assertThat(accumulator.getAvg(), equalTo(-5.5));
    }

    public void testAsMap() {
        StatsAccumulator accumulator = new StatsAccumulator();
        accumulator.add(5.0);
        accumulator.add(10.0);

        Map<String, Double> expectedMap = new HashMap<>();
        expectedMap.put("min", 5.0);
        expectedMap.put("max", 10.0);
        expectedMap.put("avg", 7.5);
        expectedMap.put("total", 15.0);
        assertThat(accumulator.asMap(), equalTo(expectedMap));
    }

    public void testMerge() {
        StatsAccumulator accumulator = new StatsAccumulator();
        accumulator.add(5.0);
        accumulator.add(10.0);

        assertThat(accumulator.getMin(), equalTo(5.0));
        assertThat(accumulator.getMax(), equalTo(10.0));
        assertThat(accumulator.getTotal(), equalTo(15.0));
        assertThat(accumulator.getAvg(), equalTo(7.5));

        StatsAccumulator accumulator2 = new StatsAccumulator();
        accumulator2.add(1.0);
        accumulator2.add(3.0);
        accumulator2.add(7.0);

        assertThat(accumulator2.getMin(), equalTo(1.0));
        assertThat(accumulator2.getMax(), equalTo(7.0));
        assertThat(accumulator2.getTotal(), equalTo(11.0));
        assertThat(accumulator2.getAvg(), equalTo(11.0 / 3.0));

        accumulator.merge(accumulator2);
        assertThat(accumulator.getMin(), equalTo(1.0));
        assertThat(accumulator.getMax(), equalTo(10.0));
        assertThat(accumulator.getTotal(), equalTo(26.0));
        assertThat(accumulator.getAvg(), equalTo(5.2));

        // same as accumulator
        StatsAccumulator accumulator3 = new StatsAccumulator();
        accumulator3.add(5.0);
        accumulator3.add(10.0);

        // merging the other way should yield the same results
        accumulator2.merge(accumulator3);
        assertThat(accumulator2.getMin(), equalTo(1.0));
        assertThat(accumulator2.getMax(), equalTo(10.0));
        assertThat(accumulator2.getTotal(), equalTo(26.0));
        assertThat(accumulator2.getAvg(), equalTo(5.2));
    }

    public void testMergeMixedEmpty() {
        StatsAccumulator accumulator = new StatsAccumulator();

        StatsAccumulator accumulator2 = new StatsAccumulator();
        accumulator2.add(1.0);
        accumulator2.add(3.0);
        accumulator.merge(accumulator2);
        assertThat(accumulator.getMin(), equalTo(1.0));
        assertThat(accumulator.getMax(), equalTo(3.0));
        assertThat(accumulator.getTotal(), equalTo(4.0));

        StatsAccumulator accumulator3 = new StatsAccumulator();
        accumulator.merge(accumulator3);
        assertThat(accumulator.getMin(), equalTo(1.0));
        assertThat(accumulator.getMax(), equalTo(3.0));
        assertThat(accumulator.getTotal(), equalTo(4.0));

        StatsAccumulator accumulator4 = new StatsAccumulator();
        accumulator3.merge(accumulator4);

        assertThat(accumulator3.getMin(), equalTo(0.0));
        assertThat(accumulator3.getMax(), equalTo(0.0));
        assertThat(accumulator3.getTotal(), equalTo(0.0));
    }

    public void testFromStatsAggregation() {
        Stats stats = mock(Stats.class);
        when(stats.getMax()).thenReturn(25.0);
        when(stats.getMin()).thenReturn(2.5);
        when(stats.getCount()).thenReturn(4L);
        when(stats.getSum()).thenReturn(48.0);
        when(stats.getAvg()).thenReturn(12.0);

        StatsAccumulator accumulator = StatsAccumulator.fromStatsAggregation(stats);
        assertThat(accumulator.getMin(), equalTo(2.5));
        assertThat(accumulator.getMax(), equalTo(25.0));
        assertThat(accumulator.getTotal(), equalTo(48.0));
        assertThat(accumulator.getAvg(), equalTo(12.0));
    }

    @Override
    public StatsAccumulator createTestInstance() {
        StatsAccumulator accumulator = new StatsAccumulator();
        for (int i = 0; i < randomInt(10); ++i) {
            accumulator.add(randomDoubleBetween(0.0, 1000.0, true));
        }

        return accumulator;
    }

    @Override
    protected StatsAccumulator mutateInstance(StatsAccumulator instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<StatsAccumulator> instanceReader() {
        return StatsAccumulator::new;
    }
}
