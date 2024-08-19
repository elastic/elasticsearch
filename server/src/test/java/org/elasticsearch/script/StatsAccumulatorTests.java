/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class StatsAccumulatorTests extends ESTestCase {
    public void tesEmpty() {
        StatsAccumulator accumulator = new StatsAccumulator();
        assertThat(accumulator.getMin(), equalTo(0.0));
        assertThat(accumulator.getMax(), equalTo(0.0));
        assertThat(accumulator.getSum(), equalTo(0.0));
        assertThat(accumulator.getAverage(), equalTo(0.0));
    }

    public void testGivenPositiveValues() {
        StatsAccumulator accumulator = new StatsAccumulator();

        for (int i = 1; i <= 10; i++) {
            accumulator.accept(i);
        }

        assertThat(accumulator.getMin(), equalTo(1.0));
        assertThat(accumulator.getMax(), equalTo(10.0));
        assertThat(accumulator.getSum(), equalTo(55.0));
        assertThat(accumulator.getAverage(), equalTo(5.5));
    }

    public void testGivenNegativeValues() {
        StatsAccumulator accumulator = new StatsAccumulator();

        for (int i = 1; i <= 10; i++) {
            accumulator.accept(-1 * i);
        }

        assertThat(accumulator.getMin(), equalTo(-10.0));
        assertThat(accumulator.getMax(), equalTo(-1.0));
        assertThat(accumulator.getSum(), equalTo(-55.0));
        assertThat(accumulator.getAverage(), equalTo(-5.5));
    }

    public void testReset() {
        StatsAccumulator accumulator = new StatsAccumulator();
        randomDoubles(randomIntBetween(1, 20)).forEach(accumulator);
        assertThat(accumulator, not(equalTo(new StatsAccumulator())));

        accumulator.reset();
        assertThat(accumulator, equalTo(new StatsAccumulator()));
        assertThat(accumulator.getMin(), equalTo(0.0));
        assertThat(accumulator.getMax(), equalTo(0.0));
        assertThat(accumulator.getSum(), equalTo(0.0));
        assertThat(accumulator.getAverage(), equalTo(0.0));
    }

    public void testEqualsAndHashCode() {
        StatsAccumulator stats1 = new StatsAccumulator();
        StatsAccumulator stats2 = new StatsAccumulator();

        // Empty accumulators are equals.
        assertThat(stats1, equalTo(stats2));
        assertThat(stats1.hashCode(), equalTo(stats2.hashCode()));

        // Accumulators with same values are equals
        randomDoubles(randomIntBetween(0, 20)).forEach(stats1.andThen(stats2));
        assertThat(stats1, equalTo(stats2));
        assertThat(stats1.hashCode(), equalTo(stats2.hashCode()));

        // Accumulators with different values are not equals
        randomDoubles(randomIntBetween(0, 20)).forEach(stats1);
        randomDoubles(randomIntBetween(0, 20)).forEach(stats2);
        assertThat(stats1, not(equalTo(stats2)));
        assertThat(stats1.hashCode(), not(equalTo(stats2.hashCode())));
    }
}
