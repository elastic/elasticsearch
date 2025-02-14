/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class StatsSummaryTests extends ESTestCase {
    public void tesEmpty() {
        StatsSummary accumulator = new StatsSummary();
        assertThat(accumulator.getMin(), equalTo(0.0));
        assertThat(accumulator.getMax(), equalTo(0.0));
        assertThat(accumulator.getSum(), equalTo(0.0));
        assertThat(accumulator.getAverage(), equalTo(0.0));
    }

    public void testGivenPositiveValues() {
        StatsSummary accumulator = new StatsSummary();

        for (int i = 1; i <= 10; i++) {
            accumulator.accept(i);
        }

        assertThat(accumulator.getMin(), equalTo(1.0));
        assertThat(accumulator.getMax(), equalTo(10.0));
        assertThat(accumulator.getSum(), equalTo(55.0));
        assertThat(accumulator.getAverage(), equalTo(5.5));
    }

    public void testGivenNegativeValues() {
        StatsSummary accumulator = new StatsSummary();

        for (int i = 1; i <= 10; i++) {
            accumulator.accept(-1 * i);
        }

        assertThat(accumulator.getMin(), equalTo(-10.0));
        assertThat(accumulator.getMax(), equalTo(-1.0));
        assertThat(accumulator.getSum(), equalTo(-55.0));
        assertThat(accumulator.getAverage(), equalTo(-5.5));
    }

    public void testReset() {
        StatsSummary accumulator = new StatsSummary();
        randomDoubles(randomIntBetween(1, 20)).forEach(accumulator);
        assertThat(accumulator, not(equalTo(new StatsSummary())));

        accumulator.reset();
        assertThat(accumulator, equalTo(new StatsSummary()));
        assertThat(accumulator.getMin(), equalTo(0.0));
        assertThat(accumulator.getMax(), equalTo(0.0));
        assertThat(accumulator.getSum(), equalTo(0.0));
        assertThat(accumulator.getAverage(), equalTo(0.0));
    }

    public void testEqualsAndHashCode() {
        StatsSummary stats1 = new StatsSummary();
        StatsSummary stats2 = new StatsSummary();

        // Empty accumulators are equals.
        assertThat(stats1, equalTo(stats2));
        assertThat(stats1.hashCode(), equalTo(stats2.hashCode()));

        // Accumulators with same values are equals
        randomDoubles(randomIntBetween(0, 20)).forEach(stats1.andThen(stats2));
        assertThat(stats1, equalTo(stats2));
        assertThat(stats1.hashCode(), equalTo(stats2.hashCode()));

        // Accumulators with same sum, but different counts are not equals
        stats1.accept(1);
        stats1.accept(1);
        stats2.accept(2);
        assertThat(stats1.getSum(), equalTo(stats2.getSum()));
        assertThat(stats1.getCount(), not(equalTo(stats2.getCount())));
        assertThat(stats1, not(equalTo(stats2)));
        assertThat(stats1.hashCode(), not(equalTo(stats2.hashCode())));

        // Accumulators with different values are not equals
        stats1.reset();
        stats2.reset();
        randomDoubles(randomIntBetween(1, 20)).forEach(stats1.andThen(v -> stats2.accept(v + randomDoubleBetween(0.0, 1.0, false))));
        assertThat(stats1.getCount(), equalTo(stats2.getCount()));
        assertThat(stats1.getSum(), not(equalTo(stats2.getSum())));
        assertThat(stats1, not(equalTo(stats2)));
        assertThat(stats1.hashCode(), not(equalTo(stats2.hashCode())));
    }
}
