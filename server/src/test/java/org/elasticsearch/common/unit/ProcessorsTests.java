/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ProcessorsTests extends ESTestCase {
    public void testTruncatesAfterFiveDecimalPlaces() {
        final double processorCount = randomNumberOfProcessors();

        final Processors processors = new Processors(processorCount);
        final String processorsString = Double.toString(processors.count());
        final int decimalPlaces = processorsString.length() - processorsString.indexOf(".") - 1;
        assertThat(decimalPlaces, is(lessThanOrEqualTo(Processors.NUMBER_OF_DECIMAL_PLACES)));
    }

    public void testRounding() {
        {
            final Processors processors = new Processors(1.2);
            assertThat(processors.roundDown(), is(equalTo(1)));
            assertThat(processors.roundUp(), is(equalTo(2)));
        }

        {
            final Processors processors = new Processors(0.1);
            assertThat(processors.roundDown(), is(equalTo(1)));
            assertThat(processors.roundUp(), is(equalTo(1)));
        }

        {
            final Processors processors = new Processors(1E-12);
            assertThat(processors.roundDown(), is(equalTo(1)));
            assertThat(processors.roundUp(), is(equalTo(1)));
        }
    }

    public void testNeverRoundsDownToZero() {
        final Processors processors = new Processors(1E-12);
        assertThat(processors.count(), is(greaterThan(0.0)));
    }

    public void testValidation() {
        expectThrows(IllegalArgumentException.class, () -> new Processors(-1.0));
        expectThrows(IllegalArgumentException.class, () -> new Processors(Double.POSITIVE_INFINITY));
        expectThrows(IllegalArgumentException.class, () -> new Processors(Double.NEGATIVE_INFINITY));
        expectThrows(IllegalArgumentException.class, () -> new Processors(Double.NaN));
    }

    public void testAddition() {
        final Processors processorsA = new Processors(randomNumberOfProcessors());
        final Processors processorsB = new Processors(randomNumberOfProcessors());

        final Processors addedProcessors = processorsA.plus(processorsB);

        assertThat(addedProcessors, is(greaterThan(processorsA)));
        assertThat(addedProcessors, is(greaterThan(processorsB)));
    }

    public void testOverflowAddition() {
        final Processors processorsA = new Processors(Double.MAX_VALUE);
        final Processors processorsB = new Processors(Double.MAX_VALUE);

        expectThrows(ArithmeticException.class, () -> processorsA.plus(processorsB));
    }

    public void testMultiplication() {
        final Processors processors = new Processors(randomNumberOfProcessors());
        final Processors multipliedProcessors = processors.multiply(100);

        assertThat(multipliedProcessors, is(greaterThan(processors)));
    }

    private double randomNumberOfProcessors() {
        return randomDoubleBetween(Math.ulp(0.0), 512.99999999, true);
    }
}
