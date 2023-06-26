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
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ProcessorsTests extends ESTestCase {
    public void testTruncatesAfterFiveDecimalPlaces() {
        final double processorCount = randomNumberOfProcessors();

        final Processors processors = Processors.of(processorCount);
        final double roundedProcessorsCount = processors.count();
        final String processorsString = Double.toString(roundedProcessorsCount);
        final int decimalPlaces;
        if (processorsString.contains("E")) {
            int exponent = Integer.parseInt(processorsString.substring(processorsString.indexOf("E") + 1));
            assertThat(exponent, is(lessThan(0)));
            decimalPlaces = Math.abs(exponent);
        } else {
            decimalPlaces = processorsString.length() - processorsString.indexOf(".") - 1;
        }
        assertThat(decimalPlaces, is(lessThanOrEqualTo(Processors.NUMBER_OF_DECIMAL_PLACES)));

        assertThat(Processors.of(roundedProcessorsCount).count(), is(equalTo(roundedProcessorsCount)));
    }

    public void testRounding() {
        {
            final Processors processors = Processors.of(1.2);
            assertThat(processors.roundDown(), is(equalTo(1)));
            assertThat(processors.roundUp(), is(equalTo(2)));
        }

        {
            final Processors processors = Processors.of(10.1);
            assertThat(processors.roundDown(), is(equalTo(10)));
            assertThat(processors.roundUp(), is(equalTo(11)));
        }

        {
            final Processors processors = Processors.of((double) 12);
            assertThat(processors.roundDown(), is(equalTo(12)));
            assertThat(processors.roundUp(), is(equalTo(12)));
        }

        {
            final Processors processors = Processors.of(0.1);
            assertThat(processors.roundDown(), is(equalTo(1)));
            assertThat(processors.roundUp(), is(equalTo(1)));
        }

        {
            final Processors processors = Processors.of(1E-12);
            assertThat(processors.roundDown(), is(equalTo(1)));
            assertThat(processors.roundUp(), is(equalTo(1)));
        }
    }

    public void testNeverRoundsDownToZero() {
        final Processors processors = Processors.of(1E-12);
        assertThat(processors.count(), is(greaterThan(0.0)));
    }

    public void testValidation() {
        expectThrows(IllegalArgumentException.class, () -> Processors.of(-1.0));
        expectThrows(IllegalArgumentException.class, () -> Processors.of(Double.POSITIVE_INFINITY));
        expectThrows(IllegalArgumentException.class, () -> Processors.of(Double.NEGATIVE_INFINITY));
        expectThrows(IllegalArgumentException.class, () -> Processors.of(Double.NaN));
    }

    public void testAddition() {
        final Processors processorsA = Processors.of(randomNumberOfProcessors());
        final Processors processorsB = Processors.of(randomNumberOfProcessors());

        final Processors addedProcessors = processorsA.plus(processorsB);

        assertThat(addedProcessors, is(greaterThan(processorsA)));
        assertThat(addedProcessors, is(greaterThan(processorsB)));
    }

    public void testOverflowAddition() {
        final Processors processorsA = Processors.of(Double.MAX_VALUE);
        final Processors processorsB = Processors.of(Double.MAX_VALUE);

        expectThrows(ArithmeticException.class, () -> processorsA.plus(processorsB));
    }

    public void testMultiplication() {
        final Processors processors = Processors.of(randomNumberOfProcessors());
        final Processors multipliedProcessors = processors.multiply(100);

        assertThat(multipliedProcessors, is(greaterThan(processors)));
    }

    public void testFloatProcessorsConvertedToDoubleAreCloseToEqual() {
        final double processorCount = randomNumberOfProcessors();
        final float processorCountAsFloat = (float) processorCount;
        final Processors bwcProcessors = Processors.of((double) processorCountAsFloat);
        final Processors doubleProcessor = Processors.of(processorCount);
        assertThat(Processors.equalsOrCloseTo(bwcProcessors, doubleProcessor), is(true));
    }

    private double randomNumberOfProcessors() {
        return randomDoubleBetween(Math.ulp(0.0), 512.99999999, true);
    }
}
