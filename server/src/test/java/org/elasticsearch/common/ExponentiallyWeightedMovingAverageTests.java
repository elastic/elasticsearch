/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Implements exponentially weighted moving averages (commonly abbreviated EWMA) for a single value.
 */
public class ExponentiallyWeightedMovingAverageTests extends ESTestCase {

    public void testEWMA() {
        final ExponentiallyWeightedMovingAverage ewma = new ExponentiallyWeightedMovingAverage(0.5, 10);
        ewma.addValue(12);
        assertThat(ewma.getAverage(), equalTo(11.0));
        ewma.addValue(10);
        ewma.addValue(15);
        ewma.addValue(13);
        assertThat(ewma.getAverage(), equalTo(12.875));
    }

    public void testInvalidAlpha() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new ExponentiallyWeightedMovingAverage(-0.5, 10));
        assertThat(ex.getMessage(), equalTo("alpha must be greater or equal to 0 and less than or equal to 1"));

        ex = expectThrows(IllegalArgumentException.class, () -> new ExponentiallyWeightedMovingAverage(1.5, 10));
        assertThat(ex.getMessage(), equalTo("alpha must be greater or equal to 0 and less than or equal to 1"));
    }

    public void testConvergingToValue() {
        final ExponentiallyWeightedMovingAverage ewma = new ExponentiallyWeightedMovingAverage(0.5, 10000);
        for (int i = 0; i < 100000; i++) {
            ewma.addValue(1);
        }
        assertThat(ewma.getAverage(), lessThan(2.0));
    }
}
