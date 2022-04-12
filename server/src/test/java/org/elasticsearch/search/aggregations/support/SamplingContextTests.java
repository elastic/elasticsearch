/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class SamplingContextTests extends ESTestCase {
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    private static SamplingContext randomContext() {
        return new SamplingContext(randomDoubleBetween(1e-6, 0.1, false), randomInt());
    }

    public void testScaling() {
        for (int i = 0; i < 20; i++) {
            SamplingContext samplingContext = randomContext();
            long randomLong = randomLongBetween(1_000_000_000L, 100_000_000_000L);
            double randomDouble = randomDouble();
            long rescaled = samplingContext.scaleUp(samplingContext.scaleDown(randomLong));
            assertThat(
                Double.toString(samplingContext.probability()),
                (double) rescaled / randomLong,
                // No matter how you scale `long` values, the inverse back may be a little off due to rounding
                closeTo(1.0, 1e-4)
            );
            assertThat(
                Double.toString(samplingContext.probability()),
                randomDouble,
                closeTo(samplingContext.scaleUp(samplingContext.scaleDown(randomDouble)), 1e-12)
            );
        }
    }

    public void testNoScaling() {
        SamplingContext samplingContext = new SamplingContext(1.0, randomInt());
        long randomLong = randomLong();
        double randomDouble = randomDouble();
        assertThat(randomLong, equalTo(samplingContext.scaleDown(randomLong)));
        assertThat(randomDouble, equalTo(samplingContext.scaleDown(randomDouble)));
    }

}
