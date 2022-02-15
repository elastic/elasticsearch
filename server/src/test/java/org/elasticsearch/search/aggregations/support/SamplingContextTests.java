/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SamplingContextTests extends ESTestCase {
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    private static SamplingContext randomContext() {
        return new SamplingContext(randomDoubleBetween(1e-8, 0.1, false), randomInt());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/83748")
    public void testScaling() {
        for (int i = 0; i < 20; i++) {
            SamplingContext samplingContext = randomContext();
            long randomLong = randomLongBetween(100_000_000L, Long.MAX_VALUE);
            double randomDouble = randomDouble();
            long rescaled = samplingContext.inverseScale(samplingContext.scale(randomLong));
            // No matter how you scale `long` values, the inverse back may be a little off
            long error = (long) (rescaled * 1e-15);
            assertThat(
                Double.toString(samplingContext.probability()),
                rescaled,
                allOf(greaterThanOrEqualTo(randomLong - error), lessThanOrEqualTo(randomLong + error))
            );
            assertThat(
                Double.toString(samplingContext.probability()),
                randomDouble,
                closeTo(samplingContext.inverseScale(samplingContext.scale(randomDouble)), 1e-12)
            );
        }
    }

    public void testNoScaling() {
        SamplingContext samplingContext = new SamplingContext(1.0, randomInt());
        long randomLong = randomLong();
        double randomDouble = randomDouble();
        assertThat(randomLong, equalTo(samplingContext.scale(randomLong)));
        assertThat(randomDouble, equalTo(samplingContext.scale(randomDouble)));
    }

}
