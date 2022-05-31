/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class RoundingIntervalTests extends ESTestCase {
    public void testRoundingByStartTime() {
        long startTime = 111;
        long interval = 10;
        RoundingInterval rounding = new RoundingInterval(startTime, interval);
        assertThat(rounding.nextRoundingValue(113), equalTo(121L));
        assertThat(rounding.nextRoundingValue(120), equalTo(121L));
        assertThat(rounding.nextRoundingValue(121), equalTo(131L));
        assertThat(rounding.nextRoundingValue(122), equalTo(131L));
    }

    public void testRoundingByRounding() {
        long startTime = 0;
        long interval = 10;
        RoundingInterval rounding = new RoundingInterval(startTime, interval);
        assertThat(rounding.nextRoundingValue(113), equalTo(120L));
        assertThat(rounding.nextRoundingValue(119), equalTo(120L));
        assertThat(rounding.nextRoundingValue(120), equalTo(130L));
        assertThat(rounding.nextRoundingValue(121), equalTo(130L));
    }
}
