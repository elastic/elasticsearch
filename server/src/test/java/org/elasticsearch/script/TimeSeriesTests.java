/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

public class TimeSeriesTests extends ESTestCase {
    public void testMerge() {
        var first = randomTimeseries();
        var second = randomTimeseries();

        assertEquals(
            TimeSeries.merge(first, second),
            new TimeSeries(
                first.fiveMinutes + second.fiveMinutes,
                first.fifteenMinutes + second.fifteenMinutes,
                first.twentyFourHours + second.twentyFourHours,
                first.total + second.total
            )
        );
    }

    static TimeSeries randomTimeseries() {
        return new TimeSeries(randomLongBetween(0, 10000), randomLongBetween(0, 10000), randomLongBetween(0, 10000), randomLongBetween(0, 10000));
    }
}
