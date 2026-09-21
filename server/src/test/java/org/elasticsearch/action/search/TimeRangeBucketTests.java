/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class TimeRangeBucketTests extends ESTestCase {

    public void testHistogramBoundariesMatchEveryThreshold() {
        assertEquals(Arrays.stream(TimeRangeBucket.values()).map(TimeRangeBucket::millis).toList(), TimeRangeBucket.histogramBoundaries());
    }

    public void testResolveClampsNegativeAndOverflowAges() {
        assertEquals(TimeRangeBucket.FifteenMinutes.label(), TimeRangeBucket.resolve(-1));
        assertEquals(TimeRangeBucket.FifteenMinutes.label(), TimeRangeBucket.resolve(0));
        assertEquals(TimeRangeBucket.FifteenMinutes.label(), TimeRangeBucket.resolve(TimeRangeBucket.FifteenMinutes.millis()));
        assertEquals(TimeRangeBucket.OneHour.label(), TimeRangeBucket.resolve(TimeRangeBucket.FifteenMinutes.millis() + 1));
        assertEquals(TimeRangeBucket.OlderThan14Days.label(), TimeRangeBucket.resolve(TimeRangeBucket.FourteenDays.millis() + 1));
        assertEquals(TimeRangeBucket.OlderThan14Days.label(), TimeRangeBucket.resolve(Long.MAX_VALUE));
    }
}
