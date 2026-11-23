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

import java.util.List;

public class TimeRangeBucketTests extends ESTestCase {

    public void testHistogramBoundariesAreHoursWithDoubleMaxOverflow() {
        List<Double> bounds = TimeRangeBucket.histogramHourBoundaries();
        assertEquals(TimeRangeBucket.values().length, bounds.size());
        assertEquals(0.25, bounds.get(0), 0.0);
        assertEquals(1.0, bounds.get(1), 0.0);
        assertEquals(12.0, bounds.get(2), 0.0);
        assertEquals(24.0, bounds.get(3), 0.0);
        assertEquals(72.0, bounds.get(4), 0.0);
        assertEquals(168.0, bounds.get(5), 0.0);
        assertEquals(336.0, bounds.get(6), 0.0);
        assertEquals(Double.MAX_VALUE, bounds.get(7), 0.0);
    }

    public void testToHours() {
        assertEquals(0.25, TimeRangeBucket.toHours(TimeRangeBucket.FifteenMinutes.millis()), 0.0);
        assertEquals(1.0, TimeRangeBucket.toHours(TimeRangeBucket.OneHour.millis()), 0.0);
        assertEquals(336.0, TimeRangeBucket.toHours(TimeRangeBucket.FourteenDays.millis()), 0.0);
        assertEquals(-1_000 / 3_600_000.0, TimeRangeBucket.toHours(-1_000), 0.0);
    }

    public void testResolveClampsNegativeAndOverflowAges() {
        assertEquals(TimeRangeBucket.FifteenMinutes.label(), TimeRangeBucket.resolve(Long.MIN_VALUE));
        assertEquals(TimeRangeBucket.FifteenMinutes.label(), TimeRangeBucket.resolve(-1));
        assertEquals(TimeRangeBucket.FifteenMinutes.label(), TimeRangeBucket.resolve(0));
        assertEquals(TimeRangeBucket.FifteenMinutes.label(), TimeRangeBucket.resolve(TimeRangeBucket.FifteenMinutes.millis()));
        long tolerance = TimeRangeBucket.RESOLVE_TOLERANCE.millis();
        assertEquals(TimeRangeBucket.OneHour.label(), TimeRangeBucket.resolve(TimeRangeBucket.FifteenMinutes.millis() + tolerance + 1));
        assertEquals(
            TimeRangeBucket.OlderThan14Days.label(),
            TimeRangeBucket.resolve(TimeRangeBucket.FourteenDays.millis() + tolerance + 1)
        );
        assertEquals(TimeRangeBucket.OlderThan14Days.label(), TimeRangeBucket.resolve(Long.MAX_VALUE));
    }

    /**
     * Clients like Kibana send absolute timestamps for presets such as "Last 1 hour", so the age arrives slightly past the
     * threshold. Up to {@link TimeRangeBucket#RESOLVE_TOLERANCE} past a threshold still counts as within it.
     */
    public void testResolveToleratesAgesJustPastThreshold() {
        long tolerance = TimeRangeBucket.RESOLVE_TOLERANCE.millis();
        for (TimeRangeBucket bucket : TimeRangeBucket.values()) {
            if (bucket == TimeRangeBucket.OlderThan14Days) {
                continue;
            }
            assertEquals(bucket.label(), TimeRangeBucket.resolve(bucket.millis() + randomLongBetween(1, tolerance)));
            assertNotEquals(bucket.label(), TimeRangeBucket.resolve(bucket.millis() + tolerance + 1));
        }
    }
}
