/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rollup;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;

import java.time.ZoneId;

public class RollupShardDeciderTests extends ESTestCase {

    public void testCanMatchCalendarInterval() {
        assertTrue(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.HOUR, DateHistogramInterval.HOUR));
        assertTrue(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.DAY, DateHistogramInterval.DAY));
        assertTrue(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.WEEK, DateHistogramInterval.WEEK));
        assertTrue(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.MONTH, DateHistogramInterval.MONTH));
        assertTrue(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.YEAR, DateHistogramInterval.YEAR));

        assertTrue(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.MONTH, DateHistogramInterval.DAY));
        assertFalse(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.DAY, DateHistogramInterval.MONTH));
        assertTrue(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.WEEK, DateHistogramInterval.DAY));
        assertFalse(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.DAY, DateHistogramInterval.WEEK));
        assertTrue(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.DAY, DateHistogramInterval.HOUR));
        assertFalse(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.HOUR, DateHistogramInterval.DAY));

        assertTrue(RollupShardDecider.canMatchCalendarInterval(null, DateHistogramInterval.HOUR));
        assertFalse(RollupShardDecider.canMatchCalendarInterval(DateHistogramInterval.HOUR, null));
    }

    public void testCanMatchTimezone() {
        assertTrue(RollupShardDecider.canMatchTimezone(ZoneId.of("Z"), ZoneId.of("Z")));
        assertTrue(RollupShardDecider.canMatchTimezone(ZoneId.of("Z"), ZoneId.of("UTC")));
        assertTrue(RollupShardDecider.canMatchTimezone(ZoneId.of("UTC"), ZoneId.of("+00:00")));
        assertFalse(RollupShardDecider.canMatchTimezone(ZoneId.of("Europe/Paris"), ZoneId.of("+01:00")));
        assertTrue(RollupShardDecider.canMatchTimezone(ZoneId.of("Europe/Paris"), ZoneId.of("Europe/Paris")));
        assertFalse(RollupShardDecider.canMatchTimezone(ZoneId.of("Europe/Paris"), ZoneId.of("Europe/Athens")));
        assertFalse(RollupShardDecider.canMatchTimezone(ZoneId.of("UTC"), ZoneId.of("+01:00")));
    }

}
