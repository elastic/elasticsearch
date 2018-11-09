/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils.time;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.time.TimeUtils;

import java.util.concurrent.TimeUnit;

public class TimeUtilsTests extends ESTestCase {

    public void testdateStringToEpoch() {
        assertEquals(1462096800000L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00Z"));
        assertEquals(1462096800333L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.333Z"));
        assertEquals(1462096800334L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.334+00"));
        assertEquals(1462096800335L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.335+0000"));
        assertEquals(1462096800333L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.333+00:00"));
        assertEquals(1462093200333L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.333+01"));
        assertEquals(1462093200333L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.333+0100"));
        assertEquals(1462093200333L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.333+01:00"));
        assertEquals(1462098600333L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.333-00:30"));
        assertEquals(1462098600333L, TimeUtils.dateStringToEpoch("2016-05-01T10:00:00.333-0030"));
        assertEquals(1477058573000L, TimeUtils.dateStringToEpoch("1477058573"));
        assertEquals(1477058573500L, TimeUtils.dateStringToEpoch("1477058573500"));
    }

    public void testCheckMultiple_GivenMultiples() {
        TimeUtils.checkMultiple(TimeValue.timeValueHours(1), TimeUnit.SECONDS, new ParseField("foo"));
        TimeUtils.checkMultiple(TimeValue.timeValueHours(1), TimeUnit.MINUTES, new ParseField("foo"));
        TimeUtils.checkMultiple(TimeValue.timeValueHours(1), TimeUnit.HOURS, new ParseField("foo"));
        TimeUtils.checkMultiple(TimeValue.timeValueHours(2), TimeUnit.HOURS, new ParseField("foo"));
        TimeUtils.checkMultiple(TimeValue.timeValueSeconds(60), TimeUnit.SECONDS, new ParseField("foo"));
        TimeUtils.checkMultiple(TimeValue.timeValueSeconds(60), TimeUnit.MILLISECONDS, new ParseField("foo"));
    }

    public void testCheckMultiple_GivenNonMultiple() {
        expectThrows(IllegalArgumentException.class, () ->
                TimeUtils.checkMultiple(TimeValue.timeValueMillis(500), TimeUnit.SECONDS, new ParseField("foo")));
    }

    public void testCheckPositiveMultiple_GivenNegative() {
        expectThrows(IllegalArgumentException.class, () ->
                TimeUtils.checkPositiveMultiple(TimeValue.timeValueMillis(-1), TimeUnit.MILLISECONDS, new ParseField("foo")));
    }

    public void testCheckPositiveMultiple_GivenZero() {
        expectThrows(IllegalArgumentException.class, () ->
                TimeUtils.checkPositiveMultiple(TimeValue.ZERO, TimeUnit.SECONDS, new ParseField("foo")));
    }

    public void testCheckPositiveMultiple_GivenPositiveNonMultiple() {
        expectThrows(IllegalArgumentException.class, () ->
                TimeUtils.checkPositiveMultiple(TimeValue.timeValueMillis(500), TimeUnit.SECONDS, new ParseField("foo")));
    }

    public void testCheckPositiveMultiple_GivenPositiveMultiple() {
        TimeUtils.checkPositiveMultiple(TimeValue.timeValueMillis(1), TimeUnit.MILLISECONDS, new ParseField("foo"));
    }

    public void testCheckNonNegativeMultiple_GivenNegative() {
        expectThrows(IllegalArgumentException.class, () ->
                TimeUtils.checkNonNegativeMultiple(TimeValue.timeValueMillis(-1), TimeUnit.MILLISECONDS, new ParseField("foo")));
    }

    public void testCheckNonNegativeMultiple_GivenZero() {
        TimeUtils.checkNonNegativeMultiple(TimeValue.ZERO, TimeUnit.SECONDS, new ParseField("foo"));
    }

    public void testCheckNonNegativeMultiple_GivenPositiveNonMultiple() {
        expectThrows(IllegalArgumentException.class, () ->
                TimeUtils.checkNonNegativeMultiple(TimeValue.timeValueMillis(500), TimeUnit.SECONDS, new ParseField("foo")));
    }

    public void testCheckNonNegativeMultiple_GivenPositiveMultiple() {
        TimeUtils.checkNonNegativeMultiple(TimeValue.timeValueMillis(1), TimeUnit.MILLISECONDS, new ParseField("foo"));
    }
}
