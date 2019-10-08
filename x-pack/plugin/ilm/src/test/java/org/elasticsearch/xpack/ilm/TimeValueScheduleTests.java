/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

public class TimeValueScheduleTests extends ESTestCase {

    private long start;
    private TimeValue interval;

    public TimeValueSchedule createRandomInstance() {
        return new TimeValueSchedule(createRandomTimeValue());
    }

    private TimeValue createRandomTimeValue() {
        return new TimeValue(randomLongBetween(1, 10000), randomFrom(TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS));
    }

    @Before
    public void setUpStartAndInterval() {
        // start with random epoch between 1/1/1970 and 31/12/2035 so that start is not
        // so large such that (start + interval) > Long.MAX
        start = randomLongBetween(0, 2082672000000L);
        interval = createRandomTimeValue();
    }

    public void testHashcodeAndEquals() {
        for (int i = 0; i < 20; i++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomInstance(),
                    instance -> new TimeValueSchedule(instance.getInterval()),
                    instance -> new TimeValueSchedule(randomValueOtherThan(instance.getInterval(), () -> createRandomTimeValue())));
        }
    }

    public void testNextScheduledTimeFirstTriggerNotReached() {
        long triggerTime = start + interval.millis();
        long now = start + randomLongBetween(0, interval.millis() - 1);
        TimeValueSchedule schedule = new TimeValueSchedule(interval);
        assertEquals(triggerTime, schedule.nextScheduledTimeAfter(start, now));
    }

    public void testNextScheduledTimeAtFirstInterval() {
        long triggerTime = start + 2 * interval.millis();
        long now = start + interval.millis();
        TimeValueSchedule schedule = new TimeValueSchedule(interval);
        assertEquals(triggerTime, schedule.nextScheduledTimeAfter(start, now));
    }

    public void testNextScheduledTimeAtStartTime() {
        long triggerTime = start + interval.millis();
        TimeValueSchedule schedule = new TimeValueSchedule(interval);
        assertEquals(triggerTime, schedule.nextScheduledTimeAfter(start, start));
    }

    public void testNextScheduledTimeAfterFirstTrigger() {
        long numberIntervalsPassed = randomLongBetween(0, 10000);
        long triggerTime = start + (numberIntervalsPassed + 1) * interval.millis();
        long now = start
                + randomLongBetween(numberIntervalsPassed * interval.millis(), (numberIntervalsPassed + 1) * interval.millis() - 1);
        TimeValueSchedule schedule = new TimeValueSchedule(interval);
        assertEquals(triggerTime, schedule.nextScheduledTimeAfter(start, now));
    }

    public void testInvalidInterval() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new TimeValueSchedule(new TimeValue(0)));
        assertEquals("interval must be greater than 0 milliseconds", exception.getMessage());
    }
}
