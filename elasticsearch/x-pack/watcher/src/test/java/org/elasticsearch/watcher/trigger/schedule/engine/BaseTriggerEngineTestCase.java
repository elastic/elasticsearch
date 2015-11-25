/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.engine;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.Schedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.support.DayOfWeek;
import org.elasticsearch.watcher.trigger.schedule.support.WeekTimes;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.watcher.trigger.schedule.Schedules.daily;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.hourly;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.weekly;
import static org.hamcrest.Matchers.is;
import static org.joda.time.DateTimeZone.UTC;

public abstract class BaseTriggerEngineTestCase extends ESTestCase {
    private TriggerEngine engine;

    @Before
    public void init() throws Exception {
        engine = createEngine();
    }

    protected abstract TriggerEngine createEngine();

    @After
    public void cleanup() throws Exception {
        engine.stop();
    }

    public void testStart() throws Exception {
        int count = randomIntBetween(2, 5);
        final CountDownLatch latch = new CountDownLatch(count);
        List<TriggerEngine.Job> jobs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            jobs.add(new SimpleJob(String.valueOf(i), interval("1s")));
        }
        final BitSet bits = new BitSet(count);
        engine.register(new TriggerEngine.Listener() {

            @Override
            public void triggered(Iterable<TriggerEvent> events) {
                for (TriggerEvent event : events) {
                    int index = Integer.parseInt(event.jobName());
                    if (!bits.get(index)) {
                        logger.info("job [{}] first fire", index);
                        bits.set(index);
                    } else {
                        latch.countDown();
                        logger.info("job [{}] second fire", index);
                    }
                }
            }
        });
        engine.start(jobs);
        if (!latch.await(3 * count, TimeUnit.SECONDS)) {
            fail("waiting too long for all watches to be triggered");
        }
        engine.stop();
        assertThat(bits.cardinality(), is(count));
    }

    public void testAddHourly() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.<TriggerEngine.Job>emptySet());
        engine.register(new TriggerEngine.Listener() {

            @Override
            public void triggered(Iterable<TriggerEvent> events) {
                for (TriggerEvent event : events) {
                    assertThat(event.jobName(), is(name));
                    logger.info("triggered job on [{}]", SystemClock.INSTANCE.nowUTC());
                }
                latch.countDown();
            }
        });
        DateTime now = new DateTime(UTC);
        Minute minOfHour = new Minute(now);
        if (now.getSecondOfMinute() < 58) {
            minOfHour.inc(1);
        } else {
            minOfHour.inc(2);
        }
        int minute = minOfHour.value;
        logger.info("scheduling hourly job [{}]", minute);
        logger.info("current date [{}]", now);
        engine.add(new SimpleJob(name, hourly(minute)));
        long secondsToWait = now.getSecondOfMinute() < 29 ? 62 - now.getSecondOfMinute() : 122 - now.getSecondOfMinute();
        logger.info("waiting at least [{}] seconds for response", secondsToWait);
        if (!latch.await(secondsToWait, TimeUnit.SECONDS)) {
            fail("waiting too long for all watches to be triggered");
        }
    }

    public void testAddDaily() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.<TriggerEngine.Job>emptySet());
        engine.register(new TriggerEngine.Listener() {

            @Override
            public void triggered(Iterable<TriggerEvent> events) {
                for (TriggerEvent event : events) {
                    assertThat(event.jobName(), is(name));
                    logger.info("triggered job on [{}]", SystemClock.INSTANCE.nowUTC());
                    latch.countDown();
                }
            }
        });
        DateTime now = new DateTime(UTC);
        Minute minOfHour = new Minute(now);
        Hour hourOfDay = new Hour(now);
        boolean jumpedHour = now.getSecondOfMinute() < 29 ? minOfHour.inc(1) : minOfHour.inc(2);
        int minute = minOfHour.value;
        if (jumpedHour) {
            hourOfDay.inc(1);
        }
        int hour = hourOfDay.value;
        logger.info("scheduling hourly job [{}:{}]", hour, minute);
        logger.info("current date [{}]", now);
        engine.add(new SimpleJob(name, daily().at(hour, minute).build()));
        // 30 sec is the default idle time for the scheduler
        long secondsToWait = now.getSecondOfMinute() < 29 ? 62 - now.getSecondOfMinute() : 122 - now.getSecondOfMinute();
        logger.info("waiting at least [{}] seconds for response", secondsToWait);
        if (!latch.await(secondsToWait, TimeUnit.SECONDS)) {
            fail("waiting too long for all watches to be triggered");
        }
    }

    public void testAddWeekly() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.<TriggerEngine.Job>emptySet());
        engine.register(new TriggerEngine.Listener() {

            @Override
            public void triggered(Iterable<TriggerEvent> events) {
                for (TriggerEvent event : events) {
                    assertThat(event.jobName(), is(name));
                    logger.info("triggered job");
                }
                latch.countDown();
            }
        });
        DateTime now = new DateTime(UTC);
        Minute minOfHour = new Minute(now);
        Hour hourOfDay = new Hour(now);
        Day dayOfWeek = new Day(now);
        boolean jumpedHour = now.getSecondOfMinute() < 29 ? minOfHour.inc(1) : minOfHour.inc(2);
        int minute = minOfHour.value;
        if (jumpedHour && hourOfDay.inc(1)) {
            dayOfWeek.inc(1);
        }
        int hour = hourOfDay.value;
        DayOfWeek day = dayOfWeek.day();
        logger.info("scheduling hourly job [{} {}:{}]", day, hour, minute);
        logger.info("current date [{}]", now);
        engine.add(new SimpleJob(name, weekly().time(WeekTimes.builder().on(day).at(hour, minute).build()).build()));
        // 30 sec is the default idle time for the scheduler
        long secondsToWait = now.getSecondOfMinute() < 29 ? 62 - now.getSecondOfMinute() : 122 - now.getSecondOfMinute();
        logger.info("waiting at least [{}] seconds for response", secondsToWait);
        if (!latch.await(secondsToWait, TimeUnit.SECONDS)) {
            fail("waiting too long for all watches to be triggered");
        }
    }

    public void testAddSameJobSeveralTimes() {
        engine.start(Collections.<TriggerEngine.Job>emptySet());
        engine.register(new TriggerEngine.Listener() {

            @Override
            public void triggered(Iterable<TriggerEvent> events) {
                logger.info("triggered job");
            }
        });

        int times = scaledRandomIntBetween(3, 30);
        for (int i = 0; i < times; i++) {
            engine.add(new SimpleJob("_id", interval("10s")));
        }
    }

    static class SimpleJob implements TriggerEngine.Job {

        private final String name;
        private final ScheduleTrigger trigger;

        public SimpleJob(String name, Schedule schedule) {
            this.name = name;
            this.trigger = new ScheduleTrigger(schedule);
        }

        @Override
        public String id() {
            return name;
        }

        @Override
        public Trigger trigger() {
            return trigger;
        }
    }

    static class Hour {

        int value;

        Hour(DateTime time) {
            value = time.getHourOfDay();
        }

        /**
         * increments the hour and returns whether the day jumped. (note, only supports increment steps &lt; 24)
         */
        boolean inc(int inc) {
            value += inc;
            if (value > 23) {
                value %= 24;
                return true;
            }
            return false;
        }
    }

    static class Minute {

        int value;

        Minute(DateTime time) {
            value = time.getMinuteOfHour();
        }

        /**
         * increments the minute and returns whether the hour jumped. (note, only supports increment steps &lt; 60)
         */
        boolean inc(int inc) {
            value += inc;
            if (value > 59) {
                value %= 60;
                return true;
            }
            return false;
        }
    }

    static class Day {

        int value;

        Day(DateTime time) {
            value = time.getDayOfWeek() - 1;
        }

        /**
         * increments the minute and returns whether the week jumped. (note, only supports increment steps &lt; 8)
         */
        boolean inc(int inc) {
            value += inc;
            if (value > 6) {
                value %= 7;
                return true;
            }
            return false;
        }

        DayOfWeek day() {
            switch (value) {
                case 0 : return DayOfWeek.MONDAY;
                case 1 : return DayOfWeek.TUESDAY;
                case 2 : return DayOfWeek.WEDNESDAY;
                case 3 : return DayOfWeek.THURSDAY;
                case 4 : return DayOfWeek.FRIDAY;
                case 5 : return DayOfWeek.SATURDAY;
                default : return DayOfWeek.SUNDAY;
            }
        }
    }
}
