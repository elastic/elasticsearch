/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.quartz;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.Schedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.support.DayOfWeek;
import org.elasticsearch.watcher.trigger.schedule.support.WeekTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.watcher.trigger.schedule.Schedules.*;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 *
 */
@Slow
public class QuartzScheduleEngineTests extends ElasticsearchTestCase {

    private ThreadPool threadPool;
    private QuartzScheduleTriggerEngine engine;

    @Before
    public void init() throws Exception {
        WatcherPlugin plugin = new WatcherPlugin(ImmutableSettings.EMPTY);
        Settings settings = ImmutableSettings.builder()
                .put(plugin.additionalSettings())
                .put("name", "test")
                .build();
        threadPool = new ThreadPool(settings, null);
        engine = new QuartzScheduleTriggerEngine(ImmutableSettings.EMPTY, mock(ScheduleRegistry.class), threadPool, SystemClock.INSTANCE);
    }

    @After
    public void cleanup() throws Exception {
        engine.stop();
        threadPool.shutdownNow();
    }

    @Test
    public void testStart() throws Exception {
        int count = randomIntBetween(2, 5);
        final CountDownLatch latch = new CountDownLatch(count);
        List<TriggerEngine.Job> jobs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            jobs.add(new SimpleJob(String.valueOf(i), interval("3s")));
        }
        final BitSet bits = new BitSet(count);
        engine.register(new TriggerEngine.Listener() {
            @Override
            public void triggered(String jobName, TriggerEvent event) {
                int index = Integer.parseInt(jobName);
                if (!bits.get(index)) {
                    logger.info("job [" + index + "] first fire: " + new DateTime());
                    bits.set(index);
                } else {
                    latch.countDown();
                    logger.info("job [" + index + "] second fire: " + new DateTime());
                }
            }
        });
        engine.start(jobs);
        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting too long for all watches to be triggered");
        }
        engine.stop();
        assertThat(bits.cardinality(), is(count));
    }

    @Test
    public void testAdd_Hourly() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.<TriggerEngine.Job>emptySet());
        engine.register(new TriggerEngine.Listener() {
            @Override
            public void triggered(String jobName, TriggerEvent event) {
                assertThat(jobName, is(name));
                logger.info("triggered job on [{}]", new DateTime());
                latch.countDown();
            }
        });
        DateTime now = new DateTime(DateTimeZone.UTC);
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

    @Test
    public void testAdd_Daily() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.<TriggerEngine.Job>emptySet());
        engine.register(new TriggerEngine.Listener() {
            @Override
            public void triggered(String jobName, TriggerEvent event) {
                assertThat(jobName, is(name));
                logger.info("triggered job on [{}]", new DateTime());
                latch.countDown();
            }
        });
        DateTime now = new DateTime(DateTimeZone.UTC);
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
        // 30 sec is the default idle time of quartz
        long secondsToWait = now.getSecondOfMinute() < 29 ? 62 - now.getSecondOfMinute() : 122 - now.getSecondOfMinute();
        logger.info("waiting at least [{}] seconds for response", secondsToWait);
        if (!latch.await(secondsToWait, TimeUnit.SECONDS)) {
            fail("waiting too long for all watches to be triggered");
        }
    }

    @Test
    public void testAdd_Weekly() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.<TriggerEngine.Job>emptySet());
        engine.register(new TriggerEngine.Listener() {
            @Override
            public void triggered(String jobName, TriggerEvent event) {
                assertThat(jobName, is(name));
                logger.info("triggered job on [{}]", new DateTime());
                latch.countDown();
            }
        });
        DateTime now = new DateTime(DateTimeZone.UTC);
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
        // 30 sec is the default idle time of quartz
        long secondsToWait = now.getSecondOfMinute() < 29 ? 62 - now.getSecondOfMinute() : 122 - now.getSecondOfMinute();
        logger.info("waiting at least [{}] seconds for response", secondsToWait);
        if (!latch.await(secondsToWait, TimeUnit.SECONDS)) {
            fail("waiting too long for all watches to be triggered");
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
        public String name() {
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
         * increments the hour and returns whether the day jumped. (note, only supports increment steps < 24)
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
         * increments the minute and returns whether the hour jumped. (note, only supports increment steps < 60)
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
         * increments the minute and returns whether the week jumped. (note, only supports increment steps < 8)
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
