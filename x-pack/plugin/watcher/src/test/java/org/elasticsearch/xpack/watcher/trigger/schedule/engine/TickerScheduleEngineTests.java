/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.engine;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayOfWeek;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.WeekTimes;
import org.junit.After;
import org.junit.Before;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.daily;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.weekly;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class TickerScheduleEngineTests extends ESTestCase {

    private TickerScheduleTriggerEngine engine;
    protected ClockMock clock = ClockMock.frozen();

    @Before
    public void init() throws Exception {
        engine = createEngine();
    }

    private TickerScheduleTriggerEngine createEngine() {
        Settings settings = Settings.EMPTY;
        // having a low value here speeds up the tests tremendously, we still want to run with the defaults every now and then
        if (usually()) {
            settings = Settings.builder().put(TickerScheduleTriggerEngine.TICKER_INTERVAL_SETTING.getKey(), "10ms").build();
        }
        return new TickerScheduleTriggerEngine(settings, mock(ScheduleRegistry.class), clock);
    }

    private void advanceClockIfNeeded(ZonedDateTime newCurrentDateTime) {
        clock.setTime(newCurrentDateTime);
    }

    @After
    public void cleanup() throws Exception {
        engine.stop();
    }

    public void testStart() throws Exception {
        int count = randomIntBetween(2, 5);
        final CountDownLatch firstLatch = new CountDownLatch(count);
        final CountDownLatch secondLatch = new CountDownLatch(count);
        List<Watch> watches = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            watches.add(createWatch(String.valueOf(i), interval("1s")));
        }
        final BitSet bits = new BitSet(count);

        engine.register(new Consumer<Iterable<TriggerEvent>>() {
            @Override
            public void accept(Iterable<TriggerEvent> events) {
                for (TriggerEvent event : events) {
                    int index = Integer.parseInt(event.jobName());
                    if (bits.get(index) == false) {
                        logger.info("job [{}] first fire", index);
                        bits.set(index);
                        firstLatch.countDown();
                    } else {
                        logger.info("job [{}] second fire", index);
                        secondLatch.countDown();
                    }
                }
            }
        });

        engine.start(watches);
        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        if (firstLatch.await(3 * count, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        if (secondLatch.await(3 * count, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }
        engine.stop();
        assertThat(bits.cardinality(), is(count));
    }

    public void testAddHourly() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.emptySet());
        engine.register(new Consumer<Iterable<TriggerEvent>>() {
            @Override
            public void accept(Iterable<TriggerEvent> events) {
                for (TriggerEvent event : events) {
                    assertThat(event.jobName(), is(name));
                    logger.info("triggered job on [{}]", clock);
                }
                latch.countDown();
            }
        });

        int randomMinute = randomIntBetween(0, 59);
        ZonedDateTime testNowTime = clock.instant()
            .atZone(ZoneOffset.UTC)
            .with(ChronoField.MINUTE_OF_HOUR, randomMinute)
            .with(ChronoField.SECOND_OF_MINUTE, 59);

        ZonedDateTime scheduledTime = testNowTime.plusSeconds(2);
        logger.info("Setting current time to [{}], job execution time [{}]", testNowTime, scheduledTime);

        clock.setTime(testNowTime);
        engine.add(createWatch(name, daily().at(scheduledTime.getHour(), scheduledTime.getMinute()).build()));
        advanceClockIfNeeded(scheduledTime);

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }
    }

    public void testAddDaily() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.emptySet());

        engine.register(new Consumer<Iterable<TriggerEvent>>() {
            @Override
            public void accept(Iterable<TriggerEvent> events) {
                for (TriggerEvent event : events) {
                    assertThat(event.jobName(), is(name));
                    logger.info("triggered job on [{}]", clock.instant().atZone(ZoneOffset.UTC));
                    latch.countDown();
                }
            }
        });

        int randomHour = randomIntBetween(0, 23);
        int randomMinute = randomIntBetween(0, 59);

        ZonedDateTime testNowTime = clock.instant()
            .atZone(ZoneOffset.UTC)
            .with(ChronoField.HOUR_OF_DAY, randomHour)
            .with(ChronoField.MINUTE_OF_HOUR, randomMinute)
            .with(ChronoField.SECOND_OF_MINUTE, 59);

        ZonedDateTime scheduledTime = testNowTime.plusSeconds(2);
        logger.info("Setting current time to [{}], job execution time [{}]", testNowTime, scheduledTime);

        clock.setTime(testNowTime);
        engine.add(createWatch(name, daily().at(scheduledTime.getHour(), scheduledTime.getMinute()).build()));
        advanceClockIfNeeded(scheduledTime);

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }
    }

    public void testAddWeekly() throws Exception {
        final String name = "job_name";
        final CountDownLatch latch = new CountDownLatch(1);
        engine.start(Collections.emptySet());
        engine.register(new Consumer<Iterable<TriggerEvent>>() {
            @Override
            public void accept(Iterable<TriggerEvent> events) {
                for (TriggerEvent event : events) {
                    assertThat(event.jobName(), is(name));
                    logger.info("triggered job");
                }
                latch.countDown();
            }
        });

        int randomHour = randomIntBetween(0, 23);
        int randomMinute = randomIntBetween(0, 59);
        int randomDay = randomIntBetween(1, 7);

        ZonedDateTime testNowTime = clock.instant()
            .atZone(ZoneOffset.UTC)
            .with(ChronoField.DAY_OF_WEEK, randomDay)
            .with(ChronoField.HOUR_OF_DAY, randomHour)
            .with(ChronoField.MINUTE_OF_HOUR, randomMinute)
            .with(ChronoField.SECOND_OF_MINUTE, 59);

        ZonedDateTime scheduledTime = testNowTime.plusSeconds(2);

        logger.info("Setting current time to [{}], job execution time [{}]", testNowTime, scheduledTime);
        clock.setTime(testNowTime);

        // fun part here (aka WTF): DayOfWeek with Joda is MON-SUN, starting at 1
        // DayOfWeek with Watcher is SUN-SAT, starting at 1
        int watcherDay = (scheduledTime.getDayOfWeek().getValue() % 7) + 1;
        engine.add(
            createWatch(
                name,
                weekly().time(
                    WeekTimes.builder().on(DayOfWeek.resolve(watcherDay)).at(scheduledTime.getHour(), scheduledTime.getMinute()).build()
                ).build()
            )
        );
        advanceClockIfNeeded(scheduledTime);

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }
    }

    public void testAddSameJobSeveralTimesAndExecutedOnce() throws InterruptedException {
        engine.start(Collections.emptySet());

        final CountDownLatch firstLatch = new CountDownLatch(1);
        final CountDownLatch secondLatch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(0);
        engine.register(new Consumer<Iterable<TriggerEvent>>() {
            @Override
            public void accept(Iterable<TriggerEvent> events) {
                events.forEach(event -> {
                    if (counter.getAndIncrement() == 0) {
                        firstLatch.countDown();
                    } else {
                        secondLatch.countDown();
                    }
                });
            }
        });

        int times = scaledRandomIntBetween(3, 30);
        for (int i = 0; i < times; i++) {
            engine.add(createWatch("_id", interval("1s")));
        }

        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        if (firstLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        if (secondLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        // ensure job was only called twice independent from its name
        assertThat(counter.get(), is(2));
    }

    public void testAddOnlyWithNewSchedule() {
        engine.start(Collections.emptySet());

        // add watch with schedule
        Watch oncePerSecondWatch = createWatch("_id", interval("1s"));
        engine.add(oncePerSecondWatch);
        TickerScheduleTriggerEngine.ActiveSchedule activeSchedule = engine.getSchedules().get("_id");
        engine.add(oncePerSecondWatch);
        assertThat(engine.getSchedules().get("_id"), is(activeSchedule));

        // add watch with same id but different watch
        Watch oncePerMinuteWatch = createWatch("_id", interval("1m"));
        engine.add(oncePerMinuteWatch);
        assertThat(engine.getSchedules().get("_id"), not(is(activeSchedule)));
    }

    /**
     * This test verifies that a watch with a valid lastCheckedTime executes before the interval time to ensure the job resumes waiting
     * from the same point it left off before the reallocation / restart
     */
    public void testWatchWithLastCheckedTimeExecutesBeforeInitialInterval() throws Exception {
        final var firstLatch = new CountDownLatch(1);
        final var secondLatch = new CountDownLatch(1);

        Watch watch = new Watch(
            "watch",
            new ScheduleTrigger(interval("1s")),
            new ExecutableNoneInput(),
            InternalAlwaysCondition.INSTANCE,
            null,
            null,
            Collections.emptyList(),
            null,
            new WatchStatus(-1L, null, null, clock.instant().minusMillis(500).atZone(ZoneOffset.UTC), null, null, null),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );

        var watches = Collections.singletonList(watch);

        var runCount = new AtomicInteger(0);

        engine.register(events -> {
            for (TriggerEvent ignored : events) {
                if (runCount.getAndIncrement() == 0) {
                    logger.info("job first fire");
                    firstLatch.countDown();
                } else {
                    logger.info("job second fire");
                    secondLatch.countDown();
                }
            }
        });

        engine.start(watches);
        advanceClockIfNeeded(clock.instant().plusMillis(510).atZone(ZoneOffset.UTC));
        if (firstLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        if (secondLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        assertThat(runCount.get(), is(2));

        engine.stop();
    }

    /**
     * This test verifies that a watch without a lastCheckedTime but with a valid activationTime executes before the interval time to
     * ensure the job resumes waiting from the same point it left off before the reallocation / restart
     */
    public void testWatchWithNoLastCheckedTimeButHasActivationTimeExecutesBeforeInitialInterval() throws Exception {
        final var firstLatch = new CountDownLatch(1);
        final var secondLatch = new CountDownLatch(1);

        Watch watch = new Watch(
            "watch",
            new ScheduleTrigger(interval("1s")),
            new ExecutableNoneInput(),
            InternalAlwaysCondition.INSTANCE,
            null,
            null,
            Collections.emptyList(),
            null,
            new WatchStatus(
                -1L,
                new WatchStatus.State(true, clock.instant().minusMillis(500).atZone(ZoneOffset.UTC)),
                null,
                null,
                null,
                null,
                null
            ),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );

        var watches = Collections.singletonList(watch);

        var runCount = new AtomicInteger(0);

        engine.register(events -> {
            for (TriggerEvent ignored : events) {
                if (runCount.getAndIncrement() == 0) {
                    logger.info("job first fire");
                    firstLatch.countDown();
                } else {
                    logger.info("job second fire");
                    secondLatch.countDown();
                }
            }
        });

        engine.start(watches);
        advanceClockIfNeeded(clock.instant().plusMillis(510).atZone(ZoneOffset.UTC));
        if (firstLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        if (secondLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        assertThat(runCount.get(), is(2));

        engine.stop();
    }

    /**
     * This test verifies that a watch added after service start with a lastCheckedTime executes before the interval time to ensure the job
     * resumes waiting from the same point it left off before the reallocation / restart
     */
    public void testAddWithLastCheckedTimeExecutesBeforeInitialInterval() throws Exception {
        final var firstLatch = new CountDownLatch(1);
        final var secondLatch = new CountDownLatch(1);

        Watch watch = new Watch(
            "watch",
            new ScheduleTrigger(interval("1s")),
            new ExecutableNoneInput(),
            InternalAlwaysCondition.INSTANCE,
            null,
            null,
            Collections.emptyList(),
            null,
            new WatchStatus(-1L, null, null, clock.instant().minusMillis(500).atZone(ZoneOffset.UTC), null, null, null),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );

        var runCount = new AtomicInteger(0);

        engine.register(events -> {
            for (TriggerEvent ignored : events) {
                if (runCount.getAndIncrement() == 0) {
                    logger.info("job first fire");
                    firstLatch.countDown();
                } else {
                    logger.info("job second fire");
                    secondLatch.countDown();
                }
            }
        });

        engine.start(Collections.emptyList());
        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        engine.add(watch);

        advanceClockIfNeeded(clock.instant().plusMillis(510).atZone(ZoneOffset.UTC));
        if (firstLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        if (secondLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        assertThat(runCount.get(), is(2));

        engine.stop();
    }

    /**
     * This test verifies that a watch added after service start without a lastCheckedTime but with a valid activationTime executes before
     * the interval time to ensure the job resumes waiting from the same point it left off before the reallocation / restart
     */
    public void testAddWithNoLastCheckedTimeButHasActivationTimeExecutesBeforeInitialInterval() throws Exception {
        final var firstLatch = new CountDownLatch(1);
        final var secondLatch = new CountDownLatch(1);

        Watch watch = new Watch(
            "watch",
            new ScheduleTrigger(interval("1s")),
            new ExecutableNoneInput(),
            InternalAlwaysCondition.INSTANCE,
            null,
            null,
            Collections.emptyList(),
            null,
            new WatchStatus(
                -1L,
                new WatchStatus.State(true, clock.instant().minusMillis(500).atZone(ZoneOffset.UTC)),
                null,
                null,
                null,
                null,
                null
            ),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );

        var runCount = new AtomicInteger(0);

        engine.register(events -> {
            for (TriggerEvent ignored : events) {
                if (runCount.getAndIncrement() == 0) {
                    logger.info("job first fire");
                    firstLatch.countDown();
                } else {
                    logger.info("job second fire");
                    secondLatch.countDown();
                }
            }
        });

        engine.start(Collections.emptyList());
        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        engine.add(watch);

        advanceClockIfNeeded(clock.instant().plusMillis(510).atZone(ZoneOffset.UTC));
        if (firstLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        advanceClockIfNeeded(clock.instant().plusMillis(1100).atZone(ZoneOffset.UTC));
        if (secondLatch.await(3, TimeUnit.SECONDS) == false) {
            fail("waiting too long for all watches to be triggered");
        }

        assertThat(runCount.get(), is(2));

        engine.stop();
    }

    private Watch createWatch(String name, Schedule schedule) {
        return new Watch(
            name,
            new ScheduleTrigger(schedule),
            new ExecutableNoneInput(),
            InternalAlwaysCondition.INSTANCE,
            null,
            null,
            Collections.emptyList(),
            null,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );
    }
}
