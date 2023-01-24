/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.transform.Transform;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class TransformSchedulerTests extends ESTestCase {

    private static final TimeValue TEST_SCHEDULER_FREQUENCY = TimeValue.timeValueSeconds(1);
    private static final Settings SETTINGS = Settings.builder()
        .put(Transform.SCHEDULER_FREQUENCY.getKey(), TEST_SCHEDULER_FREQUENCY)
        .build();

    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    public void testScheduling() {
        String transformId = "test-with-fake-clock";
        int frequencySeconds = 5;
        TimeValue frequency = TimeValue.timeValueSeconds(frequencySeconds);
        TransformTaskParams transformTaskParams = new TransformTaskParams(transformId, Version.CURRENT, frequency, false);
        FakeClock clock = new FakeClock(Instant.ofEpochMilli(0));
        CopyOnWriteArrayList<TransformScheduler.Event> events = new CopyOnWriteArrayList<>();
        TransformScheduler.Listener listener = events::add;

        TransformScheduler transformScheduler = new TransformScheduler(clock, threadPool, SETTINGS);
        transformScheduler.registerTransform(transformTaskParams, listener);
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(new TransformScheduledTask(transformId, frequency, 0L, 0, 5000, listener))
        );
        assertThat(events, hasSize(1));

        for (int i = 0; i < frequencySeconds; ++i) {
            transformScheduler.processScheduledTasks();
            assertThat(
                transformScheduler.getTransformScheduledTasks(),
                contains(new TransformScheduledTask(transformId, frequency, 0L, 0, 5000, listener))
            );
            assertThat(events, hasSize(1));
            clock.advanceTimeBy(Duration.ofMillis(1001));
        }
        assertThat(clock.currentTime, is(equalTo(Instant.ofEpochMilli(5005))));

        for (int i = 0; i < frequencySeconds; ++i) {
            transformScheduler.processScheduledTasks();
            assertThat(
                transformScheduler.getTransformScheduledTasks(),
                contains(new TransformScheduledTask(transformId, frequency, 5005L, 0, 10005, listener))
            );
            assertThat(events, hasSize(2));
            clock.advanceTimeBy(Duration.ofMillis(1001));
        }
        assertThat(clock.currentTime, is(equalTo(Instant.ofEpochMilli(10010))));

        for (int i = 0; i < frequencySeconds; ++i) {
            transformScheduler.processScheduledTasks();
            assertThat(
                transformScheduler.getTransformScheduledTasks(),
                contains(new TransformScheduledTask(transformId, frequency, 10010L, 0, 15010, listener))
            );
            assertThat(events, hasSize(3));
            clock.advanceTimeBy(Duration.ofMillis(1001));
        }
        assertThat(clock.currentTime, is(equalTo(Instant.ofEpochMilli(15015))));

        assertThat(events.get(0), is(equalTo(new TransformScheduler.Event(transformId, 0, 0))));
        assertThat(events.get(1), is(equalTo(new TransformScheduler.Event(transformId, 5000, 5005))));
        assertThat(events.get(2), is(equalTo(new TransformScheduler.Event(transformId, 10005, 10010))));

        transformScheduler.deregisterTransform(transformId);
        assertThat(transformScheduler.getTransformScheduledTasks(), is(empty()));

        transformScheduler.stop();
    }

    public void testSchedulingWithFailures() {
        String transformId = "test-failure-with-fake-clock";
        TimeValue frequency = TimeValue.timeValueHours(1);
        TransformTaskParams transformTaskParams = new TransformTaskParams(transformId, Version.CURRENT, frequency, false);
        FakeClock clock = new FakeClock(Instant.ofEpochMilli(0));
        CopyOnWriteArrayList<TransformScheduler.Event> events = new CopyOnWriteArrayList<>();
        TransformScheduler.Listener listener = events::add;

        TransformScheduler transformScheduler = new TransformScheduler(clock, threadPool, SETTINGS);
        transformScheduler.registerTransform(transformTaskParams, listener);
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(new TransformScheduledTask(transformId, frequency, 0L, 0, 60 * 60 * 1000, listener))
        );
        assertThat(events, hasSize(1));

        for (int i = 0; i < 60; ++i) {
            transformScheduler.processScheduledTasks();
            assertThat(
                transformScheduler.getTransformScheduledTasks(),
                contains(new TransformScheduledTask(transformId, frequency, 0L, 0, 60 * 60 * 1000, listener))
            );
            assertThat(events, hasSize(1));
            clock.advanceTimeBy(Duration.ofMillis(TEST_SCHEDULER_FREQUENCY.millis()));
        }
        assertThat(clock.currentTime, is(equalTo(Instant.ofEpochSecond(60))));

        transformScheduler.handleTransformFailureCountChanged(transformId, 1);
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(new TransformScheduledTask(transformId, frequency, 0L, 1, 5 * 1000, listener))
        );
        assertThat(events, hasSize(1));

        transformScheduler.processScheduledTasks();
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(new TransformScheduledTask(transformId, frequency, 60 * 1000L, 1, 65 * 1000, listener))
        );
        assertThat(events, hasSize(2));

        assertThat(
            events,
            contains(new TransformScheduler.Event(transformId, 0, 0), new TransformScheduler.Event(transformId, 5 * 1000, 60 * 1000))
        );

        transformScheduler.deregisterTransform(transformId);
        assertThat(transformScheduler.getTransformScheduledTasks(), is(empty()));

        transformScheduler.stop();
    }

    public void testConcurrentProcessing() throws Exception {
        String transformId = "test-with-fake-clock-concurrent";
        int frequencySeconds = 5;
        TimeValue frequency = TimeValue.timeValueSeconds(frequencySeconds);
        TransformTaskParams transformTaskParams = new TransformTaskParams(transformId, Version.CURRENT, frequency, false);
        FakeClock clock = new FakeClock(Instant.ofEpochMilli(0));
        CopyOnWriteArrayList<TransformScheduler.Event> events = new CopyOnWriteArrayList<>();
        TransformScheduler.Listener listener = events::add;

        TransformScheduler transformScheduler = new TransformScheduler(clock, threadPool, SETTINGS);
        transformScheduler.registerTransform(transformTaskParams, listener);
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(new TransformScheduledTask(transformId, frequency, 0L, 0, 5000, listener))
        );
        assertThat(events, hasSize(1));

        clock.advanceTimeBy(Duration.ofMillis(5000));

        List<Callable<Void>> concurrentProcessingTasks = new ArrayList<>(10);
        for (int i = 0; i < 10; ++i) {
            concurrentProcessingTasks.add(() -> {
                transformScheduler.processScheduledTasks();
                return null;
            });
        }
        List<Future<Void>> futures = threadPool.generic().invokeAll(concurrentProcessingTasks);
        for (Future<Void> future : futures) {
            future.get();
        }

        assertThat(events, hasSize(2));
        assertThat(
            events,
            contains(new TransformScheduler.Event(transformId, 0, 0), new TransformScheduler.Event(transformId, 5 * 1000, 5 * 1000))
        );
    }

    public void testConcurrentModifications() {
        String transformId = "test-with-fake-clock-concurrent";
        int frequencySeconds = 5;
        TimeValue frequency = TimeValue.timeValueSeconds(frequencySeconds);
        TransformTaskParams transformTaskParams = new TransformTaskParams(transformId, Version.CURRENT, frequency, false);
        FakeClock clock = new FakeClock(Instant.ofEpochMilli(0));
        CopyOnWriteArrayList<TransformScheduler.Event> events = new CopyOnWriteArrayList<>();

        TransformScheduler transformScheduler = new TransformScheduler(clock, threadPool, SETTINGS);
        TransformScheduler.Listener taskModifyingListener = new TransformScheduler.Listener() {
            private boolean firstTime = true;

            @Override
            public void triggered(TransformScheduler.Event event) {
                events.add(event);
                assertThat(event.transformId(), is(equalTo(transformId)));
                if (firstTime) {
                    firstTime = false;
                } else {
                    transformScheduler.handleTransformFailureCountChanged(transformId, 666);
                }
            }
        };
        transformScheduler.registerTransform(transformTaskParams, taskModifyingListener);
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(new TransformScheduledTask(transformId, frequency, 0L, 0, 5000, taskModifyingListener))
        );
        assertThat(events, hasSize(1));

        clock.advanceTimeBy(Duration.ofMillis(5000));
        transformScheduler.processScheduledTasks();
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(new TransformScheduledTask(transformId, frequency, 5000L, 666, 3605000, taskModifyingListener))
        );
        assertThat(events, hasSize(2));
        assertThat(
            events,
            contains(new TransformScheduler.Event(transformId, 0, 0), new TransformScheduler.Event(transformId, 5 * 1000, 5 * 1000))
        );
    }

    public void testWithSystemClock() throws Exception {
        String transformId = "test-with-system-clock";
        TimeValue frequency = TimeValue.timeValueSeconds(1);
        TransformTaskParams transformTaskParams = new TransformTaskParams(transformId, Version.CURRENT, frequency, false);
        Clock clock = Clock.systemUTC();
        CopyOnWriteArrayList<TransformScheduler.Event> events = new CopyOnWriteArrayList<>();

        TransformScheduler transformScheduler = new TransformScheduler(clock, threadPool, SETTINGS);
        transformScheduler.start();
        transformScheduler.registerTransform(transformTaskParams, events::add);
        assertThat(events, hasSize(1));

        assertBusy(() -> assertThat(events, hasSize(greaterThanOrEqualTo(3))), 20, TimeUnit.SECONDS);

        assertThat(events, hasSize(greaterThanOrEqualTo(3)));
        assertThat(events.get(0).transformId(), is(equalTo(transformId)));
        assertThat(events.get(1).transformId(), is(equalTo(transformId)));
        assertThat(events.get(2).transformId(), is(equalTo(transformId)));
        assertThat(events.get(1).scheduledTime() - events.get(0).triggeredTime(), is(equalTo(frequency.millis())));
        assertThat(events.get(2).scheduledTime() - events.get(1).triggeredTime(), is(equalTo(frequency.millis())));

        transformScheduler.deregisterTransform(transformId);
        transformScheduler.stop();
    }

    public void testScheduledTransformTaskEqualsAndHashCode() {
        Supplier<TransformScheduler.Listener> listenerSupplier = () -> new TransformScheduler.Listener() {
            @Override
            public void triggered(TransformScheduler.Event event) {}

            @Override
            public boolean equals(Object o) {
                return this == o;
            }

            @Override
            public int hashCode() {
                return 123;
            }
        };
        TransformScheduler.Listener listener1 = listenerSupplier.get();
        TransformScheduler.Listener listener2 = listenerSupplier.get();
        TransformScheduledTask task1 = new TransformScheduledTask("transform-id", TimeValue.timeValueSeconds(10), 123L, 0, 50, listener1);
        TransformScheduledTask task2 = new TransformScheduledTask("transform-id", TimeValue.timeValueSeconds(10), 123L, 0, 50, listener2);
        // Verify the tasks are not equal. The equality check for listeners is performed using their identity.
        assertThat(task1, is(not(equalTo(task2))));
        assertThat(task1.hashCode(), is(not(equalTo(task2.hashCode()))));
    }

    public void testRegisterMultipleTransforms() {
        String transformId1 = "test-register-transforms-1";
        String transformId2 = "test-register-transforms-2";
        String transformId3 = "test-register-transforms-3";
        TimeValue frequency = TimeValue.timeValueSeconds(5);
        TransformTaskParams transformTaskParams1 = new TransformTaskParams(transformId1, Version.CURRENT, frequency, false);
        TransformTaskParams transformTaskParams2 = new TransformTaskParams(transformId2, Version.CURRENT, frequency, false);
        TransformTaskParams transformTaskParams3 = new TransformTaskParams(transformId3, Version.CURRENT, frequency, false);
        FakeClock clock = new FakeClock(Instant.ofEpochMilli(0));
        CopyOnWriteArrayList<TransformScheduler.Event> events = new CopyOnWriteArrayList<>();
        TransformScheduler.Listener listener = events::add;

        TransformScheduler transformScheduler = new TransformScheduler(clock, threadPool, SETTINGS);
        transformScheduler.registerTransform(transformTaskParams1, listener);
        transformScheduler.registerTransform(transformTaskParams2, listener);
        transformScheduler.registerTransform(transformTaskParams3, listener);
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(
                new TransformScheduledTask(transformId1, frequency, 0L, 0, 5000, listener),
                new TransformScheduledTask(transformId2, frequency, 0L, 0, 5000, listener),
                new TransformScheduledTask(transformId3, frequency, 0L, 0, 5000, listener)
            )
        );
        assertThat(events, hasSize(3));
        assertThat(events.get(0).transformId(), is(equalTo(transformId1)));
        assertThat(events.get(1).transformId(), is(equalTo(transformId2)));
        assertThat(events.get(2).transformId(), is(equalTo(transformId3)));
    }

    public void testMultipleTransformsEligibleForProcessingAtOnce() {
        String transformId1 = "test-register-transforms-1";
        String transformId2 = "test-register-transforms-2";
        String transformId3 = "test-register-transforms-3";
        TimeValue frequency = TimeValue.timeValueSeconds(5);
        TransformTaskParams transformTaskParams1 = new TransformTaskParams(transformId1, Version.CURRENT, frequency, false);
        TransformTaskParams transformTaskParams2 = new TransformTaskParams(transformId2, Version.CURRENT, frequency, false);
        TransformTaskParams transformTaskParams3 = new TransformTaskParams(transformId3, Version.CURRENT, frequency, false);
        FakeClock clock = new FakeClock(Instant.ofEpochMilli(0));
        CopyOnWriteArrayList<TransformScheduler.Event> events = new CopyOnWriteArrayList<>();
        TransformScheduler.Listener listener = events::add;

        TransformScheduler transformScheduler = new TransformScheduler(clock, threadPool, SETTINGS);
        transformScheduler.registerTransform(transformTaskParams1, listener);
        transformScheduler.registerTransform(transformTaskParams2, listener);
        transformScheduler.registerTransform(transformTaskParams3, listener);
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(
                new TransformScheduledTask(transformId1, frequency, 0L, 0, 5000, listener),
                new TransformScheduledTask(transformId2, frequency, 0L, 0, 5000, listener),
                new TransformScheduledTask(transformId3, frequency, 0L, 0, 5000, listener)
            )
        );
        assertThat(events, hasSize(3));
        assertThat(events.get(0).transformId(), is(equalTo(transformId1)));
        assertThat(events.get(1).transformId(), is(equalTo(transformId2)));
        assertThat(events.get(2).transformId(), is(equalTo(transformId3)));

        clock.advanceTimeBy(Duration.ofSeconds(5));  // Advance time to the next scheduled time of all 3 transforms
        transformScheduler.processScheduledTasks();
        assertThat(
            transformScheduler.getTransformScheduledTasks(),
            contains(
                new TransformScheduledTask(transformId1, frequency, 5000L, 0, 10000, listener),
                new TransformScheduledTask(transformId2, frequency, 5000L, 0, 10000, listener),
                new TransformScheduledTask(transformId3, frequency, 5000L, 0, 10000, listener)
            )
        );
        assertThat(events, hasSize(6));
        assertThat(events.get(3).transformId(), is(equalTo(transformId1)));
        assertThat(events.get(4).transformId(), is(equalTo(transformId2)));
        assertThat(events.get(5).transformId(), is(equalTo(transformId3)));
    }

    private static class FakeClock extends Clock {

        private Instant currentTime;

        FakeClock(Instant time) {
            assertThat(time, is(notNullValue()));
            currentTime = time;
        }

        public void setCurrentTime(Instant time) {
            // We cannot go back in time.
            assertThat(time, is(greaterThanOrEqualTo(currentTime)));
            currentTime = time;
        }

        public void advanceTimeBy(Duration duration) {
            assertThat(duration, is(notNullValue()));
            setCurrentTime(currentTime.plus(duration));
        }

        @Override
        public Instant instant() {
            return currentTime;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.systemDefault();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
