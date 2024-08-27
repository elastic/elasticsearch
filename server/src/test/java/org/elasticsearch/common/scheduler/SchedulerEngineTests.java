/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.scheduler;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.scheduler.SchedulerEngine.ActiveSchedule;
import org.elasticsearch.common.scheduler.SchedulerEngine.Job;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SchedulerEngineTests extends ESTestCase {

    public void testListenersThrowingExceptionsDoNotCauseOtherListenersToBeSkipped() throws InterruptedException {
        final Logger mockLogger = mock(Logger.class);
        final SchedulerEngine engine = new SchedulerEngine(Settings.EMPTY, Clock.systemUTC(), mockLogger);
        try {
            final List<Tuple<SchedulerEngine.Listener, AtomicBoolean>> listeners = new ArrayList<>();
            final int numberOfListeners = randomIntBetween(1, 32);
            int numberOfFailingListeners = 0;
            final CountDownLatch latch = new CountDownLatch(numberOfListeners);

            for (int i = 0; i < numberOfListeners; i++) {
                final AtomicBoolean trigger = new AtomicBoolean();
                final SchedulerEngine.Listener listener;
                if (randomBoolean()) {
                    listener = event -> {
                        if (trigger.compareAndSet(false, true)) {
                            latch.countDown();
                        } else {
                            fail("listener invoked twice");
                        }
                    };
                } else {
                    numberOfFailingListeners++;
                    listener = event -> {
                        if (trigger.compareAndSet(false, true)) {
                            // we count down the latch after this exception is caught and mock logged in SchedulerEngine#notifyListeners
                            throw new RuntimeException(getTestName());
                        } else {
                            fail("listener invoked twice");
                        }
                    };
                    doAnswer(invocationOnMock -> {
                        // this happens after the listener has been notified, threw an exception, and then mock logged the exception
                        latch.countDown();
                        return null;
                    }).when(mockLogger).warn(any(Supplier.class), any(RuntimeException.class));
                }
                listeners.add(Tuple.tuple(listener, trigger));
            }

            // randomize the order and register the listeners
            Collections.shuffle(listeners, random());
            listeners.stream().map(Tuple::v1).forEach(engine::register);

            final AtomicBoolean scheduled = new AtomicBoolean();
            engine.add(new SchedulerEngine.Job(getTestName(), (startTime, now) -> {
                // only allow one triggering of the listeners
                if (scheduled.compareAndSet(false, true)) {
                    return 0;
                } else {
                    return -1;
                }
            }));

            latch.await();

            // now check that every listener was invoked
            assertTrue(listeners.stream().map(Tuple::v2).allMatch(AtomicBoolean::get));
            if (numberOfFailingListeners > 0) {
                assertFailedListenerLogMessage(mockLogger, numberOfFailingListeners);
            }
            // Verify the debug logging:
            verifyDebugLogging(mockLogger);

            verifyNoMoreInteractions(mockLogger);
        } finally {
            engine.stop();
        }
    }

    public void testListenersThrowingExceptionsDoNotCauseNextScheduledTaskToBeSkipped() throws InterruptedException {
        final Logger mockLogger = mock(Logger.class);
        final SchedulerEngine engine = new SchedulerEngine(Settings.EMPTY, Clock.systemUTC(), mockLogger);
        try {
            final List<Tuple<SchedulerEngine.Listener, AtomicInteger>> listeners = new ArrayList<>();
            final int numberOfListeners = randomIntBetween(1, 32);
            final int numberOfSchedules = randomIntBetween(1, 32);
            final CountDownLatch listenersLatch = new CountDownLatch(numberOfSchedules * numberOfListeners);
            for (int i = 0; i < numberOfListeners; i++) {
                final AtomicInteger triggerCount = new AtomicInteger();
                final SchedulerEngine.Listener listener = event -> {
                    if (triggerCount.incrementAndGet() <= numberOfSchedules) {
                        listenersLatch.countDown();
                        throw new RuntimeException(getTestName());
                    } else {
                        fail("listener invoked more than [" + numberOfSchedules + "] times");
                    }
                };
                listeners.add(Tuple.tuple(listener, triggerCount));
                engine.register(listener);
            }

            // latch for each invocation of nextScheduledTimeAfter, once for each scheduled run, and then a final time when we disable
            final CountDownLatch latch = new CountDownLatch(1 + numberOfSchedules);
            engine.add(new SchedulerEngine.Job(getTestName(), (startTime, now) -> {
                if (latch.getCount() >= 2) {
                    latch.countDown();
                    return 0;
                } else if (latch.getCount() == 1) {
                    latch.countDown();
                    return -1;
                } else {
                    throw new AssertionError("nextScheduledTimeAfter invoked more than the expected number of times");
                }
            }));

            listenersLatch.await();
            assertTrue(listeners.stream().map(Tuple::v2).allMatch(count -> count.get() == numberOfSchedules));
            latch.await();
            assertFailedListenerLogMessage(mockLogger, numberOfSchedules * numberOfListeners);
            // Verify the debug logging:
            verifyDebugLogging(mockLogger);

            verifyNoMoreInteractions(mockLogger);
        } finally {
            engine.stop();
        }
    }

    public void testCancellingDuringRunPreventsRescheduling() throws Exception {
        final CountDownLatch jobRunningLatch = new CountDownLatch(1);
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        final AtomicInteger calledCount = new AtomicInteger(0);
        final SchedulerEngine engine = new SchedulerEngine(Settings.EMPTY, Clock.systemUTC());
        final String jobId = randomAlphaOfLength(4);
        try {
            engine.register(event -> {
                assertThat(event.jobName(), is(jobId));
                calledCount.incrementAndGet();
                jobRunningLatch.countDown();
                try {
                    listenerLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            engine.add(new Job(jobId, ((startTime, now) -> 0)));

            jobRunningLatch.await();
            final int called = calledCount.get();
            assertEquals(1, called);
            engine.remove(jobId);
            listenerLatch.countDown();

            assertBusy(() -> assertEquals(called, calledCount.get()), 5, TimeUnit.MILLISECONDS);
        } finally {
            engine.stop();
        }
    }

    public void testNextScheduledTimeAfterCurrentScheduledTime() throws Exception {
        final Clock clock = Clock.fixed(Clock.systemUTC().instant(), ZoneId.of("UTC"));
        final long oneHourMillis = TimeUnit.HOURS.toMillis(1L);
        final String jobId = randomAlphaOfLength(4);
        final SchedulerEngine engine = new SchedulerEngine(Settings.EMPTY, clock);
        try {
            engine.add(new Job(jobId, ((startTime, now) -> now + oneHourMillis)));

            ActiveSchedule activeSchedule = engine.getSchedule(jobId);
            assertNotNull(activeSchedule);
            assertEquals(clock.millis() + oneHourMillis, activeSchedule.getScheduledTime());

            assertEquals(
                clock.millis() + oneHourMillis + oneHourMillis,
                activeSchedule.computeNextScheduledTime(clock.millis() - randomIntBetween(1, 999))
            );
            assertEquals(
                clock.millis() + oneHourMillis + oneHourMillis,
                activeSchedule.computeNextScheduledTime(clock.millis() + TimeUnit.SECONDS.toMillis(10L))
            );
        } finally {
            engine.stop();
        }
    }

    private void assertFailedListenerLogMessage(Logger mockLogger, int times) {
        @SuppressWarnings("rawtypes")
        final ArgumentCaptor<Supplier> messageCaptor = ArgumentCaptor.forClass(Supplier.class);
        final ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockLogger, times(times)).warn(messageCaptor.capture(), throwableCaptor.capture());
        for (@SuppressWarnings("rawtypes")
        final Supplier supplier : messageCaptor.getAllValues()) {
            assertThat(supplier.get().toString(), equalTo("listener failed while handling triggered event [" + getTestName() + "]"));
        }
        for (final Throwable throwable : throwableCaptor.getAllValues()) {
            assertThat(throwable, instanceOf(RuntimeException.class));
            assertThat(throwable.getMessage(), equalTo(getTestName()));
        }
    }

    private static void verifyDebugLogging(Logger mockLogger) {
        verify(mockLogger, atLeastOnce()).debug(any(Supplier.class));
    }

}
