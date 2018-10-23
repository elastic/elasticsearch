/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.scheduler;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.argThat;
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
                    }).when(mockLogger).warn(argThat(any(ParameterizedMessage.class)), argThat(any(RuntimeException.class)));
                }
                listeners.add(Tuple.tuple(listener, trigger));
            }

            // randomize the order and register the listeners
            Collections.shuffle(listeners, random());
            listeners.stream().map(Tuple::v1).forEach(engine::register);

            final AtomicBoolean scheduled = new AtomicBoolean();
            engine.add(new SchedulerEngine.Job(
                    getTestName(),
                    (startTime, now) -> {
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
            engine.add(new SchedulerEngine.Job(
                    getTestName(),
                    (startTime, now) -> {
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
            verifyNoMoreInteractions(mockLogger);
        } finally {
            engine.stop();
        }
    }

    private void assertFailedListenerLogMessage(Logger mockLogger, int times) {
        final ArgumentCaptor<ParameterizedMessage> messageCaptor = ArgumentCaptor.forClass(ParameterizedMessage.class);
        final ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockLogger, times(times)).warn(messageCaptor.capture(), throwableCaptor.capture());
        for (final ParameterizedMessage message : messageCaptor.getAllValues()) {
            assertThat(message.getFormat(), equalTo("listener failed while handling triggered event [{}]"));
            assertThat(message.getParameters(), arrayWithSize(1));
            assertThat(message.getParameters()[0], equalTo(getTestName()));
        }
        for (final Throwable throwable : throwableCaptor.getAllValues()) {
            assertThat(throwable, instanceOf(RuntimeException.class));
            assertThat(throwable.getMessage(), equalTo(getTestName()));
        }
    }

}
