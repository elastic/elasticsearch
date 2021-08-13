/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.scheduler;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class EvilSchedulerEngineTests extends ESTestCase {

    public void testOutOfMemoryErrorWhileTriggeredIsRethrownAndIsUncaught() throws InterruptedException {
        final AtomicReference<Throwable> maybeFatal = new AtomicReference<>();
        final CountDownLatch uncaughtLatuch = new CountDownLatch(1);
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        try {
            /*
             * We want to test that the out of memory error thrown from the scheduler engine goes uncaught on another thread; this gives us
             * confidence that an error thrown during a triggered event will lead to the node being torn down.
             */
            final AtomicReference<Thread> maybeThread = new AtomicReference<>();
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                maybeFatal.set(e);
                maybeThread.set(Thread.currentThread());
                uncaughtLatuch.countDown();
            });
            final Logger mockLogger = mock(Logger.class);
            final SchedulerEngine engine = new SchedulerEngine(Settings.EMPTY, Clock.systemUTC(), mockLogger);
            try {
                final AtomicBoolean trigger = new AtomicBoolean();
                engine.register(event -> {
                    if (trigger.compareAndSet(false, true)) {
                        throw new OutOfMemoryError("640K ought to be enough for anybody");
                    } else {
                        fail("listener invoked twice");
                    }
                });
                final CountDownLatch schedulerLatch = new CountDownLatch(1);
                engine.add(new SchedulerEngine.Job(
                        getTestName(),
                        (startTime, now) -> {
                            if (schedulerLatch.getCount() == 1) {
                                schedulerLatch.countDown();
                                return 0;
                            } else {
                                throw new AssertionError("nextScheduledTimeAfter invoked more than the expected number of times");
                            }
                        }));

                uncaughtLatuch.await();
                assertTrue(trigger.get());
                assertNotNull(maybeFatal.get());
                assertThat(maybeFatal.get(), instanceOf(OutOfMemoryError.class));
                assertThat(maybeFatal.get(), hasToString(containsString("640K ought to be enough for anybody")));
                assertNotNull(maybeThread.get());
                assertThat(maybeThread.get(), not(equalTo(Thread.currentThread()))); // the error should be rethrown on another thread
                schedulerLatch.await();
                verifyNoMoreInteractions(mockLogger); // we never logged anything
            } finally {
                engine.stop();
            }
        } finally {
            // restore the uncaught exception handler
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

}
