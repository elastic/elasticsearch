/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class StepListenerTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testSimpleSteps() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Consumer<Exception> onFailure = e -> {
            latch.countDown();
            fail("test a happy path");
        };

        StepListener<String> step1 = new StepListener<>(); // [a]sync provide a string
        executeAction(() -> step1.onResponse("hello"));
        StepListener<Integer> step2 = new StepListener<>(); // [a]sync calculate the length of the string
        step1.whenComplete(str -> executeAction(() -> step2.onResponse(str.length())), onFailure);
        step2.whenComplete(length -> executeAction(latch::countDown), onFailure);
        latch.await();
        assertThat(step1.result(), equalTo("hello"));
        assertThat(step2.result(), equalTo(5));
    }

    public void testAbortOnFailure() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        int failedStep = randomBoolean() ? 1 : 2;
        AtomicInteger failureNotified = new AtomicInteger();
        Consumer<Exception> onFailure = e -> {
            failureNotified.getAndIncrement();
            latch.countDown();
            assertThat(e.getMessage(), equalTo("failed at step " + failedStep));
        };

        StepListener<String> step1 = new StepListener<>(); // [a]sync provide a string
        if (failedStep == 1) {
            executeAction(() -> step1.onFailure(new RuntimeException("failed at step 1")));
        } else {
            executeAction(() -> step1.onResponse("hello"));
        }

        StepListener<Integer> step2 = new StepListener<>(); // [a]sync calculate the length of the string
        step1.whenComplete(str -> {
            if (failedStep == 2) {
                executeAction(() -> step2.onFailure(new RuntimeException("failed at step 2")));
            } else {
                executeAction(() -> step2.onResponse(str.length()));
            }
        }, onFailure);

        step2.whenComplete(length -> latch.countDown(), onFailure);
        latch.await();
        assertThat(failureNotified.get(), equalTo(1));

        if (failedStep == 1) {
            assertThat(expectThrows(RuntimeException.class, step1::result).getMessage(), equalTo("failed at step 1"));
            assertFalse(step2.isDone());
        } else {
            assertThat(step1.result(), equalTo("hello"));
            assertThat(expectThrows(RuntimeException.class, step2::result).getMessage(), equalTo("failed at step 2"));
        }
    }

    private void executeAction(Runnable runnable) {
        if (randomBoolean()) {
            threadPool.generic().execute(runnable);
        } else {
            runnable.run();
        }
    }

    /**
     * This test checks that we no longer unwrap exceptions when using StepListener.
     */
    public void testNoUnwrap() {
        StepListener<String> step = new StepListener<>();
        step.onFailure(new RemoteTransportException("test", new RuntimeException("expected")));
        AtomicReference<RuntimeException> exception = new AtomicReference<>();
        step.whenComplete(null, e -> { exception.set((RuntimeException) e); });

        assertEquals(RemoteTransportException.class, exception.get().getClass());
        RuntimeException e = expectThrows(RuntimeException.class, () -> step.result());
        assertEquals(RemoteTransportException.class, e.getClass());
    }

    public void testAddedListenersReleasedOnCompletion() {
        final StepListener<Void> step = new StepListener<>();
        final ReachabilityChecker reachabilityChecker = new ReachabilityChecker();

        for (int i = between(1, 3); i > 0; i--) {
            step.addListener(reachabilityChecker.register(ActionListener.running(() -> {})));
        }
        reachabilityChecker.checkReachable();
        if (randomBoolean()) {
            step.onResponse(null);
        } else {
            step.onFailure(new ElasticsearchException("simulated"));
        }
        reachabilityChecker.ensureUnreachable();

        step.addListener(reachabilityChecker.register(ActionListener.running(() -> {})));
        reachabilityChecker.ensureUnreachable();
    }
}
