/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class AllocationActionMultiListenerTests extends ESTestCase {

    public void testShouldDelegateWhenBothComplete() {
        var listener = new AllocationActionMultiListener<Integer>(createEmptyThreadContext());

        var l1 = new AtomicInteger();
        var l2 = new AtomicInteger();
        listener.delay(ActionTestUtils.assertNoFailureListener(l1::set)).onResponse(1);
        listener.delay(ActionTestUtils.assertNoFailureListener(l2::set)).onResponse(2);
        if (randomBoolean()) {
            listener.reroute().onResponse(null);
        } else {
            listener.noRerouteNeeded();
        }

        assertThat(l1.get(), equalTo(1));
        assertThat(l2.get(), equalTo(2));
    }

    public void testShouldNotDelegateWhenOnlyOneComplete() {
        var listener = new AllocationActionMultiListener<AcknowledgedResponse>(createEmptyThreadContext());

        var completed = new AtomicBoolean(false);
        var delegate = listener.delay(ActionTestUtils.assertNoFailureListener(ignore -> completed.set(true)));

        switch (randomInt(2)) {
            case 0 -> delegate.onResponse(AcknowledgedResponse.TRUE);
            case 1 -> listener.reroute().onResponse(null);
            case 2 -> listener.noRerouteNeeded();
        }

        assertThat(completed.get(), equalTo(false));
    }

    public void testShouldDelegateFailureImmediately() {
        var listener = new AllocationActionMultiListener<AcknowledgedResponse>(createEmptyThreadContext());

        var completed = new AtomicBoolean(false);
        listener.delay(
            ActionListener.wrap(ignore -> { throw new AssertionError("Should not complete in test"); }, exception -> completed.set(true))
        ).onFailure(new RuntimeException());

        assertThat(completed.get(), equalTo(true));
    }

    public void testConcurrency() throws InterruptedException {

        var listener = new AllocationActionMultiListener<AcknowledgedResponse>(createEmptyThreadContext());

        var count = randomIntBetween(1, 100);
        var completed = new CountDownLatch(count);

        var start = new CountDownLatch(3);
        var threadPool = new TestThreadPool(getTestName());

        threadPool.executor(ThreadPool.Names.CLUSTER_COORDINATION).submit(() -> {
            start.countDown();
            awaitQuietly(start);
            for (int i = 0; i < count; i++) {
                listener.delay(ActionTestUtils.assertNoFailureListener(ignore -> completed.countDown()))
                    .onResponse(AcknowledgedResponse.TRUE);
            }
        });

        threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> {
            start.countDown();
            awaitQuietly(start);
            if (randomBoolean()) {
                listener.reroute().onResponse(null);
            } else {
                listener.noRerouteNeeded();
            }
        });
        start.countDown();

        assertTrue("Expected to call all delayed listeners within timeout", completed.await(10, TimeUnit.SECONDS));
        terminate(threadPool);
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            assertTrue("Latch did not complete within timeout", latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for test to start", e);
        }
    }

    public void testShouldExecuteWithCorrectContext() {

        final var requestHeaderName = "header";
        final var responseHeaderName = "responseHeader";

        final var expectedRequestHeader = randomAlphaOfLength(10);
        final var expectedResponseHeader = randomAlphaOfLength(10);

        var context = new ThreadContext(Settings.EMPTY);
        var listener = new AllocationActionMultiListener<>(context);

        context.putHeader(requestHeaderName, expectedRequestHeader);
        context.addResponseHeader(responseHeaderName, expectedResponseHeader);

        var isComplete = new AtomicBoolean();
        try (var refs = new RefCountingRunnable(() -> assertTrue(isComplete.compareAndSet(false, true)))) {

            List<Runnable> actions = new ArrayList<>();

            for (int i = between(0, 5); i > 0; i--) {
                var expectedVal = new Object();
                var delayedListener = listener.delay(
                    ActionListener.releaseAfter(ActionListener.running(Assert::fail).delegateFailure((l, val) -> {
                        assertSame(expectedVal, val);
                        assertEquals(expectedRequestHeader, context.getHeader(requestHeaderName));
                        assertEquals(List.of(expectedResponseHeader), context.getResponseHeaders().get(responseHeaderName));
                        context.addResponseHeader(responseHeaderName, randomAlphaOfLength(10));
                    }), refs.acquire())
                );
                actions.add(() -> delayedListener.onResponse(expectedVal));
            }

            final var additionalResponseHeader = randomAlphaOfLength(10);
            context.addResponseHeader(responseHeaderName, additionalResponseHeader);

            actions.add(() -> listener.reroute().onResponse(null));

            for (var action : shuffledList(actions)) {
                try (var ignored = context.stashContext()) {
                    final var localRequestHeader = randomAlphaOfLength(10);
                    final var localResponseHeader = randomAlphaOfLength(10);
                    context.putHeader(requestHeaderName, localRequestHeader);
                    context.addResponseHeader(responseHeaderName, localResponseHeader);
                    action.run();
                    assertEquals(localRequestHeader, context.getHeader(requestHeaderName));
                    assertEquals(List.of(localResponseHeader), context.getResponseHeaders().get(responseHeaderName));
                }
            }

            assertEquals(
                Set.of(expectedResponseHeader, additionalResponseHeader),
                Set.copyOf(context.getResponseHeaders().get(responseHeaderName))
            );
        }

        assertTrue(isComplete.get());
    }

    private static ThreadContext createEmptyThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }
}
