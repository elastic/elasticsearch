/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.OngoingStubbing;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdjustableCapacityBlockingQueueTests extends ESTestCase {
    private static final Function<Integer, BlockingQueue<Integer>> QUEUE_CREATOR = LinkedBlockingQueue::new;

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testSetCapacity_ChangesTheQueueCapacityToTwo() {
        var queue = new AdjustableCapacityBlockingQueue<>(1, QUEUE_CREATOR);
        assertThat(queue.remainingCapacity(), is(1));

        queue.setCapacity(2);
        assertThat(queue.remainingCapacity(), is(2));
    }

    public void testSetCapacity_ReturnsOverflowEntries_WhenReducingTheQueueCapacity() throws InterruptedException {
        var queue = new AdjustableCapacityBlockingQueue<>(2, QUEUE_CREATOR);
        assertThat(queue.remainingCapacity(), is(2));

        queue.offer(0);
        queue.offer(1);

        var remainingEntries = queue.setCapacity(1);
        assertThat(remainingEntries, is(List.of(1)));
        assertThat(queue.remainingCapacity(), is(0));
        assertThat(queue.size(), is(1));
        assertThat(queue.take(), is(0));
    }

    public void testOffer_CompetingOffersSucceedCorrectly_WithoutDeadlock2() throws InterruptedException, ExecutionException,
        TimeoutException {
        var waitForSecondOfferLatch = new CountDownLatch(1);
        var calledOfferWith0Latch = new CountDownLatch(1);

        @SuppressWarnings("unchecked")
        BlockingQueue<Integer> queue = mock(LinkedBlockingQueue.class);

        when(queue.offer(eq(0))).thenAnswer(invocation -> {
            calledOfferWith0Latch.countDown();
            waitForSecondOfferLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            return true;
        });

        var adjustableQueue = new AdjustableCapacityBlockingQueue<>(1, (capacity) -> queue);

        Future<?> offerZeroFuture = threadPool.generic().submit(() -> {
            try {
                adjustableQueue.offer(0);
            } catch (Exception e) {
                fail(Strings.format("Failed to offer item to queue: %s", e));
            }
        });

        Future<?> offerOneFuture = threadPool.generic().submit(() -> {
            try {
                calledOfferWith0Latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                // this should not block
                adjustableQueue.offer(1);
                waitForSecondOfferLatch.countDown();
            } catch (Exception e) {
                fail(Strings.format("Failed to offer item to queue: %s", e));
            }
        });

        offerZeroFuture.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        offerOneFuture.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }

    public void testOffer_CompetingOffersSucceedCorrectly_WithoutDeadlock() throws InterruptedException, ExecutionException,
        TimeoutException {
        StubbingFunction<Boolean> createStub = (capacity, queue) -> when(queue.offer(eq(capacity)));
        Supplier<Boolean> getResultForAnswer = () -> false;

        var whenStub = new WhenStub<>(createStub, getResultForAnswer);
        var methodTest = new Method<>(whenStub, (value, queue) -> queue.offer(value), threadPool);

        methodTest.executeTestDoesNotDeadlock();
    }

    @FunctionalInterface
    private interface StubbingFunction<T> {
        OngoingStubbing<T> createStub(Integer capacity, BlockingQueue<Integer> queue);
    }

    private record WhenStub<T>(StubbingFunction<T> stubbingFunction, Supplier<T> getResultForAnswer) {}

    @FunctionalInterface
    private interface MethodCall<R> {
        R doOperation(Integer value, AdjustableCapacityBlockingQueue<Integer> queue);
    }

    private record Method<T>(WhenStub<T> whenStub, MethodCall<T> methodCall, ThreadPool threadPool) {
        public void executeTestDoesNotDeadlock() throws ExecutionException, InterruptedException, TimeoutException {
            var waitForSecondOfferLatch = new CountDownLatch(1);
            var calledOfferWith0Latch = new CountDownLatch(1);

            @SuppressWarnings("unchecked")
            BlockingQueue<Integer> queue = mock(LinkedBlockingQueue.class);

            whenStub.stubbingFunction.createStub(0, queue).thenAnswer(invocation -> {
                calledOfferWith0Latch.countDown();
                waitForSecondOfferLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                return whenStub.getResultForAnswer.get();
            });

            var adjustableQueue = new AdjustableCapacityBlockingQueue<>(1, (capacity) -> queue);

            Future<?> offerZeroFuture = threadPool.generic().submit(() -> {
                try {
                    methodCall.doOperation(0, adjustableQueue);
                } catch (Exception e) {
                    fail(Strings.format("Failed to offer item to queue: %s", e));
                }
            });

            Future<?> offerOneFuture = threadPool.generic().submit(() -> {
                try {
                    calledOfferWith0Latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                    // this should not block
                    methodCall.doOperation(1, adjustableQueue);
                    waitForSecondOfferLatch.countDown();
                } catch (Exception e) {
                    fail(Strings.format("Failed to offer item to queue: %s", e));
                }
            });

            offerZeroFuture.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            offerOneFuture.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        }
    }
}
