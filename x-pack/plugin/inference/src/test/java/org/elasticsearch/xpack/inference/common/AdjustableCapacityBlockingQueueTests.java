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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.is;

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

    public void testOffer_AddsItemToTheQueue() {
        var queue = new AdjustableCapacityBlockingQueue<>(1, QUEUE_CREATOR);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        assertThat(queue.size(), is(1));
    }

    public void testDrainTo_MovesAllItemsFromQueueToList() {
        var queue = new AdjustableCapacityBlockingQueue<>(2, QUEUE_CREATOR);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        queue.offer(1);
        assertThat(queue.size(), is(2));

        var entriesList = new ArrayList<Integer>();
        queue.drainTo(entriesList);

        assertThat(queue.size(), is(0));
        assertThat(entriesList, is(List.of(0, 1)));
    }

    public void testDrainTo_MovesOnlyOneItemFromQueueToList() {
        var queue = new AdjustableCapacityBlockingQueue<>(2, QUEUE_CREATOR);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        queue.offer(1);
        assertThat(queue.size(), is(2));

        var entriesList = new ArrayList<Integer>();
        queue.drainTo(entriesList, 1);

        assertThat(queue.size(), is(1));
        assertThat(entriesList, is(List.of(0)));
    }

    public void testPoll_RemovesAnItemFromTheQueue_AfterItBecomesAvailable() throws ExecutionException, InterruptedException,
        TimeoutException {
        var queue = new AdjustableCapacityBlockingQueue<>(1, QUEUE_CREATOR);
        assertThat(queue.size(), is(0));

        var waitForOfferCallLatch = new CountDownLatch(1);

        Future<?> pollFuture = threadPool.generic().submit(() -> {
            try {
                waitForOfferCallLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                queue.poll(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            } catch (Exception e) {
                fail(Strings.format("Failed to polling queue: %s", e));
            }
        });

        queue.offer(0);
        assertThat(queue.size(), is(1));
        waitForOfferCallLatch.countDown();

        pollFuture.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        assertThat(queue.size(), is(0));
    }

    public void testTake_RemovesItemFromQueue() throws InterruptedException {
        var queue = new AdjustableCapacityBlockingQueue<>(1, QUEUE_CREATOR);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        assertThat(queue.size(), is(1));

        assertThat(queue.take(), is(0));
        assertThat(queue.size(), is(0));
    }
}
