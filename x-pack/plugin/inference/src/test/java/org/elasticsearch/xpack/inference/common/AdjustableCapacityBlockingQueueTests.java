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

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.is;

public class AdjustableCapacityBlockingQueueTests extends ESTestCase {
    private static final AdjustableCapacityBlockingQueue.QueueCreator<Integer> QUEUE_CREATOR =
        new AdjustableCapacityBlockingQueue.QueueCreator<>() {
            @Override
            public BlockingQueue<Integer> create(int capacity) {
                return new LinkedBlockingQueue<>(capacity);
            }

            @Override
            public BlockingQueue<Integer> create() {
                return new LinkedBlockingQueue<>();
            }
        };

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
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        assertThat(queue.remainingCapacity(), is(1));

        queue.setCapacity(2);
        assertThat(queue.remainingCapacity(), is(2));
    }

    public void testInitiallySetsCapacityToUnbounded_WhenCapacityIsNull() {
        assertThat(new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, null).remainingCapacity(), is(Integer.MAX_VALUE));
    }

    public void testSetCapacity_RemainingCapacityIsZero_WhenReducingTheQueueCapacityToOne_WhenItemsExistInTheQueue()
        throws InterruptedException {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 2);
        assertThat(queue.remainingCapacity(), is(2));
        assertThat(queue.size(), is(0));

        queue.offer(0);
        queue.offer(1);
        assertThat(queue.remainingCapacity(), is(0));

        queue.setCapacity(1);
        assertThat(queue.remainingCapacity(), is(0));
        assertThat(queue.size(), is(2));
        assertThat(queue.take(), is(0));
        assertThat(queue.take(), is(1));
    }

    public void testSetCapacity_RetainsOrdering_WhenReturningItems_AfterDecreasingCapacity() {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 3);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        queue.offer(1);
        queue.offer(2);
        assertThat(queue.size(), is(3));

        queue.setCapacity(2);

        var entriesList = new ArrayList<Integer>();
        assertThat(queue.drainTo(entriesList), is(3));

        assertThat(queue.size(), is(0));
        assertThat(entriesList, is(List.of(0, 1, 2)));
    }

    public void testSetCapacity_RetainsOrdering_WhenReturningItems_AfterIncreasingCapacity() {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 2);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        queue.offer(1);
        assertThat(queue.size(), is(2));

        queue.setCapacity(3);

        queue.offer(2);

        var entriesList = new ArrayList<Integer>();
        assertThat(queue.drainTo(entriesList), is(3));

        assertThat(queue.size(), is(0));
        assertThat(entriesList, is(List.of(0, 1, 2)));
    }

    public void testSetCapacity_RetainsOrdering_WhenReturningItems_AfterDecreasingCapacity_UsingTake() throws InterruptedException {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 3);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        queue.offer(1);
        queue.offer(2);
        assertThat(queue.size(), is(3));

        queue.setCapacity(2);

        assertThat(queue.take(), is(0));
        assertThat(queue.take(), is(1));
        assertThat(queue.take(), is(2));

        assertThat(queue.size(), is(0));
    }

    public void testSetCapacity_RetainsOrdering_WhenReturningItems_AfterIncreasingCapacity_UsingTake() throws InterruptedException {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 2);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        queue.offer(1);
        assertThat(queue.size(), is(2));

        queue.setCapacity(3);

        queue.offer(2);

        assertThat(queue.take(), is(0));
        assertThat(queue.take(), is(1));
        assertThat(queue.take(), is(2));

        assertThat(queue.size(), is(0));
    }

    public void testOffer_AddsItemToTheQueue() {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        assertThat(queue.size(), is(1));
    }

    public void testDrainTo_MovesAllItemsFromQueueToList() {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 2);
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
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 2);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        queue.offer(1);
        assertThat(queue.size(), is(2));

        var entriesList = new ArrayList<Integer>();
        assertThat(queue.drainTo(entriesList, 1), is(1));

        assertThat(queue.size(), is(1));
        assertThat(entriesList, is(List.of(0)));
    }

    public void testPoll_RemovesAnItemFromTheQueue_AfterItBecomesAvailable() throws ExecutionException, InterruptedException,
        TimeoutException {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        assertThat(queue.size(), is(0));

        var waitForOfferCallLatch = new CountDownLatch(1);

        Future<Integer> pollFuture = threadPool.generic().submit(() -> {
            try {
                waitForOfferCallLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                return queue.poll(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            } catch (Exception e) {
                fail(Strings.format("Failed to polling queue: %s", e));
            }

            return null;
        });

        queue.offer(0);
        assertThat(queue.size(), is(1));
        waitForOfferCallLatch.countDown();

        assertThat(pollFuture.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS), is(0));

        assertThat(queue.size(), is(0));
    }

    public void testTake_RemovesItemFromQueue() throws InterruptedException {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        assertThat(queue.size(), is(1));

        assertThat(queue.take(), is(0));
        assertThat(queue.size(), is(0));
    }

    public void testPeek_ReturnsItemWithoutRemoving() {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        assertThat(queue.size(), is(0));

        queue.offer(0);
        assertThat(queue.size(), is(1));
        assertThat(queue.peek(), is(0));
        assertThat(queue.size(), is(1));
        assertThat(queue.peek(), is(0));
    }

    public void testPeek_ExistingItem_RemainsAtFront_AfterCapacityChange() throws InterruptedException {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        queue.offer(0);
        assertThat(queue.size(), is(1));
        assertThat(queue.remainingCapacity(), is(0));
        assertThat(queue.peek(), is(0));

        queue.setCapacity(2);
        assertThat(queue.remainingCapacity(), is(1));
        assertThat(queue.peek(), is(0));

        queue.offer(1);
        assertThat(queue.peek(), is(0));
        assertThat(queue.take(), is(0));
        assertThat(queue.peek(), is(1));
    }

    public void testPoll_ReturnsNull_WhenNoItemsAreAvailable() {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        assertNull(queue.poll());
    }

    public void testPoll_ReturnsFirstElement() {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        queue.offer(0);
        assertThat(queue.poll(), is(0));
        assertThat(queue.size(), is(0));
        assertThat(queue.remainingCapacity(), is(1));
    }

    public void testPoll_ReturnsFirstElement_AfterCapacityIncrease() {
        var queue = new AdjustableCapacityBlockingQueue<>(QUEUE_CREATOR, 1);
        queue.offer(0);
        queue.setCapacity(2);
        queue.offer(1);

        assertThat(queue.remainingCapacity(), is(0));
        assertThat(queue.size(), is(2));

        assertThat(queue.poll(), is(0));
        assertThat(queue.size(), is(1));
        assertThat(queue.remainingCapacity(), is(1));

        assertThat(queue.poll(), is(1));
        assertThat(queue.size(), is(0));
        assertThat(queue.remainingCapacity(), is(2));
    }

    public static <E> AdjustableCapacityBlockingQueue.QueueCreator<E> mockQueueCreator(BlockingQueue<E> backingQueue) {
        return new AdjustableCapacityBlockingQueue.QueueCreator<>() {
            @Override
            public BlockingQueue<E> create(int capacity) {
                return backingQueue;
            }

            @Override
            public BlockingQueue<E> create() {
                return backingQueue;
            }
        };
    }
}
