/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class PendingListenersQueueTests extends ESTestCase {

    public void testShouldExecuteOnlyCompleted() throws InterruptedException {
        var threadPool = new TestThreadPool(getTestName());
        var queue = new PendingListenersQueue(threadPool);
        var executed = new CountDownLatch(2);

        queue.add(1, ActionListener.running(executed::countDown));
        queue.add(2, ActionListener.running(executed::countDown));
        queue.add(3, ActionListener.running(() -> fail("Should not complete in test")));
        queue.complete(2);

        try {
            assertThat(executed.await(1, TimeUnit.SECONDS), equalTo(true));
        } finally {
            terminate(threadPool);
        }
    }

    public void testShouldAdvanceOnly() throws InterruptedException {
        var threadPool = new TestThreadPool(getTestName());
        var queue = new PendingListenersQueue(threadPool);
        var executed = new CountDownLatch(2);

        queue.add(1, ActionListener.running(executed::countDown));
        queue.add(2, ActionListener.running(executed::countDown));
        queue.add(3, ActionListener.running(() -> fail("Should not complete in test")));
        queue.complete(2);
        queue.complete(1);

        try {
            assertThat(executed.await(1, TimeUnit.SECONDS), equalTo(true));
            assertThat(queue.getCompletedIndex(), equalTo(2L));
        } finally {
            terminate(threadPool);
        }
    }

    public void testShouldExecuteAllAsNonMaster() throws InterruptedException {
        var threadPool = new TestThreadPool(getTestName());
        var queue = new PendingListenersQueue(threadPool);
        var executed = new CountDownLatch(2);

        queue.add(1, ActionListener.wrap(ignored -> fail("Should not complete in test"), exception -> executed.countDown()));
        queue.add(2, ActionListener.wrap(ignored -> fail("Should not complete in test"), exception -> executed.countDown()));
        queue.completeAllAsNotMaster();

        try {
            assertThat(executed.await(1, TimeUnit.SECONDS), equalTo(true));
        } finally {
            terminate(threadPool);
        }
    }
}
