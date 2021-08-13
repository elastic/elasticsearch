/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

public class MockSinglePrioritizingExecutorTests extends ESTestCase {

    public void testPrioritizedEsThreadPoolExecutor() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

        final PrioritizedEsThreadPoolExecutor executor = new MockSinglePrioritizingExecutor("test", taskQueue, taskQueue.getThreadPool());
        final AtomicBoolean called1 = new AtomicBoolean();
        final AtomicBoolean called2 = new AtomicBoolean();
        executor.execute(new PrioritizedRunnable(Priority.NORMAL) {
            @Override
            public void run() {
                assertTrue(called1.compareAndSet(false, true)); // check that this is only called once
            }

        });
        executor.execute(new PrioritizedRunnable(Priority.HIGH) {
            @Override
            public void run() {
                assertTrue(called2.compareAndSet(false, true)); // check that this is only called once
            }
        });
        assertFalse(called1.get());
        assertFalse(called2.get());
        taskQueue.runRandomTask();
        assertFalse(called1.get());
        assertTrue(called2.get());
        taskQueue.runRandomTask();
        assertTrue(called1.get());
        assertTrue(called2.get());
        taskQueue.runRandomTask();
        assertFalse(taskQueue.hasRunnableTasks());
    }
}
