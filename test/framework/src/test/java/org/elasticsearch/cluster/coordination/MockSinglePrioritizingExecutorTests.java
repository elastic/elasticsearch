/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

public class MockSinglePrioritizingExecutorTests extends ESTestCase {

    public void testPrioritizedEsThreadPoolExecutor() {
        final DeterministicTaskQueue taskQueue = DeterministicTaskQueueTests.newTaskQueue();
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
