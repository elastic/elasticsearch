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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;

public class DeterministicTaskQueueTests extends ESTestCase {

    public void testRunNextTask() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        taskQueue.scheduleNow(() -> strings.add("foo"));
        taskQueue.scheduleNow(() -> strings.add("bar"));

        assertThat(strings, empty());

        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));

        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runNextTask();
        assertThat(strings, contains("foo", "bar"));

        assertFalse(taskQueue.hasRunnableTasks());
    }

    public void testRunRandomTask() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(4);

        taskQueue.scheduleNow(() -> strings.add("foo"));
        taskQueue.scheduleNow(() -> strings.add("bar"));
        taskQueue.scheduleNow(() -> strings.add("baz"));
        taskQueue.scheduleNow(() -> strings.add("quux"));

        assertThat(strings, empty());

        while (taskQueue.hasRunnableTasks()) {
            taskQueue.runRandomTask(random());
        }

        assertThat(strings, containsInAnyOrder("foo", "bar", "baz", "quux"));
    }

    public void testStartsAtTimeZero() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        assertThat(taskQueue.getCurrentTimeMillis(), is(0L));
    }

    public void testDoesNotDeferTasksForImmediateExecution() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(1);

        taskQueue.scheduleAt(0, () -> strings.add("foo"));
        assertTrue(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));

        assertFalse(taskQueue.hasRunnableTasks());
    }

    public void testDoesNotDeferTasksScheduledInThePast() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(1);

        taskQueue.scheduleAt(-1, () -> strings.add("foo"));
        assertTrue(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));

        assertFalse(taskQueue.hasRunnableTasks());
    }

    public void testDefersTasksWithPositiveDelays() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(1);

        taskQueue.scheduleAt(100, () -> strings.add("foo"));
        assertThat(taskQueue.getCurrentTimeMillis(), is(0L));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(100L));
        assertTrue(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));

        assertFalse(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
    }

    public void testKeepsFutureTasksDeferred() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        taskQueue.scheduleAt(100, () -> strings.add("foo"));
        taskQueue.scheduleAt(200, () -> strings.add("bar"));

        assertThat(taskQueue.getCurrentTimeMillis(), is(0L));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(100L));
        assertTrue(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(200L));
        assertTrue(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        assertThat(strings, contains("foo", "bar"));
    }

    public void testExecutesTasksInTimeOrder() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(3);

        taskQueue.scheduleAt(100, () -> strings.add("foo"));
        taskQueue.scheduleAt(200, () -> strings.add("bar"));

        assertThat(taskQueue.getCurrentTimeMillis(), is(0L));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(100L));
        assertTrue(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.scheduleAt(150, () -> strings.add("baz"));

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(150L));
        assertTrue(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        taskQueue.advanceTime();
        taskQueue.runNextTask();
        assertThat(strings, contains("foo", "baz", "bar"));
        assertThat(taskQueue.getCurrentTimeMillis(), is(200L));
        assertFalse(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
    }

    public void testExecutorServiceEnqueuesTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        final ExecutorService executorService = taskQueue.getExecutorService();
        assertFalse(taskQueue.hasRunnableTasks());
        executorService.execute(() -> strings.add("foo"));
        assertTrue(taskQueue.hasRunnableTasks());
        executorService.execute(() -> strings.add("bar"));

        assertThat(strings, empty());

        while (taskQueue.hasRunnableTasks()) {
            taskQueue.runRandomTask(random());
        }

        assertThat(strings, containsInAnyOrder("foo", "bar"));
    }

    public void testThreadPoolEnqueuesTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        final ThreadPool threadPool = taskQueue.getThreadPool();
        assertFalse(taskQueue.hasRunnableTasks());
        threadPool.generic().execute(() -> strings.add("foo"));
        assertTrue(taskQueue.hasRunnableTasks());
        threadPool.executor("anything").execute(() -> strings.add("bar"));

        assertThat(strings, empty());

        while (taskQueue.hasRunnableTasks()) {
            taskQueue.runRandomTask(random());
        }

        assertThat(strings, containsInAnyOrder("foo", "bar"));
    }

    public void testFutureExecutorSchedulesTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(5);

        final FutureExecutor futureExecutor = taskQueue.getFutureExecutor();
        futureExecutor.schedule(TimeValue.timeValueMillis(100), () -> strings.add("deferred"));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        futureExecutor.schedule(TimeValue.ZERO, () -> strings.add("runnable"));
        assertTrue(taskQueue.hasRunnableTasks());

        futureExecutor.schedule(TimeValue.MINUS_ONE, () -> strings.add("also runnable"));

        runAllTasks(taskQueue);

        assertThat(taskQueue.getCurrentTimeMillis(), is(100L));
        assertThat(strings, contains("runnable", "also runnable", "deferred"));

        futureExecutor.schedule(TimeValue.timeValueMillis(100), () -> strings.add("further deferred"));
        futureExecutor.schedule(TimeValue.timeValueMillis(50), () -> strings.add("not quite so deferred"));

        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        runAllTasks(taskQueue);
        assertThat(taskQueue.getCurrentTimeMillis(), is(200L));
        assertThat(strings, contains("runnable", "also runnable", "deferred", "not quite so deferred", "further deferred"));
    }

    private static void runAllTasks(DeterministicTaskQueue taskQueue) {
        while (true) {
            while (taskQueue.hasRunnableTasks()) {
                taskQueue.runNextTask();
            }
            if (taskQueue.hasDeferredTasks()) {
                taskQueue.advanceTime();
            } else {
                break;
            }
        }
    }

    private static DeterministicTaskQueue newTaskQueue() {
        return new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build());
    }
}
