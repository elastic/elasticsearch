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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
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
        final List<String> strings1 = getResultsOfRunningRandomly(new Random(4520795446362137264L));
        final List<String> strings2 = getResultsOfRunningRandomly(new Random(266504691902226821L));
        assertThat(strings1, not(equalTo(strings2)));
    }

    private List<String> getResultsOfRunningRandomly(Random random) {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(4);

        taskQueue.scheduleNow(() -> strings.add("foo"));
        taskQueue.scheduleNow(() -> strings.add("bar"));
        taskQueue.scheduleNow(() -> strings.add("baz"));
        taskQueue.scheduleNow(() -> strings.add("quux"));

        assertThat(strings, empty());

        while (taskQueue.hasRunnableTasks()) {
            taskQueue.runRandomTask(random);
        }

        assertThat(strings, containsInAnyOrder("foo", "bar", "baz", "quux"));
        return strings;
    }

    public void testStartsAtTimeZero() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        assertThat(taskQueue.getCurrentTimeMillis(), is(0L));
    }

    private void advanceToRandomTime(DeterministicTaskQueue taskQueue) {
        taskQueue.scheduleAt(randomLongBetween(1, 100), () -> {
        });
        taskQueue.advanceTime();
        taskQueue.runNextTask();
        assertFalse(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
    }

    public void testDoesNotDeferTasksForImmediateExecution() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);

        final List<String> strings = new ArrayList<>(1);

        taskQueue.scheduleAt(taskQueue.getCurrentTimeMillis(), () -> strings.add("foo"));
        assertTrue(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));

        assertFalse(taskQueue.hasRunnableTasks());
    }

    public void testDoesNotDeferTasksScheduledInThePast() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);

        final List<String> strings = new ArrayList<>(1);

        taskQueue.scheduleAt(taskQueue.getCurrentTimeMillis() - randomInt(200), () -> strings.add("foo"));
        assertTrue(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));

        assertFalse(taskQueue.hasRunnableTasks());
    }

    public void testDefersTasksWithPositiveDelays() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(1);

        final long executionTimeMillis = randomLongBetween(1, 100);
        taskQueue.scheduleAt(executionTimeMillis, () -> strings.add("foo"));
        assertThat(taskQueue.getCurrentTimeMillis(), is(0L));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis));
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

        final long executionTimeMillis1 = randomLongBetween(1, 100);
        final long executionTimeMillis2 = randomLongBetween(executionTimeMillis1 + 1, 200);

        taskQueue.scheduleAt(executionTimeMillis1, () -> strings.add("foo"));
        taskQueue.scheduleAt(executionTimeMillis2, () -> strings.add("bar"));

        assertThat(taskQueue.getCurrentTimeMillis(), is(0L));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis1));
        assertTrue(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis2));
        assertTrue(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        assertThat(strings, contains("foo", "bar"));
    }

    public void testExecutesTasksInTimeOrder() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(3);

        final long executionTimeMillis1 = randomLongBetween(1, 100);
        final long executionTimeMillis2 = randomLongBetween(executionTimeMillis1 + 100, 300);

        taskQueue.scheduleAt(executionTimeMillis1, () -> strings.add("foo"));
        taskQueue.scheduleAt(executionTimeMillis2, () -> strings.add("bar"));

        assertThat(taskQueue.getCurrentTimeMillis(), is(0L));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis1));
        assertTrue(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        assertThat(strings, contains("foo"));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        final long executionTimeMillis3 = randomLongBetween(executionTimeMillis1 + 1, executionTimeMillis2 - 1);
        taskQueue.scheduleAt(executionTimeMillis3, () -> strings.add("baz"));

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis3));
        assertTrue(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runNextTask();
        taskQueue.advanceTime();
        taskQueue.runNextTask();
        assertThat(strings, contains("foo", "baz", "bar"));
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis2));
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

    public void testThreadPoolWrapsRunnable() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final AtomicBoolean called = new AtomicBoolean();
        final ThreadPool threadPool = taskQueue.getThreadPool(runnable -> () -> {
            assertFalse(called.get());
            called.set(true);
            runnable.run();
        });
        threadPool.generic().execute(() -> logger.info("runnable executed"));
        assertFalse(called.get());
        taskQueue.runAllRunnableTasks();
        assertTrue(called.get());
    }

    public void testExecutorServiceWrapsRunnable() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final AtomicBoolean called = new AtomicBoolean();
        final ExecutorService executorService = taskQueue.getExecutorService(runnable -> () -> {
            assertFalse(called.get());
            called.set(true);
            runnable.run();
        });
        executorService.execute(() -> logger.info("runnable executed"));
        assertFalse(called.get());
        taskQueue.runAllRunnableTasks();
        assertTrue(called.get());
    }

    public void testThreadPoolSchedulesFutureTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);
        final long startTime = taskQueue.getCurrentTimeMillis();

        final List<String> strings = new ArrayList<>(5);

        final ThreadPool threadPool = taskQueue.getThreadPool();
        final long delayMillis = randomLongBetween(1, 100);

        threadPool.schedule(TimeValue.timeValueMillis(delayMillis), GENERIC, () -> strings.add("deferred"));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        threadPool.schedule(TimeValue.ZERO, GENERIC, () -> strings.add("runnable"));
        assertTrue(taskQueue.hasRunnableTasks());

        threadPool.schedule(TimeValue.MINUS_ONE, GENERIC, () -> strings.add("also runnable"));

        taskQueue.runAllTasks();

        assertThat(taskQueue.getCurrentTimeMillis(), is(startTime + delayMillis));
        assertThat(strings, contains("runnable", "also runnable", "deferred"));

        final long delayMillis1 = randomLongBetween(2, 100);
        final long delayMillis2 = randomLongBetween(1, delayMillis1 - 1);

        threadPool.schedule(TimeValue.timeValueMillis(delayMillis1), GENERIC, () -> strings.add("further deferred"));
        threadPool.schedule(TimeValue.timeValueMillis(delayMillis2), GENERIC, () -> strings.add("not quite so deferred"));

        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runAllTasks();
        assertThat(taskQueue.getCurrentTimeMillis(), is(startTime + delayMillis + delayMillis1));

        final TimeValue cancelledDelay = TimeValue.timeValueMillis(randomLongBetween(1, 100));
        final ScheduledFuture<?> future = threadPool.schedule(cancelledDelay, "", () -> strings.add("cancelled before execution"));

        future.cancel(false);
        taskQueue.runAllTasks(random());

        assertThat(strings, contains("runnable", "also runnable", "deferred", "not quite so deferred", "further deferred"));
    }

    private static DeterministicTaskQueue newTaskQueue() {
        return new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build());
    }
}
