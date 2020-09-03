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
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.core.Is.is;

public class DeterministicTaskQueueTests extends ESTestCase {

    public void testRunRandomTask() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        taskQueue.scheduleNow(() -> strings.add("foo"));
        taskQueue.scheduleNow(() -> strings.add("bar"));

        assertThat(strings, empty());

        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runRandomTask();
        assertThat(strings, contains(oneOf("foo", "bar")));

        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runRandomTask();
        assertThat(strings, containsInAnyOrder("foo", "bar"));

        assertFalse(taskQueue.hasRunnableTasks());
    }

    public void testRunRandomTaskVariesOrder() {
        final List<String> strings1 = getResultsOfRunningRandomly(new Random(4520795446362137264L));
        final List<String> strings2 = getResultsOfRunningRandomly(new Random(266504691902226821L));
        assertThat(strings1, not(equalTo(strings2)));
    }

    private List<String> getResultsOfRunningRandomly(Random random) {
        final DeterministicTaskQueue taskQueue = newTaskQueue(random);
        final List<String> strings = new ArrayList<>(4);

        taskQueue.scheduleNow(() -> strings.add("foo"));
        taskQueue.scheduleNow(() -> strings.add("bar"));
        taskQueue.scheduleNow(() -> strings.add("baz"));
        taskQueue.scheduleNow(() -> strings.add("quux"));

        assertThat(strings, empty());

        while (taskQueue.hasRunnableTasks()) {
            taskQueue.runRandomTask();
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
        taskQueue.runRandomTask();
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
        taskQueue.runRandomTask();
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
        taskQueue.runRandomTask();
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

        taskQueue.runRandomTask();
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

        taskQueue.runRandomTask();
        assertThat(strings, contains("foo"));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis2));
        assertTrue(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());

        taskQueue.runRandomTask();
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

        taskQueue.runRandomTask();
        assertThat(strings, contains("foo"));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        final long executionTimeMillis3 = randomLongBetween(executionTimeMillis1 + 1, executionTimeMillis2 - 1);
        taskQueue.scheduleAt(executionTimeMillis3, () -> strings.add("baz"));

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis3));
        assertTrue(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runRandomTask();
        taskQueue.advanceTime();
        taskQueue.runRandomTask();
        assertThat(strings, contains("foo", "baz", "bar"));
        assertThat(taskQueue.getCurrentTimeMillis(), is(executionTimeMillis2));
        assertFalse(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
    }

    public void testRunInTimeOrder() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        final long executionTimeMillis1 = randomLongBetween(1, 100);
        final long executionTimeMillis2 = randomLongBetween(executionTimeMillis1 + 1, 200);

        taskQueue.scheduleAt(executionTimeMillis1, () -> strings.add("foo"));
        taskQueue.scheduleAt(executionTimeMillis2, () -> strings.add("bar"));

        taskQueue.runAllTasksInTimeOrder();
        assertThat(strings, contains("foo", "bar"));
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
            taskQueue.runRandomTask();
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

    public void testThreadPoolSchedulesFutureTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);
        final long startTime = taskQueue.getCurrentTimeMillis();

        final List<String> strings = new ArrayList<>(5);

        final ThreadPool threadPool = taskQueue.getThreadPool();
        final long delayMillis = randomLongBetween(1, 100);

        threadPool.schedule(() -> strings.add("deferred"), TimeValue.timeValueMillis(delayMillis), GENERIC);
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        threadPool.schedule(() -> strings.add("runnable"), TimeValue.ZERO, GENERIC);
        assertTrue(taskQueue.hasRunnableTasks());

        threadPool.schedule(() -> strings.add("also runnable"), TimeValue.MINUS_ONE, GENERIC);

        taskQueue.runAllTasks();

        assertThat(taskQueue.getCurrentTimeMillis(), is(startTime + delayMillis));
        assertThat(strings, containsInAnyOrder("runnable", "also runnable", "deferred"));

        final long delayMillis1 = randomLongBetween(2, 100);
        final long delayMillis2 = randomLongBetween(1, delayMillis1 - 1);

        threadPool.schedule(() -> strings.add("further deferred"), TimeValue.timeValueMillis(delayMillis1), GENERIC);
        threadPool.schedule(() -> strings.add("not quite so deferred"), TimeValue.timeValueMillis(delayMillis2), GENERIC);

        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        taskQueue.runAllTasks();
        assertThat(taskQueue.getCurrentTimeMillis(), is(startTime + delayMillis + delayMillis1));

        final TimeValue cancelledDelay = TimeValue.timeValueMillis(randomLongBetween(1, 100));
        final Scheduler.Cancellable cancelledBeforeExecution =
            threadPool.schedule(() -> strings.add("cancelled before execution"), cancelledDelay, "");

        cancelledBeforeExecution.cancel();
        taskQueue.runAllTasks();

        assertThat(strings, containsInAnyOrder("runnable", "also runnable", "deferred", "not quite so deferred", "further deferred"));
    }

    public void testDelayVariabilityAppliesToImmediateTasks() {
        final DeterministicTaskQueue deterministicTaskQueue = newTaskQueue();
        advanceToRandomTime(deterministicTaskQueue);
        final long variabilityMillis = randomLongBetween(100, 500);
        deterministicTaskQueue.setExecutionDelayVariabilityMillis(variabilityMillis);
        for (int i = 0; i < 100; i++) {
            deterministicTaskQueue.scheduleNow(() -> {});
        }

        final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
        deterministicTaskQueue.runAllTasks();
        final long elapsedTime = deterministicTaskQueue.getCurrentTimeMillis() - startTime;
        assertThat(elapsedTime, greaterThan(0L)); // fails with negligible probability 2^{-100}
        assertThat(elapsedTime, lessThanOrEqualTo(variabilityMillis));
    }

    public void testDelayVariabilityAppliesToFutureTasks() {
        final DeterministicTaskQueue deterministicTaskQueue = newTaskQueue();
        advanceToRandomTime(deterministicTaskQueue);
        final long nominalExecutionTime = randomLongBetween(0, 60000);
        final long variabilityMillis = randomLongBetween(1, 500);
        final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
        deterministicTaskQueue.setExecutionDelayVariabilityMillis(variabilityMillis);
        for (int i = 0; i < 100; i++) {
            deterministicTaskQueue.scheduleAt(nominalExecutionTime, () -> {});
        }
        final long expectedEndTime = deterministicTaskQueue.getLatestDeferredExecutionTime();
        assertThat(expectedEndTime, greaterThan(nominalExecutionTime)); // fails if every task has zero variability -- vanishingly unlikely
        assertThat(expectedEndTime, lessThanOrEqualTo(Math.max(startTime, nominalExecutionTime + variabilityMillis)));

        deterministicTaskQueue.runAllTasks();
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), is(expectedEndTime));
    }

    public void testThreadPoolSchedulesPeriodicFutureTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);
        final List<String> strings = new ArrayList<>(5);

        final ThreadPool threadPool = taskQueue.getThreadPool();
        final long intervalMillis = randomLongBetween(1, 100);

        final AtomicInteger counter = new AtomicInteger(0);
        Scheduler.Cancellable cancellable = threadPool.scheduleWithFixedDelay(
            () -> strings.add("periodic-" + counter.getAndIncrement()), TimeValue.timeValueMillis(intervalMillis), GENERIC);
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());

        for (int i = 0; i < 3; ++i) {
            taskQueue.advanceTime();
            assertTrue(taskQueue.hasRunnableTasks());
            taskQueue.runAllRunnableTasks();
        }

        assertThat(strings, contains("periodic-0", "periodic-1", "periodic-2"));

        cancellable.cancel();

        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertThat(strings, contains("periodic-0", "periodic-1", "periodic-2"));
    }

    public void testSameExecutor() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final ThreadPool threadPool = taskQueue.getThreadPool();
        final AtomicBoolean executed = new AtomicBoolean(false);
        final AtomicBoolean executedNested = new AtomicBoolean(false);
        threadPool.generic().execute(() -> {
            threadPool.executor(ThreadPool.Names.SAME).execute(() -> executedNested.set(true));
            assertThat(executedNested.get(), is(true));
            executed.set(true);
        });
        taskQueue.runAllRunnableTasks();
        assertThat(executed.get(), is(true));
    }

    static DeterministicTaskQueue newTaskQueue() {
        return newTaskQueue(random());
    }

    private static DeterministicTaskQueue newTaskQueue(Random random) {
        return new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random);
    }
}
