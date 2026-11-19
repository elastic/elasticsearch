/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.node.ShutdownPrepareService.PluginDrain;
import org.elasticsearch.node.ShutdownPrepareService.TaskScope;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.node.ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShutdownPrepareServiceTests extends ESTestCase {

    public void testAwaitTasksCompleteInternal_timesOut() {
        String actionName = "test:action/name";
        TaskManager taskManager = mock();
        long taskId = randomNonNegativeLong();
        String nodeId = randomAlphaOfLength(10);
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        when(taskManager.getTasks()).thenReturn(Map.of(taskId, task));

        boolean completed = ShutdownPrepareService.awaitTasksCompleteInternal(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            new FakeTime(),
            Set.of(actionName),
            randomFrom(TaskScope.values()),
            "finish",
            taskManager,
            null,
            null
        );
        assertThat(completed, is(false));
    }

    public void testAwaitTasksCompleteInternal_completes() {
        String actionName = "test:action/name";
        TaskManager taskManager = mock();
        long taskId = randomNonNegativeLong();
        String nodeId = randomAlphaOfLength(10);
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        when(taskManager.getTasks())
            // First call: non-empty
            .thenReturn(Map.of(taskId, task))
            // Second call: non-empty
            .thenReturn(Map.of(taskId, task))
            // Third call: empty
            .thenReturn(Map.of());

        boolean completed = ShutdownPrepareService.awaitTasksCompleteInternal(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            new FakeTime(),
            Set.of(actionName),
            randomFrom(TaskScope.values()),
            "finish",
            taskManager,
            null,
            null
        );
        assertThat(completed, is(true));
    }

    public void testAwaitTasksCompleteInternal_invokesNotifierExactlyOnceForEachTask() {
        String actionName = "test:action/name";
        TaskManager taskManager = mock();
        String nodeId = randomAlphaOfLength(10);
        long taskId1 = randomNonNegativeLong();
        long taskId2 = randomValueOtherThan(taskId1, ESTestCase::randomNonNegativeLong);
        long taskId3 = randomValueOtherThanMany(Set.of(taskId1, taskId2)::contains, ESTestCase::randomNonNegativeLong);
        BulkByPaginatedSearchTask task1 = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId1),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        BulkByPaginatedSearchTask task2 = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId2),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        BulkByPaginatedSearchTask task3 = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId3),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        when(taskManager.getTasks())
            // First call: tasks 1 and 2
            .thenReturn(Map.of(taskId1, task1, taskId2, task2))
            // Second call: tasks 2 and 3
            .thenReturn(Map.of(taskId2, task2, taskId3, task3))
            // Third call: task 2 only
            .thenReturn(Map.of(taskId2, task2));

        List<Task> notified = new ArrayList<>();
        ShutdownPrepareService.awaitTasksCompleteInternal(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            new FakeTime(),
            Set.of(actionName),
            TaskScope.ALL,
            "finish",
            taskManager,
            notified::add,
            null
        );
        assertThat(notified, containsInAnyOrder(task1, task2, task3)); // should notify each task exactly once
    }

    /**
     * Waits for the tasks of every listed action and for those only: the task of an unlisted action neither delays the completion nor
     * gets notified.
     */
    public void testAwaitTasksCompleteInternal_mixedActions() {
        String action1 = "test:action/one";
        String action2 = "test:action/two";
        TaskManager taskManager = mock();
        long taskId1 = randomNonNegativeLong();
        long taskId2 = randomValueOtherThan(taskId1, ESTestCase::randomNonNegativeLong);
        long taskId3 = randomValueOtherThanMany(Set.of(taskId1, taskId2)::contains, ESTestCase::randomNonNegativeLong);
        Task task1 = newTask(taskId1, action1);
        Task task2 = newTask(taskId2, action2);
        Task unlistedTask = newTask(taskId3, "test:action/unlisted");
        when(taskManager.getTasks())
            // First call: tasks of both actions, plus the unlisted one
            .thenReturn(Map.of(taskId1, task1, taskId2, task2, taskId3, unlistedTask))
            // Second call: only the second action's task, plus the unlisted one
            .thenReturn(Map.of(taskId2, task2, taskId3, unlistedTask))
            // Third call: the unlisted one only
            .thenReturn(Map.of(taskId3, unlistedTask));

        List<Task> notified = new ArrayList<>();
        boolean completed = ShutdownPrepareService.awaitTasksCompleteInternal(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            new FakeTime(),
            Set.of(action1, action2),
            randomFrom(TaskScope.values()),
            "finish",
            taskManager,
            notified::add,
            null
        );
        assertThat(completed, is(true));
        assertThat(notified, containsInAnyOrder(task1, task2));
        verify(taskManager, times(3)).getTasks();
    }

    /**
     * On timeout, the tasks still running are reported, but only those of the listed actions.
     */
    public void testAwaitTasksCompleteInternal_timesOutWithMixedActions() {
        String action1 = "test:action/one";
        String action2 = "test:action/two";
        TaskManager taskManager = mock();
        long taskId1 = randomNonNegativeLong();
        long taskId2 = randomValueOtherThan(taskId1, ESTestCase::randomNonNegativeLong);
        long taskId3 = randomValueOtherThanMany(Set.of(taskId1, taskId2)::contains, ESTestCase::randomNonNegativeLong);
        Task task1 = newTask(taskId1, action1);
        Task task2 = newTask(taskId2, action2);
        Task unlistedTask = newTask(taskId3, "test:action/unlisted");
        when(taskManager.getTasks()).thenReturn(Map.of(taskId1, task1, taskId2, task2, taskId3, unlistedTask));

        List<Task> timedOut = new ArrayList<>();
        boolean completed = ShutdownPrepareService.awaitTasksCompleteInternal(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            new FakeTime(),
            Set.of(action1, action2),
            randomFrom(TaskScope.values()),
            "finish",
            taskManager,
            null,
            timedOut::addAll
        );
        assertThat(completed, is(false));
        assertThat(timedOut, containsInAnyOrder(task1, task2));
    }

    /**
     * A task that starts after the first poll is waited for (and notified) with the {@link TaskScope#ALL} scope, but ignored with the
     * {@link TaskScope#INITIAL} one.
     */
    public void testAwaitTasksCompleteInternal_scopeOfTasksStartedWhileWaiting() {
        String actionName = "test:action/name";
        TaskManager taskManager = mock();
        long taskId1 = randomNonNegativeLong();
        long taskId2 = randomValueOtherThan(taskId1, ESTestCase::randomNonNegativeLong);
        Task task1 = newTask(taskId1, actionName);
        Task task2 = newTask(taskId2, actionName);
        when(taskManager.getTasks())
            // First call: task 1
            .thenReturn(Map.of(taskId1, task1))
            // Second call: tasks 1 and 2
            .thenReturn(Map.of(taskId1, task1, taskId2, task2))
            // Later calls: task 2 only
            .thenReturn(Map.of(taskId2, task2));

        TaskScope scope = randomFrom(TaskScope.values());
        List<Task> notified = new ArrayList<>();
        List<Task> timedOut = new ArrayList<>();
        boolean completed = ShutdownPrepareService.awaitTasksCompleteInternal(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            new FakeTime(),
            Set.of(actionName),
            scope,
            "finish",
            taskManager,
            notified::add,
            timedOut::addAll
        );
        switch (scope) {
            case ALL -> {
                assertThat(completed, is(false));
                assertThat(notified, containsInAnyOrder(task1, task2));
                assertThat(timedOut, contains(task2));
            }
            case INITIAL -> {
                assertThat(completed, is(true));
                assertThat(notified, contains(task1));
                assertThat(timedOut, empty());
            }
        }
    }

    private static Task newTask(long id, String action) {
        return new Task(id, "transport", action, "description", TaskId.EMPTY_TASK_ID, Map.of());
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_nonRelocatableLeader() {
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            "transport",
            "test:action/name",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        task.setWorkerCount(randomIntBetween(1, 20), Float.POSITIVE_INFINITY);
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_relocatableLeader() {
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            "transport",
            "test:action/name",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            true,
            randomOrigin()
        );
        task.setWorkerCount(randomIntBetween(1, 20), Float.POSITIVE_INFINITY);
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_nonRelocatableWorker() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomNonNegativeLong();
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId),
            "transport",
            "test:action/name",
            "description",
            new TaskId(nodeId, randomValueOtherThan(taskId, ESTestCase::randomNonNegativeLong)),
            Map.of(),
            false,
            randomOrigin()
        );
        task.setWorker(randomFloat(), randomInt());
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_relocatableWorker() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomNonNegativeLong();
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId),
            "transport",
            "test:action/name",
            "description",
            new TaskId(nodeId, randomValueOtherThan(taskId, ESTestCase::randomNonNegativeLong)),
            Map.of(),
            true,
            randomOrigin()
        );
        task.setWorker(randomFloat(), randomInt());
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_notBulkByPaginatedSearch() {
        Task task = new Task(randomInt(), "transport", "test:action/name", "description", new TaskId("localNode", randomLong()), Map.of());
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        // No assertion, just check it doesn't blow up
    }

    private static ResumeInfo.RelocationOrigin randomOrigin() {
        return randomBoolean()
            ? null
            : new ResumeInfo.RelocationOrigin(new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()), randomNonNegativeLong());
    }

    /**
     * The wait is measured on the sleeper's clock, so time spent outside the sleeps counts too, and the last sleep is shortened to end
     * at the timeout.
     */
    public void testAwaitTasksCompleteInternal_measuresTimeOnClock() {
        final var fakeTime = new FakeTime();
        final TaskManager taskManager = mock();
        final long taskId = randomNonNegativeLong();
        final Task task = newTask(taskId, "test:action/name");
        // each poll takes 400ms
        when(taskManager.getTasks()).thenAnswer(invocation -> {
            fakeTime.advance(timeValueMillis(400));
            return Map.of(taskId, task);
        });

        final boolean completed = ShutdownPrepareService.awaitTasksCompleteInternal(
            timeValueMillis(1500),
            fakeTime,
            Set.of("test:action/name"),
            randomFrom(TaskScope.values()),
            "finish",
            taskManager,
            null,
            null
        );
        // polls end at 400ms, 1300ms and 1900ms; the second leaves 200ms
        assertThat(completed, is(false));
        assertThat(fakeTime.sleepIntervals, contains(AWAIT_TASKS_POLL_INTERVAL, timeValueMillis(200)));
        verify(taskManager, times(3)).getTasks();
    }

    /**
     * A timeout shorter than the granularity of the sleeps polls once and returns.
     */
    public void testAwaitTasksCompleteInternal_subMillisecondTimeoutPollsOnce() {
        final var fakeTime = new FakeTime();
        final TaskManager taskManager = mock();
        final long taskId = randomNonNegativeLong();
        final Task task = newTask(taskId, "test:action/name");
        when(taskManager.getTasks()).thenReturn(Map.of(taskId, task));

        final List<Task> timedOut = new ArrayList<>();
        final boolean completed = ShutdownPrepareService.awaitTasksCompleteInternal(
            TimeValue.timeValueNanos(randomLongBetween(1, 999_999)),
            fakeTime,
            Set.of("test:action/name"),
            randomFrom(TaskScope.values()),
            "finish",
            taskManager,
            null,
            timedOut::addAll
        );
        assertThat(completed, is(false));
        assertThat(timedOut, contains(task));
        assertThat(fakeTime.sleeps(), is(0));
        verify(taskManager, times(1)).getTasks();
    }

    // Tests of prepareForShutdown(): the order of the hooks, the plugin drains and the task wait, and the shutdown deadline.
    // A test that holds a task until it has seen the wait for it uses an unbounded grace period: a bounded one would let the wait time
    // out on the fake clock, which runs much faster than the real one, before the test gets to see it.

    private static final String AWAITED_ACTION = "test:action/awaited";
    private static final int SHUTDOWN_BUDGET_POLLS = 200;
    // long in real time too, so that the real waits for the hooks and the plugin drains do not time out under load
    private static final TimeValue SHUTDOWN_BUDGET = timeValueMillis(SHUTDOWN_BUDGET_POLLS * AWAIT_TASKS_POLL_INTERVAL.millis());

    private TestThreadPool threadPool;
    private TaskManager taskManager;
    private TransportService transportService;
    private FakeTime fakeTime;

    @Before
    public void setUpShutdown() {
        threadPool = new TestThreadPool(getTestName());
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        // The service only takes the task manager from the transport service; building a real one needs a whole transport stack.
        transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        fakeTime = new FakeTime();
    }

    @After
    public void tearDownShutdown() {
        terminate(threadPool);
    }

    /**
     * With a termination handler, the drain starts only once the handler and every other hook have finished: a task registered after
     * the handler returned, while another hook was still running, is waited for, and so is the plugin drain.
     */
    public void testPrepareForShutdown_drainStartsAfterHandlerAndOtherHooks() throws Exception {
        final var handler = new HeldHandler(() -> TerminationHandler.Outcome.READY);
        final var drain = new TestDrain(SubscribableListener.nullSuccess());
        final var service = newService(TimeValue.ZERO, handler, List.of(drain.pluginDrain()));
        final var otherHookRelease = new CountDownLatch(1);
        service.addShutdownHook("other-hook", () -> safeAwait(otherHookRelease));

        final Task task1 = registerTask();
        final Thread shutdown = startShutdown(service);
        safeAwait(handler.entered);
        assertDrainNotStarted(drain);

        handler.release.countDown();
        safeAwait(handler.exited);
        final Task task2 = registerTask();
        assertDrainNotStarted(drain);

        otherHookRelease.countDown();
        assertBusy(() -> assertThat(fakeTime.sleeps(), greaterThanOrEqualTo(1)));
        assertThat(drain.starts.get(), is(1));
        taskManager.unregister(task1);
        assertStillWaiting(shutdown);
        taskManager.unregister(task2);
        safeJoin(shutdown);

        assertThat(drain.starts.get(), is(1));
        assertThat(service.awaitTimeouts, contains(TimeValue.ZERO));
        assertThat(service.awaitResults, contains(true));
    }

    /**
     * A handler that does not report the node ready, or that throws, skips the plugin drains. The tasks are still waited for, but
     * only those running when the wait starts.
     */
    public void testPrepareForShutdown_handlerNotReadySkipsPluginDrains() throws Exception {
        final TerminationHandler handler = randomBoolean() ? () -> TerminationHandler.Outcome.NOT_READY : () -> {
            throw new ElasticsearchException("simulated handler failure");
        };
        final var drain = new TestDrain(SubscribableListener.nullSuccess());
        final var service = newService(TimeValue.ZERO, handler, List.of(drain.pluginDrain()));

        final Task task1 = registerTask();
        final Thread shutdown = startShutdown(service);
        assertBusy(() -> assertThat(fakeTime.sleeps(), greaterThanOrEqualTo(1)));
        assertTrue(shutdown.isAlive());
        final Task task2 = registerTask();
        taskManager.unregister(task1);
        safeJoin(shutdown);
        taskManager.unregister(task2);

        assertThat(drain.starts.get(), is(0));
        assertThat(service.awaitResults, contains(true));
    }

    /**
     * Even on a ready node with its plugin drains completed, only the tasks running when the wait starts are waited for: senders that
     * do not see the shutdown, such as a remote cluster, can keep new tasks arriving until the node closes.
     */
    public void testPrepareForShutdown_readyNodeDoesNotWaitForTrailingTasks() throws Exception {
        final List<PluginDrain> drains = randomBoolean()
            ? List.of()
            : List.of(new TestDrain(SubscribableListener.nullSuccess()).pluginDrain());
        final var service = newService(TimeValue.ZERO, () -> TerminationHandler.Outcome.READY, drains);

        final Task task1 = registerTask();
        final Thread shutdown = startShutdown(service);
        assertBusy(() -> assertThat(fakeTime.sleeps(), greaterThanOrEqualTo(1)));
        final Task task2 = registerTask();
        taskManager.unregister(task1);
        safeJoin(shutdown);
        taskManager.unregister(task2);

        assertThat(service.awaitResults, contains(true));
    }

    /**
     * Once the handler reports the node ready, the plugin drains start once each, a drain completed before the service subscribes is
     * observed, a failed drain counts as completed, and the tasks are captured only after all the drains completed.
     */
    public void testPrepareForShutdown_tasksCapturedAfterPluginDrains() throws Exception {
        final var doneDrain = new TestDrain(SubscribableListener.nullSuccess());
        final var pendingDrain = new TestDrain(new SubscribableListener<>());
        final var service = newService(
            TimeValue.ZERO,
            () -> TerminationHandler.Outcome.READY,
            shuffledList(List.of(doneDrain.pluginDrain(), pendingDrain.pluginDrain()))
        );

        final Thread shutdown = startShutdown(service);
        safeAwait(pendingDrain.started);
        final Task task = registerTask();
        assertThat(fakeTime.sleeps(), is(0));
        assertTrue(shutdown.isAlive());

        if (randomBoolean()) {
            pendingDrain.completion.onResponse(null);
        } else {
            pendingDrain.completion.onFailure(new ElasticsearchException("simulated drain failure"));
        }
        assertBusy(() -> assertThat(fakeTime.sleeps(), greaterThanOrEqualTo(1)));
        assertTrue(shutdown.isAlive());
        taskManager.unregister(task);
        safeJoin(shutdown);

        assertThat(doneDrain.starts.get(), is(1));
        assertThat(pendingDrain.starts.get(), is(1));
        assertThat(service.awaitResults, contains(true));
    }

    /**
     * A plugin drain that fails to start, or returns no completion, holds up neither the other drains nor the task wait.
     */
    public void testPrepareForShutdown_failingPluginDrainStillDrainsTasks() throws Exception {
        final var badDrain = new PluginDrain("bad-drain", randomBoolean() ? () -> null : () -> {
            throw new ElasticsearchException("simulated drain start failure");
        });
        final var goodDrain = new TestDrain(SubscribableListener.nullSuccess());
        final var service = newService(
            TimeValue.ZERO,
            () -> TerminationHandler.Outcome.READY,
            shuffledList(List.of(badDrain, goodDrain.pluginDrain()))
        );

        final Task task = registerTask();
        final Thread shutdown = startShutdown(service);
        assertBusy(() -> assertThat(fakeTime.sleeps(), greaterThanOrEqualTo(1)));
        assertTrue(shutdown.isAlive());
        taskManager.unregister(task);
        safeJoin(shutdown);

        assertThat(goodDrain.starts.get(), is(1));
        assertThat(service.awaitResults, contains(true));
    }

    /**
     * Without a termination handler (stateful), the tasks are captured when the shutdown starts, while the other hooks still run: a
     * later task is not waited for, and the plugin drains never run.
     */
    public void testPrepareForShutdown_withoutHandlerCapturesTasksAtShutdownStart() throws Exception {
        final var drain = new TestDrain(SubscribableListener.nullSuccess());
        final var service = newService(TimeValue.ZERO, null, List.of(drain.pluginDrain()));
        final var otherHookRelease = new CountDownLatch(1);
        service.addShutdownHook("other-hook", () -> safeAwait(otherHookRelease));

        final Task task1 = registerTask();
        final Thread shutdown = startShutdown(service);
        assertBusy(() -> assertThat(fakeTime.sleeps(), greaterThanOrEqualTo(1)));
        final Task task2 = registerTask();
        taskManager.unregister(task1);
        assertBusy(() -> assertThat(service.awaitResults, contains(true)));
        assertTrue(shutdown.isAlive());

        otherHookRelease.countDown();
        safeJoin(shutdown);
        taskManager.unregister(task2);

        assertThat(drain.starts.get(), is(0));
        assertThat(service.awaitTimeouts, contains(TimeValue.ZERO));
    }

    /**
     * The task wait gets the part of the grace period that the hooks left, and ends at the deadline.
     */
    public void testPrepareForShutdown_partiallyConsumedBudget() {
        final int consumedPolls = randomIntBetween(1, SHUTDOWN_BUDGET_POLLS - 60);
        final var drain = new TestDrain(SubscribableListener.nullSuccess());
        final var service = newService(SHUTDOWN_BUDGET, () -> TerminationHandler.Outcome.READY, List.of(drain.pluginDrain()));
        service.addShutdownHook("slow-hook", () -> fakeTime.advance(timeValueMillis(consumedPolls * AWAIT_TASKS_POLL_INTERVAL.millis())));

        final Task task = registerTask();
        service.prepareForShutdown();
        taskManager.unregister(task);

        final int remainingPolls = SHUTDOWN_BUDGET_POLLS - consumedPolls;
        assertThat(drain.starts.get(), is(1));
        assertThat(service.awaitTimeouts, contains(timeValueMillis(remainingPolls * AWAIT_TASKS_POLL_INTERVAL.millis())));
        assertThat(service.awaitResults, contains(false));
        assertThat(fakeTime.sleeps(), is(remainingPolls));
    }

    /**
     * A remainder shorter than one poll interval still bounds the task wait, which then sleeps for that remainder only.
     */
    public void testPrepareForShutdown_remainderBelowOnePollInterval() {
        final var remainder = timeValueMillis(randomLongBetween(1, AWAIT_TASKS_POLL_INTERVAL.millis() - 1));
        final var drain = new TestDrain(
            SubscribableListener.nullSuccess(),
            () -> fakeTime.advance(timeValueMillis(SHUTDOWN_BUDGET.millis() - remainder.millis()))
        );
        final var service = newService(SHUTDOWN_BUDGET, () -> TerminationHandler.Outcome.READY, List.of(drain.pluginDrain()));

        final Task task = registerTask();
        service.prepareForShutdown();
        taskManager.unregister(task);

        assertThat(drain.starts.get(), is(1));
        assertThat(service.awaitTimeouts, contains(remainder));
        assertThat(service.awaitResults, contains(false));
        assertThat(fakeTime.sleepIntervals, contains(remainder));
    }

    /**
     * A grace period used up by the hooks skips the plugin drains, and the tasks are polled once, never waited for without bound.
     */
    public void testPrepareForShutdown_budgetExhaustedDuringHook() {
        final var drain = new TestDrain(SubscribableListener.nullSuccess());
        final var service = newService(SHUTDOWN_BUDGET, () -> TerminationHandler.Outcome.READY, List.of(drain.pluginDrain()));
        service.addShutdownHook(
            "slow-hook",
            () -> fakeTime.advance(timeValueMillis(SHUTDOWN_BUDGET.millis() + randomLongBetween(0, 1000)))
        );

        final Task task = registerTask();
        service.prepareForShutdown();
        taskManager.unregister(task);

        assertThat(drain.starts.get(), is(0));
        assertThat(service.awaitTimeouts, contains(TimeValue.timeValueNanos(1)));
        assertThat(service.awaitResults, contains(false));
        assertThat(fakeTime.sleeps(), is(0));
    }

    /**
     * A grace period used up by the plugin drains leaves them unfinished, and the tasks are polled once.
     */
    public void testPrepareForShutdown_budgetExhaustedDuringPluginDrain() {
        final var drain = new TestDrain(
            new SubscribableListener<>(),
            () -> fakeTime.advance(timeValueMillis(SHUTDOWN_BUDGET.millis() + randomLongBetween(0, 1000)))
        );
        final var service = newService(SHUTDOWN_BUDGET, () -> TerminationHandler.Outcome.READY, List.of(drain.pluginDrain()));

        final Task task = registerTask();
        service.prepareForShutdown();
        taskManager.unregister(task);

        assertThat(drain.starts.get(), is(1));
        assertFalse(drain.completion.isDone());
        assertThat(service.awaitTimeouts, contains(TimeValue.timeValueNanos(1)));
        assertThat(service.awaitResults, contains(false));
        assertThat(fakeTime.sleeps(), is(0));
    }

    /**
     * An interrupt during the plugin drains ends the waits and is preserved.
     */
    public void testPrepareForShutdown_interruptedDuringPluginDrain() {
        final var drain = new TestDrain(new SubscribableListener<>());
        final var service = newService(TimeValue.ZERO, () -> TerminationHandler.Outcome.READY, List.of(drain.pluginDrain()));

        final Task task = registerTask();
        final var interruptedAtEnd = new AtomicBoolean();
        final var shutdown = new Thread(() -> {
            service.prepareForShutdown();
            interruptedAtEnd.set(Thread.currentThread().isInterrupted());
        }, "test-shutdown");
        shutdown.start();
        safeAwait(drain.started);
        shutdown.interrupt();
        safeJoin(shutdown);
        taskManager.unregister(task);

        assertTrue(interruptedAtEnd.get());
        assertFalse(drain.completion.isDone());
        assertThat(service.awaitTimeouts, contains(TimeValue.ZERO));
        assertThat(service.awaitResults, contains(false));
        assertThat(fakeTime.sleeps(), is(0));
    }

    /**
     * An interrupt while another hook still runs skips the plugin drains, even though the handler reported the node ready: they could
     * not be waited for. The interrupt is preserved.
     */
    public void testPrepareForShutdown_interruptedDuringHooksSkipsPluginDrains() throws Exception {
        final var handler = new HeldHandler(() -> TerminationHandler.Outcome.READY);
        handler.release.countDown();
        final var drain = new TestDrain(SubscribableListener.nullSuccess());
        final var service = newService(TimeValue.ZERO, handler, List.of(drain.pluginDrain()));
        final var otherHookRelease = new CountDownLatch(1);
        final var otherHookThread = new AtomicReference<Thread>();
        final var otherHookEntered = new CountDownLatch(1);
        service.addShutdownHook("other-hook", () -> {
            otherHookThread.set(Thread.currentThread());
            otherHookEntered.countDown();
            safeAwait(otherHookRelease);
        });

        final Task task = registerTask();
        final var interruptedAtEnd = new AtomicBoolean();
        final var shutdown = new Thread(() -> {
            service.prepareForShutdown();
            interruptedAtEnd.set(Thread.currentThread().isInterrupted());
        }, "test-shutdown");
        shutdown.start();
        // the handler's hook thread ends once it recorded the outcome
        safeAwait(handler.exited);
        safeJoin(handler.thread.get());
        // the hooks start in no particular order, and the other hook has to have published its thread to be joined below
        safeAwait(otherHookEntered);
        shutdown.interrupt();
        safeJoin(shutdown);
        otherHookRelease.countDown();
        safeJoin(otherHookThread.get());
        taskManager.unregister(task);

        assertTrue(interruptedAtEnd.get());
        assertThat(drain.starts.get(), is(0));
        assertThat(service.awaitResults, contains(false));
    }

    private RecordingShutdownPrepareService newService(
        TimeValue maxTimeout,
        @Nullable TerminationHandler terminationHandler,
        List<PluginDrain> pluginDrains
    ) {
        final var settings = Settings.builder()
            .put(ShutdownPrepareService.MAXIMUM_SHUTDOWN_TIMEOUT_SETTING.getKey(), maxTimeout.getStringRep())
            .build();
        return new RecordingShutdownPrepareService(settings, terminationHandler, pluginDrains);
    }

    private Task registerTask() {
        return taskManager.register("transport", AWAITED_ACTION, new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }
        });
    }

    private static Thread startShutdown(ShutdownPrepareService service) {
        final var shutdown = new Thread(service::prepareForShutdown, "test-shutdown");
        shutdown.start();
        return shutdown;
    }

    private void assertDrainNotStarted(TestDrain drain) {
        assertThat(drain.starts.get(), is(0));
        assertThat(fakeTime.sleeps(), is(0));
    }

    /**
     * Asserts that the shutdown polled the tasks again after the call and still found some to wait for. The poll preceding the second
     * sleep that ends after the call started after it.
     */
    private void assertStillWaiting(Thread shutdown) throws Exception {
        final int sleeps = fakeTime.sleeps();
        assertBusy(() -> assertThat(fakeTime.sleeps(), greaterThanOrEqualTo(sleeps + 2)));
        assertTrue(shutdown.isAlive());
    }

    /**
     * Records the timeouts and the results of the waits for the tasks of {@link #AWAITED_ACTION}.
     */
    private class RecordingShutdownPrepareService extends ShutdownPrepareService {
        final List<TimeValue> awaitTimeouts = new CopyOnWriteArrayList<>();
        final List<Boolean> awaitResults = new CopyOnWriteArrayList<>();

        RecordingShutdownPrepareService(Settings settings, @Nullable TerminationHandler terminationHandler, List<PluginDrain> drains) {
            super(
                settings,
                // an interface of which the service only calls close()
                mock(HttpServerTransport.class),
                transportService,
                terminationHandler,
                List.of(AWAITED_ACTION),
                drains,
                fakeTime
            );
        }

        @Override
        protected boolean awaitTasksComplete(
            TimeValue timeout,
            Sleeper sleeper,
            Set<String> actionNames,
            TaskScope scope,
            TaskManager taskManager,
            @Nullable Consumer<Task> taskNotifier,
            @Nullable Consumer<List<Task>> onTimeout
        ) {
            final boolean awaited = actionNames.contains(AWAITED_ACTION);
            if (awaited) {
                assertThat(scope, equalTo(TaskScope.INITIAL));
                awaitTimeouts.add(timeout);
            }
            final boolean result = super.awaitTasksComplete(timeout, sleeper, actionNames, scope, taskManager, taskNotifier, onTimeout);
            if (awaited) {
                awaitResults.add(result);
            }
            return result;
        }
    }

    /**
     * The clock of the waits, advanced by each sleep by the time slept. A sleep also pauses for a millisecond of real time, so that a
     * wait for a task the test holds does not spin.
     */
    private static class FakeTime extends ShutdownPrepareService.Sleeper {
        private final AtomicLong nanos = new AtomicLong(randomLongBetween(-1L << 40, 1L << 40));
        final List<TimeValue> sleepIntervals = new CopyOnWriteArrayList<>();

        @Override
        void sleep(TimeValue interval) throws InterruptedException {
            Thread.sleep(1); // throws if interrupted, like a real sleep
            sleepIntervals.add(interval);
            nanos.addAndGet(interval.nanos());
        }

        @Override
        long relativeTimeNanos() {
            return nanos.get();
        }

        void advance(TimeValue timeValue) {
            nanos.addAndGet(timeValue.nanos());
        }

        int sleeps() {
            return sleepIntervals.size();
        }
    }

    /**
     * A termination handler that blocks until released.
     */
    private static class HeldHandler implements TerminationHandler {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch exited = new CountDownLatch(1);
        final AtomicReference<Thread> thread = new AtomicReference<>();
        private final Supplier<Outcome> outcome;

        HeldHandler(Supplier<Outcome> outcome) {
            this.outcome = outcome;
        }

        @Override
        public Outcome handleTermination() {
            thread.set(Thread.currentThread());
            entered.countDown();
            try {
                safeAwait(release);
                return outcome.get();
            } finally {
                exited.countDown();
            }
        }
    }

    /**
     * A plugin drain that counts its starts, runs {@code onStart} on each and returns {@code completion}.
     */
    private static class TestDrain {
        final AtomicInteger starts = new AtomicInteger();
        final CountDownLatch started = new CountDownLatch(1);
        final SubscribableListener<Void> completion;
        private final Runnable onStart;

        TestDrain(SubscribableListener<Void> completion) {
            this(completion, () -> {});
        }

        TestDrain(SubscribableListener<Void> completion, Runnable onStart) {
            this.completion = completion;
            this.onStart = onStart;
        }

        PluginDrain pluginDrain() {
            return new PluginDrain("test-drain-" + System.identityHashCode(this), () -> {
                starts.incrementAndGet();
                onStart.run();
                started.countDown();
                return completion;
            });
        }
    }
}
