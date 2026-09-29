/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class DriverTaskRunnerTests extends ESTestCase {
    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";

    private TestThreadPool threadPool;
    private MockTransportService transportService;
    private TaskManager taskManager;

    @Before
    public void setUpTransportService() {
        threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(Settings.EMPTY, ESQL_TEST_EXECUTOR, 8, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        taskManager = transportService.getTaskManager();
        taskManager.setTaskCancellationService(new TaskCancellationService(transportService));
        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @After
    public void tearDownTransportService() {
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRegistersChildTasksWhileDriversRun() throws Exception {
        CancellableTask parentTask = registerParentTask();
        int numDrivers = between(1, 4);
        CountDownLatch started = new CountDownLatch(numDrivers);
        CountDownLatch release = new CountDownLatch(1);
        List<Driver> drivers = new ArrayList<>();
        for (int i = 0; i < numDrivers; i++) {
            drivers.add(newDriver(page -> {
                started.countDown();
                safeAwait(release);
                page.releaseBlocks();
            }));
        }
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        new DriverTaskRunner(transportService).executeDrivers(parentTask, drivers, threadPool.executor(ESQL_TEST_EXECUTOR), future);
        safeAwait(started);
        List<CancellableTask> childTasks = childTasks(parentTask);
        assertThat(childTasks, hasSize(numDrivers));
        for (CancellableTask childTask : childTasks) {
            assertThat(childTask.getAction(), equalTo(DriverTaskRunner.ACTION_NAME));
            assertThat(childTask.getParentTaskId(), equalTo(new TaskId(taskManager.getNodeId(), parentTask.getId())));
        }
        release.countDown();
        safeGet(future);
        assertThat(childTasks(parentTask), empty());
        for (Driver driver : drivers) {
            assertTrue(driver.driverContext().isFinished());
        }
    }

    public void testAbortsDriversWhenParentTaskIsCancelled() throws Exception {
        PlainActionFuture<Void> cancelled = new PlainActionFuture<>();
        CancellableTask parentTask = registerParentTask();
        taskManager.cancelTaskAndDescendants(parentTask, "test", false, cancelled);
        safeGet(cancelled);
        int numDrivers = between(1, 4);
        List<Driver> drivers = new ArrayList<>();
        for (int i = 0; i < numDrivers; i++) {
            drivers.add(newDriver(Page::releaseBlocks));
        }
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        new DriverTaskRunner(transportService).executeDrivers(parentTask, drivers, threadPool.executor(ESQL_TEST_EXECUTOR), future);
        Exception failure = expectThrows(Exception.class, () -> future.get(30, TimeUnit.SECONDS));
        assertThat(failure.getCause(), instanceOf(TaskCancelledException.class));
        assertThat(childTasks(parentTask), empty());
        for (Driver driver : drivers) {
            assertTrue(driver.driverContext().isFinished());
        }
    }

    CancellableTask registerParentTask() {
        return (CancellableTask) taskManager.register("transport", "test-action", new UntypedActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new CancellableTask(id, type, action, "parent", parentTaskId, headers);
            }
        });
    }

    private List<CancellableTask> childTasks(Task parentTask) {
        TaskId parentTaskId = new TaskId(taskManager.getNodeId(), parentTask.getId());
        List<CancellableTask> children = new ArrayList<>();
        for (CancellableTask task : taskManager.getCancellableTasks().values()) {
            if (task.getParentTaskId().equals(parentTaskId)) {
                children.add(task);
            }
        }
        return children;
    }

    private static Driver newDriver(Consumer<Page> pageConsumer) {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        DriverContext driverContext = new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
        List<Page> pages = List.of(new Page(driverContext.blockFactory().newConstantIntBlockWith(1, 1)));
        return TestDriverFactory.create(
            driverContext,
            new CannedSourceOperator(pages.iterator()),
            List.of(),
            new PageConsumerOperator(pageConsumer)
        );
    }
}
