/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class TaskExecutorTests extends ESTestCase {

    protected static ThreadPool threadPool;
    protected PrioritizedEsThreadPoolExecutor threadExecutor;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(getTestClass().getName());
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void setUpExecutor() {
        threadExecutor = EsExecutors.newSinglePrioritizing(
            getClass().getName() + "/" + getTestName(),
            daemonThreadFactory(Settings.EMPTY, "test_thread"),
            threadPool.getThreadContext(),
            threadPool.scheduler()
        );
    }

    @After
    public void shutDownThreadExecutor() {
        ThreadPool.terminate(threadExecutor, 10, TimeUnit.SECONDS);
    }

    protected interface TestListener {
        void onFailure(Exception e);

        default void processed() {
            // do nothing by default
        }
    }

    protected interface TestExecutor<T> {
        void execute(List<T> tasks);

        default String describeTasks(List<T> tasks) {
            return tasks.stream().map(T::toString).reduce((s1, s2) -> {
                if (s1.isEmpty()) {
                    return s2;
                } else if (s2.isEmpty()) {
                    return s1;
                } else {
                    return s1 + ", " + s2;
                }
            }).orElse("");
        }
    }

    protected abstract static class TestTask implements TestExecutor<TestTask>, TestListener {

        @Override
        public void execute(List<TestTask> tasks) {
            tasks.forEach(TestTask::run);
        }

        @Nullable
        public TimeValue timeout() {
            return null;
        }

        public Priority priority() {
            return Priority.NORMAL;
        }

        public abstract void run();
    }

    class UpdateTask extends SourcePrioritizedRunnable {
        final TestTask testTask;

        UpdateTask(String source, TestTask testTask) {
            super(testTask.priority(), source);
            this.testTask = testTask;
        }

        @Override
        public void run() {
            logger.trace("will process {}", source);
            testTask.execute(Collections.singletonList(testTask));
            testTask.processed();
        }
    }

    // can be overridden by TaskBatcherTests
    protected void submitTask(String source, TestTask testTask) {
        SourcePrioritizedRunnable task = new UpdateTask(source, testTask);
        TimeValue timeout = testTask.timeout();
        if (timeout != null) {
            threadExecutor.execute(task, timeout, () -> threadPool.generic().execute(() -> {
                logger.debug("task [{}] timed out after [{}]", task, timeout);
                testTask.onFailure(new ProcessClusterEventTimeoutException(timeout, source));
            }));
        } else {
            threadExecutor.execute(task);
        }
    }

    public void testTimedOutTaskCleanedUp() throws Exception {
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch blockCompleted = new CountDownLatch(1);
        TestTask blockTask = new TestTask() {

            @Override
            public void run() {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                blockCompleted.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        };
        submitTask("block-task", blockTask);

        final CountDownLatch block2 = new CountDownLatch(1);
        TestTask unblockTask = new TestTask() {

            @Override
            public void run() {
                block2.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                block2.countDown();
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.ZERO;
            }
        };
        submitTask("unblock-task", unblockTask);

        block.countDown();
        block2.await();
        blockCompleted.await();
    }

    public void testTimeoutTask() throws Exception {
        final CountDownLatch block = new CountDownLatch(1);
        TestTask test1 = new TestTask() {
            @Override
            public void run() {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        };
        submitTask("block-task", test1);

        final CountDownLatch timedOut = new CountDownLatch(1);
        final AtomicBoolean executeCalled = new AtomicBoolean();
        TestTask test2 = new TestTask() {

            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueMillis(2);
            }

            @Override
            public void run() {
                executeCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                timedOut.countDown();
            }
        };
        submitTask("block-task", test2);

        timedOut.await();
        block.countDown();
        final CountDownLatch allProcessed = new CountDownLatch(1);
        TestTask test3 = new TestTask() {

            @Override
            public void run() {
                allProcessed.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        };
        submitTask("block-task", test3);
        allProcessed.await(); // executed another task to double check that execute on the timed out update task is not called...
        assertThat(executeCalled.get(), equalTo(false));
    }

    static class TaskExecutor implements TestExecutor<Integer> {
        List<Integer> tasks = new ArrayList<>();

        @Override
        public void execute(List<Integer> tasks) {
            this.tasks.addAll(tasks);
        }
    }

    /**
     * Note, this test can only work as long as we have a single thread executor executing the state update tasks!
     */
    public void testPrioritizedTasks() throws Exception {
        BlockingTask block = new BlockingTask(Priority.IMMEDIATE);
        submitTask("test", block);
        int taskCount = randomIntBetween(5, 20);

        // will hold all the tasks in the order in which they were executed
        List<PrioritizedTask> tasks = new ArrayList<>(taskCount);
        CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            Priority priority = randomFrom(Priority.values());
            PrioritizedTask task = new PrioritizedTask(priority, latch, tasks);
            submitTask("test", task);
        }

        block.close();
        latch.await();

        Priority prevPriority = null;
        for (PrioritizedTask task : tasks) {
            if (prevPriority == null) {
                prevPriority = task.priority();
            } else {
                assertThat(task.priority().sameOrAfter(prevPriority), is(true));
            }
        }
    }

    protected static class BlockingTask extends TestTask implements Releasable {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Priority priority;

        BlockingTask(Priority priority) {
            super();
            this.priority = priority;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onFailure(Exception e) {}

        @Override
        public Priority priority() {
            return priority;
        }

        public void close() {
            latch.countDown();
        }

    }

    protected static class PrioritizedTask extends TestTask {
        private final CountDownLatch latch;
        private final List<PrioritizedTask> tasks;
        private final Priority priority;

        private PrioritizedTask(Priority priority, CountDownLatch latch, List<PrioritizedTask> tasks) {
            super();
            this.latch = latch;
            this.tasks = tasks;
            this.priority = priority;
        }

        @Override
        public void run() {
            tasks.add(this);
            latch.countDown();
        }

        @Override
        public Priority priority() {
            return priority;
        }

        @Override
        public void onFailure(Exception e) {
            latch.countDown();
        }
    }

}
