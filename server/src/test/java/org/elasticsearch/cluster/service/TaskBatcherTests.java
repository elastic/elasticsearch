/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class TaskBatcherTests extends TaskExecutorTests {

    protected TestTaskBatcher taskBatcher;

    @Before
    public void setUpBatchingTaskExecutor() throws Exception {
        taskBatcher = new TestTaskBatcher(logger, threadExecutor);
    }

    static class TestTaskBatcher extends TaskBatcher {

        TestTaskBatcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
            super(logger, threadExecutor);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary) {
            List<UpdateTask> updateTasks = (List<UpdateTask>) tasks;
            ((TestExecutor<Object>) batchingKey).execute(updateTasks.stream().map(t -> t.task).toList());
            updateTasks.forEach(updateTask -> updateTask.listener.processed());
        }

        @Override
        protected void onTimeout(BatchedTask task, TimeValue timeout) {
            threadPool.generic()
                .execute(() -> ((UpdateTask) task).listener.onFailure(new ProcessClusterEventTimeoutException(timeout, task.source)));
        }

        class UpdateTask extends BatchedTask {
            final TestListener listener;

            UpdateTask(Priority priority, String source, Object task, TestListener listener, TestExecutor<?> executor) {
                super(priority, source, executor, task);
                this.listener = listener;
            }

            @Override
            @SuppressWarnings("unchecked")
            public String describeTasks(List<? extends BatchedTask> tasks) {
                return ((TestExecutor<Object>) batchingKey).describeTasks(tasks.stream().map(BatchedTask::getTask).toList());
            }
        }

    }

    @Override
    protected void submitTask(String source, TestTask testTask) {
        submitTask(source, testTask, testTask, testTask, testTask);
    }

    private <T> void submitTask(String source, T task, ClusterStateTaskConfig config, TestExecutor<T> executor, TestListener listener) {
        taskBatcher.submitTask(taskBatcher.new UpdateTask(config.priority(), source, task, listener, executor), config.timeout());
    }

    @Override
    public void testTimedOutTaskCleanedUp() throws Exception {
        super.testTimedOutTaskCleanedUp();
        assertTrue("expected empty map but was " + taskBatcher.tasksPerBatchingKey, taskBatcher.tasksPerBatchingKey.isEmpty());
    }

    public void testOneExecutorDoesntStarveAnother() throws InterruptedException {
        final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
        final Semaphore allowProcessing = new Semaphore(0);
        final Semaphore startedProcessing = new Semaphore(0);

        class TaskExecutor implements TestExecutor<String> {

            @Override
            public void execute(List<String> tasks) {
                executionOrder.addAll(tasks); // do this first, so startedProcessing can be used as a notification that this is done.
                startedProcessing.release(tasks.size());
                try {
                    allowProcessing.acquire(tasks.size());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        TaskExecutor executorA = new TaskExecutor();
        TaskExecutor executorB = new TaskExecutor();

        final ClusterStateTaskConfig config = ClusterStateTaskConfig.build(Priority.NORMAL);
        final TestListener noopListener = e -> { throw new AssertionError(e); };
        // this blocks the cluster state queue, so we can set it up right
        submitTask("0", "A0", config, executorA, noopListener);
        // wait to be processed
        startedProcessing.acquire(1);
        assertThat(executionOrder, equalTo(Arrays.asList("A0")));

        // these will be the first batch
        submitTask("1", "A1", config, executorA, noopListener);
        submitTask("2", "A2", config, executorA, noopListener);

        // release the first 0 task, but not the second
        allowProcessing.release(1);
        startedProcessing.acquire(2);
        assertThat(executionOrder, equalTo(Arrays.asList("A0", "A1", "A2")));

        // setup the queue with pending tasks for another executor same priority
        submitTask("3", "B3", config, executorB, noopListener);
        submitTask("4", "B4", config, executorB, noopListener);

        submitTask("5", "A5", config, executorA, noopListener);
        submitTask("6", "A6", config, executorA, noopListener);

        // now release the processing
        allowProcessing.release(6);

        // wait for last task to be processed
        startedProcessing.acquire(4);

        assertThat(executionOrder, equalTo(Arrays.asList("A0", "A1", "A2", "B3", "B4", "A5", "A6")));
    }

    static class TaskExecutor implements TestExecutor<Integer> {
        List<Integer> tasks = new ArrayList<>();

        @Override
        public void execute(List<Integer> tasks) {
            this.tasks.addAll(tasks);
        }
    }

    // test that for a single thread, tasks are executed in the order
    // that they are submitted
    public void testTasksAreExecutedInOrder() throws BrokenBarrierException, InterruptedException {
        int numberOfThreads = randomIntBetween(2, 8);
        TaskExecutor[] executors = new TaskExecutor[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            executors[i] = new TaskExecutor();
        }

        int tasksSubmittedPerThread = randomIntBetween(2, 1024);

        CountDownLatch updateLatch = new CountDownLatch(numberOfThreads * tasksSubmittedPerThread);

        final TestListener listener = new TestListener() {
            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void processed() {
                updateLatch.countDown();
            }
        };

        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            final int index = i;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < tasksSubmittedPerThread; j++) {
                        submitTask(
                            "[" + index + "][" + j + "]",
                            j,
                            ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                            executors[index],
                            listener
                        );
                    }
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        // wait for all threads to be ready
        barrier.await();
        // wait for all threads to finish
        barrier.await();

        assertTrue(updateLatch.await(10, TimeUnit.SECONDS));

        for (int i = 0; i < numberOfThreads; i++) {
            assertEquals(tasksSubmittedPerThread, executors[i].tasks.size());
            for (int j = 0; j < tasksSubmittedPerThread; j++) {
                assertNotNull(executors[i].tasks.get(j));
                assertEquals("cluster state update task executed out of order", j, (int) executors[i].tasks.get(j));
            }
        }
    }

    public void testSingleTaskSubmission() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final Integer task = randomInt(1024);
        TestExecutor<Integer> executor = taskList -> {
            assertThat(taskList.size(), equalTo(1));
            assertThat(taskList.get(0), equalTo(task));
        };
        submitTask("test", task, ClusterStateTaskConfig.build(randomFrom(Priority.values())), executor, new TestListener() {
            @Override
            public void processed() {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        latch.await();
    }

    public void testDuplicateSubmission() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        try (BlockingTask blockingTask = new BlockingTask(Priority.IMMEDIATE)) {
            submitTask("blocking", blockingTask);

            TestExecutor<SimpleTask> executor = tasks -> {};
            SimpleTask task = new SimpleTask(1);
            TestListener listener = new TestListener() {
                @Override
                public void processed() {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            };

            submitTask("first time", task, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);

            final AssertionError e = expectThrows(
                AssertionError.class,
                () -> submitTask("second time", task, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener)
            );
            assertThat(e, hasToString(containsString("task [1] with source [second time] is already queued")));

            submitTask("third time a charm", new SimpleTask(1), ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);

            assertThat(latch.getCount(), equalTo(2L));
        }
        latch.await();
    }

    private static class SimpleTask {
        private final int id;

        private SimpleTask(int id) {
            this.id = id;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        @Override
        public String toString() {
            return Integer.toString(id);
        }
    }

}
