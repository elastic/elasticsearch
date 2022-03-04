/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ConcurrentPriorityQueueTests extends ESTestCase {

    private final ConcurrentPriorityQueue<TestTask> queue = new ConcurrentPriorityQueue<>(TestTask::getTaskId, TestTask::getPriority);
    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    public void testEmptyQueue() {
        assertThatQueueIsEmpty();
    }

    public void testNonEmptyQueue() {
        queue.add(new TestTask("task-1", 5));
        assertThat(queue.isEmpty(), is(false));
    }

    public void testAddAndRemove() {
        queue.add(new TestTask("task-1", 5));
        queue.add(new TestTask("task-2", 1));
        queue.add(new TestTask("task-3", 9));
        assertThat(queue.isEmpty(), is(false));
        assertThat(queue.getElementIds(), containsInAnyOrder("task-1", "task-2", "task-3"));
        assertThat(queue.first(), is(equalTo(new TestTask("task-2", 1))));

        queue.remove("task-1");
        queue.remove("task-2");
        queue.remove("task-3");
        assertThatQueueIsEmpty();
    }

    public void testConcurrentAddAndRemove() throws Exception {
        {
            List<Callable<Boolean>> concurrentQueueAddTasks = new ArrayList<>(100);
            for (int i = 0; i < 100; ++i) {
                TestTask task = new TestTask("task-" + i, randomLong());
                concurrentQueueAddTasks.add(() -> queue.add(task));
            }
            List<Future<Boolean>> futures = threadPool.generic().invokeAll(concurrentQueueAddTasks);
            for (Future<Boolean> future : futures) {
                Boolean taskAdded = future.get();
                // Verify that the added task ids were unique
                assertThat(taskAdded, is(true));
            }
        }
        assertThat(queue.isEmpty(), is(false));
        assertThat(queue.getElementIds(), hasSize(100));

        {
            Set<String> removedTaskIds = new HashSet<>();
            List<Callable<TestTask>> concurrentQueueRemoveTasks = new ArrayList<>(100);
            for (int i = 0; i < 100; ++i) {
                String taskId = "task-" + i;
                concurrentQueueRemoveTasks.add(() -> queue.remove(taskId));
            }
            List<Future<TestTask>> futures = threadPool.generic().invokeAll(concurrentQueueRemoveTasks);
            for (Future<TestTask> future : futures) {
                removedTaskIds.add(future.get().getTaskId());
            }
            // Verify that the removed tasks ids were unique
            assertThat(removedTaskIds, hasSize(100));
        }
        assertThatQueueIsEmpty();
    }

    public void testAddNoOp() {
        queue.add(new TestTask("task-1", 5));
        assertThat(queue.first(), is(equalTo(new TestTask("task-1", 5))));

        // Try adding a task with a duplicate key
        queue.add(new TestTask("task-1", 6));
        // Verify that the add operation had no effect
        assertThat(queue.first(), is(equalTo(new TestTask("task-1", 5))));
    }

    public void testRemoveNoOp() {
        queue.add(new TestTask("task-1", 5));
        queue.remove("task-non-existent");
        // Verify that the remove operation had no effect
        assertThat(queue.isEmpty(), is(false));
        assertThat(queue.getElementIds(), containsInAnyOrder("task-1"));
        assertThat(queue.first(), is(equalTo(new TestTask("task-1", 5))));
    }

    public void testUpdateNoOp() {
        queue.add(new TestTask("task-1", 5));
        queue.update("task-non-existent", task -> new TestTask(task.taskId, -999));
        // Verify that the update operation had no effect
        assertThat(queue.isEmpty(), is(false));
        assertThat(queue.getElementIds(), containsInAnyOrder("task-1"));
        assertThat(queue.first(), is(equalTo(new TestTask("task-1", 5))));
    }

    public void testUpdateModifiesId() {
        queue.add(new TestTask("task-id", 5));
        Exception e = expectThrows(IllegalStateException.class, () -> queue.update("task-id", task -> new TestTask("other-id", 5)));
        assertThat(e.getMessage(), is(equalTo("Must not modify the element's id during update")));
    }

    public void testRemoveAll() {
        queue.add(new TestTask("task-1", 5));
        queue.add(new TestTask("task-2", 1));
        queue.add(new TestTask("task-3", 9));
        queue.add(new TestTask("task-4", 3));
        queue.add(new TestTask("task-5", 7));
        queue.add(new TestTask("task-6", 6));
        queue.add(new TestTask("task-7", 0));
        queue.add(new TestTask("task-8", 2));
        queue.add(new TestTask("task-9", 4));
        assertThat(queue.isEmpty(), is(false));
        assertThat(
            queue.getElementIds(),
            containsInAnyOrder("task-1", "task-2", "task-3", "task-4", "task-5", "task-6", "task-7", "task-8", "task-9")
        );
        assertThat(queue.first(), is(equalTo(new TestTask("task-7", 0))));

        List<TestTask> tasksByPriority = new ArrayList<>();
        while (queue.isEmpty() == false) {
            TestTask task = queue.first();
            tasksByPriority.add(task);
            queue.remove(task.getTaskId());
        }
        assertThatQueueIsEmpty();
        assertThat(
            tasksByPriority,
            contains(
                new TestTask("task-7", 0),
                new TestTask("task-2", 1),
                new TestTask("task-8", 2),
                new TestTask("task-4", 3),
                new TestTask("task-9", 4),
                new TestTask("task-1", 5),
                new TestTask("task-6", 6),
                new TestTask("task-5", 7),
                new TestTask("task-3", 9)
            )
        );
    }

    public void testUpdatePriority() {
        queue.add(new TestTask("task-1", 5));
        queue.add(new TestTask("task-2", 1));
        queue.add(new TestTask("task-3", 9));
        assertThat(queue.getElementIds(), containsInAnyOrder("task-1", "task-2", "task-3"));
        assertThat(queue.first(), is(equalTo(new TestTask("task-2", 1))));

        queue.update("task-3", task -> new TestTask(task.taskId, -999));
        assertThat(queue.getElementIds(), containsInAnyOrder("task-1", "task-2", "task-3"));
        assertThat(queue.first(), is(equalTo(new TestTask("task-3", -999))));

        queue.update("task-1", task -> new TestTask(task.taskId, 0));
        queue.remove("task-3");
        assertThat(queue.getElementIds(), containsInAnyOrder("task-1", "task-2"));
        assertThat(queue.first(), is(equalTo(new TestTask("task-1", 0))));
    }

    private static class TestTask {
        String taskId;
        long priority;

        TestTask(String taskId, long priority) {
            this.taskId = taskId;
            this.priority = priority;
        }

        String getTaskId() {
            return taskId;
        }

        long getPriority() {
            return priority;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            TestTask that = (TestTask) other;
            return Objects.equals(this.taskId, that.taskId) && this.priority == that.priority;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, priority);
        }

        @Override
        public String toString() {
            return new StringBuilder("TestTask[").append("taskId=")
                .append(taskId)
                .append(",priority=")
                .append(priority)
                .append("]")
                .toString();
        }
    }

    private void assertThatQueueIsEmpty() {
        assertThat(queue.isEmpty(), is(true));
        assertThat(queue.getElementIds(), is(empty()));
        expectThrows(NoSuchElementException.class, () -> queue.first());
    }
}
