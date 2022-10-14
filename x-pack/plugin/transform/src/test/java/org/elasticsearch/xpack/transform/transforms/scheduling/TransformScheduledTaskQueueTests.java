/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler.Event;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TransformScheduledTaskQueueTests extends ESTestCase {

    private final TransformScheduledTaskQueue queue = new TransformScheduledTaskQueue();
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
        queue.add(createTask("task-1", 5));
        assertThat(queue.first(), is(notNullValue()));
    }

    public void testAddAndRemove() {
        queue.add(createTask("task-1", 5));
        queue.add(createTask("task-2", 1));
        queue.add(createTask("task-3", 9));
        assertThat(queue.first(), is(notNullValue()));
        assertThat(queue.getTransformIds(), containsInAnyOrder("task-1", "task-2", "task-3"));
        assertThat(queue.first(), is(equalTo(createTask("task-2", 1))));

        queue.remove("task-1");
        queue.remove("task-2");
        queue.remove("task-3");
        assertThatQueueIsEmpty();
    }

    public void testConcurrentAddAndRemove() throws Exception {
        {
            List<Callable<Boolean>> concurrentQueueAddTasks = new ArrayList<>(100);
            for (int i = 0; i < 100; ++i) {
                TransformScheduledTask task = createTask("task-" + i, randomLong());
                concurrentQueueAddTasks.add(() -> queue.add(task));
            }
            List<Future<Boolean>> futures = threadPool.generic().invokeAll(concurrentQueueAddTasks);
            for (Future<Boolean> future : futures) {
                Boolean taskAdded = future.get();
                // Verify that the added task ids were unique
                assertThat(taskAdded, is(true));
            }
        }
        assertThat(queue.first(), is(notNullValue()));
        assertThat(queue.getTransformIds(), hasSize(100));

        {
            Set<String> removedTaskIds = new HashSet<>();
            List<Callable<TransformScheduledTask>> concurrentQueueRemoveTasks = new ArrayList<>(100);
            for (int i = 0; i < 100; ++i) {
                String taskId = "task-" + i;
                concurrentQueueRemoveTasks.add(() -> queue.remove(taskId));
            }
            List<Future<TransformScheduledTask>> futures = threadPool.generic().invokeAll(concurrentQueueRemoveTasks);
            for (Future<TransformScheduledTask> future : futures) {
                removedTaskIds.add(future.get().getTransformId());
            }
            // Verify that the removed tasks ids were unique
            assertThat(removedTaskIds, hasSize(100));
        }
        assertThatQueueIsEmpty();
    }

    public void testAddNoOp() {
        queue.add(createTask("task-1", 5));
        assertThat(queue.first(), is(equalTo(createTask("task-1", 5))));

        // Try adding a task with a duplicate key
        queue.add(createTask("task-1", 6));
        // Verify that the add operation had no effect
        assertThat(queue.first(), is(equalTo(createTask("task-1", 5))));
    }

    public void testRemoveNoOp() {
        queue.add(createTask("task-1", 5));
        queue.remove("task-non-existent");
        // Verify that the remove operation had no effect
        assertThat(queue.first(), is(notNullValue()));
        assertThat(queue.getTransformIds(), containsInAnyOrder("task-1"));
        assertThat(queue.first(), is(equalTo(createTask("task-1", 5))));
    }

    public void testUpdateNoOp() {
        queue.add(createTask("task-1", 5));
        queue.update("task-non-existent", task -> createTask(task.getTransformId(), -999));
        // Verify that the update operation had no effect
        assertThat(queue.first(), is(notNullValue()));
        assertThat(queue.getTransformIds(), containsInAnyOrder("task-1"));
        assertThat(queue.first(), is(equalTo(createTask("task-1", 5))));
    }

    public void testUpdateModifiesId() {
        queue.add(createTask("task-id", 5));
        Exception e = expectThrows(IllegalStateException.class, () -> queue.update("task-id", task -> createTask("other-id", 5)));
        assertThat(e.getMessage(), is(equalTo("Must not modify the transform's id during update")));
    }

    public void testRemoveAll() {
        queue.add(createTask("task-1", 5));
        queue.add(createTask("task-2", 1));
        queue.add(createTask("task-3", 9));
        queue.add(createTask("task-4", 3));
        queue.add(createTask("task-5", 7));
        queue.add(createTask("task-6", 6));
        queue.add(createTask("task-7", 0));
        queue.add(createTask("task-8", 2));
        queue.add(createTask("task-9", 4));
        assertThat(queue.first(), is(notNullValue()));
        assertThat(
            queue.getTransformIds(),
            containsInAnyOrder("task-1", "task-2", "task-3", "task-4", "task-5", "task-6", "task-7", "task-8", "task-9")
        );
        assertThat(queue.first(), is(equalTo(createTask("task-7", 0))));

        List<TransformScheduledTask> tasksByPriority = new ArrayList<>();
        while (queue.first() != null) {
            TransformScheduledTask task = queue.first();
            tasksByPriority.add(task);
            queue.remove(task.getTransformId());
        }
        assertThatQueueIsEmpty();
        assertThat(
            tasksByPriority,
            Matchers.contains(
                createTask("task-7", 0),
                createTask("task-2", 1),
                createTask("task-8", 2),
                createTask("task-4", 3),
                createTask("task-9", 4),
                createTask("task-1", 5),
                createTask("task-6", 6),
                createTask("task-5", 7),
                createTask("task-3", 9)
            )
        );
    }

    public void testUpdatePriority() {
        queue.add(createTask("task-1", 5));
        queue.add(createTask("task-2", 1));
        queue.add(createTask("task-3", 9));
        assertThat(queue.getTransformIds(), containsInAnyOrder("task-1", "task-2", "task-3"));
        assertThat(queue.first(), is(equalTo(createTask("task-2", 1))));

        queue.update("task-3", task -> createTask(task.getTransformId(), -999));
        assertThat(queue.getTransformIds(), containsInAnyOrder("task-1", "task-2", "task-3"));
        assertThat(queue.first(), is(equalTo(createTask("task-3", -999))));

        queue.update("task-1", task -> createTask(task.getTransformId(), 0));
        queue.remove("task-3");
        assertThat(queue.getTransformIds(), containsInAnyOrder("task-1", "task-2"));
        assertThat(queue.first(), is(equalTo(createTask("task-1", 0))));
    }

    private static TransformScheduledTask createTask(String transformId, long nextScheduledTimeMillis) {
        return new TransformScheduledTask(
            transformId,
            null,
            null,
            0,
            nextScheduledTimeMillis,
            TransformScheduledTaskQueueTests::failUnexpectedCall
        );
    }

    private static void failUnexpectedCall(Event event) {
        fail("Unexpected call to listener: " + event);
    }

    private void assertThatQueueIsEmpty() {
        assertThat(queue.first(), is(nullValue()));
        assertThat(queue.getTransformIds(), is(empty()));
    }
}
