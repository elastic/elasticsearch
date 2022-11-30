/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * {@link TransformScheduledTaskQueue} class provides a priority queue functionality with the additional capability of quickly finding a
 * task by its transform's id.
 *
 * The implementation of this queue is thread-safe and utilizes locks.
 */
class TransformScheduledTaskQueue {

    private static final Logger logger = LogManager.getLogger(TransformScheduledTaskQueue.class);

    /** {@link SortedSet} allows us to quickly retrieve the task with the lowest priority. */
    private final SortedSet<TransformScheduledTask> tasks;
    /** {@link Map} allows us to quickly retrieve the task by the transform's id. */
    private final Map<String, TransformScheduledTask> tasksById;

    TransformScheduledTaskQueue() {
        this.tasks = new TreeSet<>(
            Comparator.comparing(TransformScheduledTask::getNextScheduledTimeMillis).thenComparing(TransformScheduledTask::getTransformId)
        );
        this.tasksById = new HashMap<>();
    }

    /**
     * @return the task with the *lowest* priority or null if the queue is empty.
     */
    public synchronized TransformScheduledTask first() {
        // gh#88991 concurrent access: the empty check must run within the synchronized context
        if (tasks.isEmpty()) {
            return null;
        }

        return tasks.first();
    }

    /**
     * Adds a task to the queue.
     *
     * @param task task to add
     * @return whether the task was added
     */
    public synchronized boolean add(TransformScheduledTask task) {
        String transformId = task.getTransformId();
        logger.trace("add({}): {}", transformId, task);
        if (tasksById.containsKey(transformId)) {
            logger.debug("add({}) is a no-op as the task for this transform already exists", transformId);
            return false;
        }
        tasksById.put(transformId, task);
        tasks.add(task);
        return true;
    }

    /**
     * Updates the task with the given transform id.
     *
     * @param transformId id of the transform to update
     * @param transformer function used to modify the task. Must not modify the transform id
     */
    public synchronized void update(String transformId, Function<TransformScheduledTask, TransformScheduledTask> transformer) {
        TransformScheduledTask task = remove(transformId);
        if (task == null) {
            return;
        }
        TransformScheduledTask updatedTask = transformer.apply(task);
        if (transformId.equals(updatedTask.getTransformId()) == false) {
            throw new IllegalStateException("Must not modify the transform's id during update");
        }
        add(updatedTask);
    }

    /**
     * Removes the task with the given transform id from the queue.
     *
     * @param transformId id of the transform to remove
     * @return the removed task or {@code null} if the task does not exist
     */
    public synchronized TransformScheduledTask remove(String transformId) {
        logger.trace("remove({})", transformId);
        TransformScheduledTask task = tasksById.remove(transformId);
        if (task == null) {
            logger.debug("remove({}) is a no-op as the task for this transform does not exist", transformId);
            return null;
        }
        tasks.remove(task);
        return task;
    }

    // Visible for testing
    /**
     * @return the set of all the transform ids
     *
     * Should not be used in production as it creates the new set every time.
     */
    synchronized Set<String> getTransformIds() {
        return Collections.unmodifiableSet(new HashSet<>(tasksById.keySet()));
    }

    // Visible for testing
    /**
     * @return queue current contents
     *
     * Should not be used in production as it creates the new set every time.
     */
    synchronized List<TransformScheduledTask> listScheduledTasks() {
        return tasks.stream().toList();
    }
}
