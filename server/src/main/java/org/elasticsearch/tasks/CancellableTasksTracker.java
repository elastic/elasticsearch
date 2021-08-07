/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.stream.Stream;

/**
 * Tracks items that are associated with cancellable tasks, supporting efficient lookup by task ID and by parent task ID
 */
public class CancellableTasksTracker<T> {

    private final IntFunction<T[]> arrayConstructor;

    public CancellableTasksTracker(Class<T> clazz, IntFunction<T[]> arrayConstructor) {
        assert clazz.isArray() == false;
        this.arrayConstructor = arrayConstructor;
    }

    private final Map<Long, T> byTaskId = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // If the parent task has just one child (the common case) then we just store the value, otherwise we hold them in an array.
    private final Map<TaskId, Object> byParentTaskId = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    /**
     * Add an item for the given task. Should only be called once for each task.
     */
    public void put(Task task, T item) {
        final long taskId = task.getId();
        if (task.getParentTaskId().isSet()) {
            byParentTaskId.compute(task.getParentTaskId(), (ignored, oldValue) -> {
                if (oldValue == null) {
                    return item;
                } else if (oldValue.getClass().isArray()) {
                    //noinspection unchecked
                    final T[] oldArray = (T[])oldValue;
                    final int oldLength = oldArray.length;
                    assert oldLength > 1;
                    final T[] newArray = Arrays.copyOf(oldArray, oldLength + 1);
                    newArray[oldLength] = item;
                    return newArray;
                } else {
                    final T[] newArray = arrayConstructor.apply(2);
                    //noinspection unchecked
                    newArray[0] = (T)oldValue;
                    newArray[1] = item;
                    return newArray;
                }
            });
        }
        final T oldItem = byTaskId.put(taskId, item);
        assert oldItem == null : "duplicate entry for task [" + taskId + "]";
    }

    /**
     * Get the item that corresponds with the given task, or {@code null} if there is no such item.
     */
    public T get(long id) {
        return byTaskId.get(id);
    }

    /**
     * Remove (and return) the item that corresponds with the given task. Return {@code null} if not present. Safe to call multiple times
     * for each task. However, {@link #getByParent} may return this task even after a call to this method completes, if the removal is
     * actually being completed by a concurrent call that's still ongoing.
     */
    public T remove(Task task) {
        final long taskId = task.getId();
        final T oldItem = byTaskId.remove(taskId);
        if (oldItem != null && task.getParentTaskId().isSet()) {
            byParentTaskId.compute(task.getParentTaskId(), (ignored, oldValue) -> {

                if (oldValue == null || oldValue == oldItem) {
                    return null;
                }

                if (oldValue.getClass().isArray()) {
                    //noinspection unchecked
                    final T[] oldArray = (T[]) oldValue;
                    final int oldLength = oldArray.length;
                    assert oldLength > 1;
                    for (int i = 0; i < oldLength; i++) {
                        if (oldArray[i] == oldItem) {
                            if (oldLength == 2) {
                                return oldArray[1 - i];
                            } else {
                                final T[] newValue = arrayConstructor.apply(oldLength - 1);
                                System.arraycopy(oldArray, 0, newValue, 0, i);
                                System.arraycopy(oldArray, i + 1, newValue, i, oldLength - i - 1);
                                return newValue;
                            }
                        }
                    }
                }

                return oldValue;
            });
        }
        return oldItem;
    }

    /**
     * Return a collection of all the tracked items. May be large. In the presence of concurrent calls to {@link #put} and {@link #remove}
     * it behaves similarly to {@link ConcurrentHashMap#values()}.
     */
    public Iterable<T> values() {
        return byTaskId.values();
    }

    /**
     * Return a collection of all the tracked items with a given parent, which will include at least every item for which {@link #put}
     * completed, but {@link #remove} hasn't started. May include some additional items for which all the calls to {@link #remove} that
     * started before this method was called have not completed.
     */
    public Stream<T> getByParent(TaskId parentTaskId) {
        final Object byParent = byParentTaskId.get(parentTaskId);
        if (byParent == null) {
            return Stream.empty();
        }
        if (byParent.getClass().isArray()) {
            //noinspection unchecked
            return Arrays.stream((T[])byParent);
        } else {
            //noinspection unchecked
            return Stream.of((T)byParent);
        }
    }
}
