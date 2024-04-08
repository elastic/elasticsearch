/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Tracks items that are associated with cancellable tasks, supporting efficient lookup by task ID and by parent task ID
 */
public class CancellableTasksTracker<T> {

    public CancellableTasksTracker() {}

    private final Map<Long, T> byTaskId = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final Map<TaskId, Map<Long, T[]>> byParentTaskId = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    /**
     * Gets the cancellable children of a parent task.
     *
     * Note: children of non-positive request IDs (e.g., -1) may be grouped together.
     */
    public Stream<T> getChildrenByRequestId(TaskId parentTaskId, long childRequestId) {
        Map<Long, T[]> byRequestId = byParentTaskId.get(parentTaskId);
        if (byRequestId != null) {
            T[] children = byRequestId.get(childRequestId);
            if (children != null) {
                return Arrays.stream(children);
            }
        }
        return Stream.empty();
    }

    /**
     * Add an item for the given task. Should only be called once for each task, and {@code item} must be unique per task too.
     */
    @SuppressWarnings("unchecked")
    public void put(Task task, long requestId, T item) {
        final long taskId = task.getId();
        if (task.getParentTaskId().isSet()) {
            byParentTaskId.compute(task.getParentTaskId(), (taskKey, oldRequestIdMap) -> {
                if (oldRequestIdMap == null) {
                    oldRequestIdMap = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
                }

                oldRequestIdMap.compute(requestId, (requestIdKey, oldValue) -> {
                    if (oldValue == null) {
                        final T[] newValue = (T[]) Array.newInstance(item.getClass(), 1);
                        newValue[0] = item;
                        return newValue;
                    } else {
                        return ArrayUtils.append(oldValue, item);
                    }
                });

                return oldRequestIdMap;
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
     * Remove (and return) the item that corresponds with the given task and request ID. Return {@code null} if not present. Safe to call
     * multiple times for each task. However, {@link #getByParent} may return this task even after a call to this method completes, if
     * the removal is actually being completed by a concurrent call that's still ongoing.
     */
    public T remove(Task task) {
        final long taskId = task.getId();
        final T oldItem = byTaskId.remove(taskId);
        if (oldItem != null && task.getParentTaskId().isSet()) {
            byParentTaskId.compute(task.getParentTaskId(), (taskKey, oldRequestIdMap) -> {
                if (oldRequestIdMap == null) {
                    return null;
                }

                for (Long requestId : oldRequestIdMap.keySet()) {
                    oldRequestIdMap.compute(requestId, (requestIdKey, oldValue) -> {
                        if (oldValue == null) {
                            return null;
                        }
                        if (oldValue.length == 1) {
                            if (oldValue[0] == oldItem) {
                                return null;
                            } else {
                                return oldValue;
                            }
                        }
                        if (oldValue[0] == oldItem) {
                            return Arrays.copyOfRange(oldValue, 1, oldValue.length);
                        }
                        for (int i = 1; i < oldValue.length; i++) {
                            if (oldValue[i] == oldItem) {
                                final T[] newValue = Arrays.copyOf(oldValue, oldValue.length - 1);
                                System.arraycopy(oldValue, i + 1, newValue, i, oldValue.length - i - 1);
                                return newValue;
                            }
                        }
                        return oldValue;
                    });
                }

                if (oldRequestIdMap.keySet().isEmpty()) {
                    return null;
                }

                return oldRequestIdMap;
            });
        }
        return oldItem;
    }

    /**
     * Return a collection of all the tracked items. May be large. In the presence of concurrent calls to {@link #put} and {@link #remove}
     * it behaves similarly to {@link ConcurrentHashMap#values()}.
     */
    public Collection<T> values() {
        return byTaskId.values();
    }

    /**
     * Return a collection of all the tracked items with a given parent, which will include at least every item for which {@link #put}
     * completed, but {@link #remove} hasn't started. May include some additional items for which all the calls to {@link #remove} that
     * started before this method was called have not completed.
     */
    public Stream<T> getByParent(TaskId parentTaskId) {
        final Map<Long, T[]> byParent = byParentTaskId.get(parentTaskId);
        if (byParent == null) {
            return Stream.empty();
        }
        return byParent.values().stream().flatMap(Stream::of);
    }

    // assertion for tests, not an invariant but should eventually be true
    boolean assertConsistent() {
        // mustn't leak any items tracked by parent
        assert byTaskId.isEmpty() == false || byParentTaskId.isEmpty();

        // every by-parent value must be tracked by task too; the converse isn't true since we don't track values without a parent
        final Set<T> byTaskValues = new HashSet<>(byTaskId.values());
        byParentTaskId.values().forEach(byParentMap -> {
            byParentMap.forEach((requestId, byParentArray) -> {
                assert byParentArray.length > 0;
                for (T t : byParentArray) {
                    assert byTaskValues.contains(t);
                }
            });
        });

        return true;
    }
}
