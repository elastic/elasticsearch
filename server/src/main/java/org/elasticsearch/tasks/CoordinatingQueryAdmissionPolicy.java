/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Limits how many coordinating queries may be in flight on this node.
 * Coordinating actions ({@code _search}, ES|QL) call {@link #admit} with their task;
 * the policy registers a {@link RemovedTaskListener} on the {@link TaskManager} it is
 * constructed with, and releases the slot when the task is unregistered.
 *
 * <p>A task with a local ancestor that was already admitted is part of a query already
 * accepted on this node: it still takes a slot in {@link #current()} but is not rejected.
 * The walk covers {@code _search} → {@code _msearch} → {@code _search} from compound retrievers.
 * New queries (no such ancestor) are rejected when the limit is reached.
 */
public final class CoordinatingQueryAdmissionPolicy {

    /**
     * Admits every task and never counts. Installed when no
     * {@link CoordinatingQueryAdmissionPolicyProvider} is present, and used by tests that
     * construct coordinating actions without a real policy.
     */
    public static final CoordinatingQueryAdmissionPolicy NOOP = new CoordinatingQueryAdmissionPolicy(0, false);

    private final Set<Long> counted = ConcurrentHashMap.newKeySet();
    private final AtomicInteger current = new AtomicInteger();
    private final AtomicInteger rejected = new AtomicInteger();
    private final int max;
    private final boolean enabled;
    private final TaskManager taskManager;

    /**
     * @param max         maximum in-flight coordinating queries; must be non-negative
     * @param taskManager used to walk local parents and to release slots when tasks unregister
     */
    public CoordinatingQueryAdmissionPolicy(int max, TaskManager taskManager) {
        this(max, true, Objects.requireNonNull(taskManager, "taskManager"));
    }

    private CoordinatingQueryAdmissionPolicy(int max, boolean enabled) {
        this(max, enabled, null);
    }

    private CoordinatingQueryAdmissionPolicy(int max, boolean enabled, TaskManager taskManager) {
        if (max < 0) {
            throw new IllegalArgumentException("max must be non-negative");
        }
        this.max = max;
        this.enabled = enabled;
        this.taskManager = taskManager;
        if (enabled) {
            taskManager.registerRemovedTaskListener(this::onRemoved);
        }
    }

    /**
     * Takes a slot for {@code task}, or rejects if the limit is reached.
     * If a local ancestor was already admitted, this request is part of that query: it is counted
     * and not rejected.
     *
     * Example: a single search request will be counted and admitted, hence potentially rejected depending on the policy.
     * A multi search request will have each of its individual search requests counted and admitted, hence potentially
     * rejected. A rejection will fail only that specific item and not the entire multi search request.
     * A composite search request, which uses retrievers, will have additional sub-search requests coordinated by the same node.
     * Given the parent search has already been admitted, such sub-searches are counted but never rejected.
     * A search request triggered by a reindex / update by query will be counted and admitted, hence potentially rejected.
     */
    public void admit(Task task) {
        if (enabled == false) {
            return;
        }
        int inFlight = current.incrementAndGet();
        boolean ancestorAdmitted = hasAdmittedLocalAncestor(task);
        if (ancestorAdmitted == false && inFlight > max) {
            current.decrementAndGet();
            // TODO here we could introduce a queue to delay rejecting, with inner-searches taking precedence over new queries
            rejected.incrementAndGet();
            throw new EsRejectedExecutionException(
                "rejected execution of action ["
                    + task.getAction()
                    + "]; too many in-flight requests ["
                    + inFlight
                    + "], limit ["
                    + max
                    + "]",
                false
            );
        }
        counted.add(task.getId());
    }

    /**
     * Walks local parents until one is in {@link #counted}, the chain leaves this node,
     * or a parent task is no longer registered.
     */
    private boolean hasAdmittedLocalAncestor(Task task) {
        String localNodeId = taskManager.getNodeId();
        TaskId parentId = task.getParentTaskId();
        Set<Long> seen = new HashSet<>();
        while (parentId.isSet() && localNodeId.equals(parentId.getNodeId()) && seen.add(parentId.getId())) {
            if (counted.contains(parentId.getId())) {
                return true;
            }
            Task parent = taskManager.getTask(parentId.getId());
            if (parent == null) {
                return false;
            }
            parentId = parent.getParentTaskId();
        }
        return false;
    }

    /**
     * Releases the slot taken by {@link #admit} if this task was counted.
     */
    public void onRemoved(Task task) {
        if (counted.remove(task.getId())) {
            int remaining = current.decrementAndGet();
            assert remaining >= 0 : remaining;
        }
    }

    /**
     * Operations currently in flight.
     */
    public int current() {
        return current.get();
    }

    /**
     * Coordinating queries rejected because the limit was already reached.
     */
    public int rejected() {
        return rejected.get();
    }
}
