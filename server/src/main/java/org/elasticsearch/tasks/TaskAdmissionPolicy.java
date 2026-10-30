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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Limits how many tasks for a configured set of actions may be in flight.
 * {@link TaskManager} calls {@link #admit} before creating a task and {@link #onRemoved} when it is unregistered.
 */
public final class TaskAdmissionPolicy {

    /**
     * Admits every action and never counts. Installed when no {@link TaskAdmissionPolicyProvider}
     * is present, and used by tests that construct a {@link TaskManager} without a real policy.
     */
    public static final TaskAdmissionPolicy NOOP = new TaskAdmissionPolicy(Set.of(), 0);

    private final Set<String> actions;
    private final AtomicInteger current = new AtomicInteger();
    private final int max;

    /**
     * @param max maximum in-flight tracked actions; must be non-negative
     */
    public TaskAdmissionPolicy(Collection<String> actions, int max) {
        if (max < 0) {
            throw new IllegalArgumentException("max must be non-negative");
        }
        this.actions = Set.copyOf(actions);
        this.max = max;
    }

    /**
     * Takes a slot for a configured action, or rejects if the limit is reached.
     */
    public void admit(String action) {
        if (trackAction(action)) {
            int inFlight = current.incrementAndGet();
            if (inFlight > max) {
                current.decrementAndGet();
                // TODO here we could potentially introduce a queue to delay rejecting, and leave some headroom
                throw new EsRejectedExecutionException(
                    "rejected execution of action [" + action + "]; too many in-flight requests [" + inFlight + "], limit [" + max + "]",
                    false
                );
            }
        }
    }

    /**
     * Releases the slot taken by {@link #admit} for a configured action.
     */
    public void onRemoved(Task task) {
        if (trackAction(task.getAction())) {
            int remaining = current.decrementAndGet();
            assert remaining >= 0 : remaining;
        }
    }

    private boolean trackAction(String action) {
        return actions.contains(action);
    }

    /**
     * Operations currently in flight.
     */
    public int current() {
        return current.get();
    }
}
