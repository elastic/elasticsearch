/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.Nullable;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A task that can be canceled
 */
public abstract class CancellableTask extends Task {

    private volatile String reason;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public CancellableTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    /**
     * This method is called by the task manager when this task is cancelled.
     */
    final void cancel(String reason) {
        assert reason != null;
        if (cancelled.compareAndSet(false, true)) {
            this.reason = reason;
            onCancelled();
        }
    }

    /**
     * Returns true if this task should be automatically cancelled if the coordinating node that
     * requested this task left the cluster.
     */
    public boolean cancelOnParentLeaving() {
        return true;
    }

    /**
     * Returns true if this task can potentially have children that need to be cancelled when it parent is cancelled.
     */
    public abstract boolean shouldCancelChildrenOnCancellation();

    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * The reason the task was cancelled or null if it hasn't been cancelled.
     */
    @Nullable
    public final String getReasonCancelled() {
        return reason;
    }

    /**
     * Called after the task is cancelled so that it can take any actions that it has to take.
     */
    protected void onCancelled() {
    }
}
