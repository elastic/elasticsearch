/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Map;

/**
 * A task that can be cancelled
 */
public class CancellableTask extends Task {

    private static final VarHandle REASON_HANDLE;

    static {
        try {
            REASON_HANDLE = MethodHandles.lookup().findVarHandle(CancellableTask.class, "reason", String.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") // updated via REASON_HANDLE
    private volatile String reason;
    private final SubscribableListener<Void> listeners = new SubscribableListener<>();

    public CancellableTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    /**
     * This method is called by the task manager when this task is cancelled.
     */
    final void cancel(String reason) {
        assert reason != null;
        if (REASON_HANDLE.compareAndSet(this, null, reason) == false) {
            return;
        }
        listeners.onResponse(null);
        onCancelled();
    }

    /**
     * Returns whether this task's children need to be cancelled too. {@code true} is a reasonable response even for tasks that have no
     * children, since child tasks might be added in future and it'd be easy to forget to update this, but returning {@code false} saves
     * a bit of computation in the task manager.
     */
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    /**
     * Return whether the task is cancelled. If testing this flag to decide whether to throw a {@link TaskCancelledException}, consider
     * using {@link #ensureNotCancelled} or {@link #notifyIfCancelled} instead: these methods construct an exception that automatically
     * includes the cancellation reason.
     */
    public final boolean isCancelled() {
        return reason != null;
    }

    /**
     * The reason the task was cancelled or null if it hasn't been cancelled.
     */
    @Nullable
    public final String getReasonCancelled() {
        return reason;
    }

    /**
     * This method adds a listener that needs to be notified if this task is cancelled.
     */
    public final void addListener(CancellationListener listener) {
        listeners.addListener(new CancellationListenerAdapter(listener));
    }

    /**
     * Called after the task is cancelled so that it can take any actions that it has to take.
     */
    protected void onCancelled() {}

    /**
     * Throws a {@link TaskCancelledException} if this task has been cancelled, otherwise does nothing.
     */
    public final void ensureNotCancelled() {
        if (isCancelled()) {
            throw getTaskCancelledException();
        }
    }

    /**
     * Notifies the listener of failure with a {@link TaskCancelledException} if this task has been cancelled, otherwise does nothing.
     * @return {@code true} if the task is cancelled and the listener was notified, otherwise {@code false}.
     */
    public final <T> boolean notifyIfCancelled(ActionListener<T> listener) {
        if (isCancelled() == false) {
            return false;
        }
        listener.onFailure(getTaskCancelledException());
        return true;
    }

    @Override
    public String toString() {
        String cancelReason = reason;
        return "CancellableTask{" + super.toString() + ", reason='" + cancelReason + '\'' + ", isCancelled=" + (cancelReason != null) + '}';
    }

    private TaskCancelledException getTaskCancelledException() {
        assert reason != null;
        return new TaskCancelledException("task cancelled [" + reason + ']');
    }

    /**
     * This interface is implemented by any class that needs to react to the cancellation of this task.
     */
    public interface CancellationListener {
        void onCancelled();
    }

    private record CancellationListenerAdapter(CancellationListener cancellationListener) implements ActionListener<Void> {
        @Override
        public void onResponse(Void unused) {
            cancellationListener.onCancelled();
        }

        @Override
        public void onFailure(Exception e) {
            assert false : e;
        }
    }
}
