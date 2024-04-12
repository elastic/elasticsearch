/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

class TransformRetryableStartUpListener<Response> implements TransformScheduler.Listener {
    private final String transformId;
    private final Consumer<ActionListener<Response>> action;
    private final ActionListener<Response> actionListener;
    private final ActionListener<Boolean> retryScheduledListener;
    private final Supplier<Boolean> shouldRetry;
    private final TransformContext context;
    private final AtomicBoolean isFirstRun;
    private final AtomicBoolean shouldRunAction;

    /**
     * @param transformId the transform associated with this listener. All events to this listener must be for the same transformId.
     * @param action the action this listener will take. When the TransformScheduler invokes {@link #triggered(TransformScheduler.Event)},
     *               the call is forwarded to this action.
     * @param actionListener actionListener will be notified via #onResponse when the action succeeds or via #onFailure when retries have
     *                       stopped. If the Transform Stop API deregisters this class from the Scheduler, this actionListener will *not* be
     *                       invoked.
     * @param retryScheduledListener retryScheduledListener will be notified after the first call. If true, another thread has started the
     *                               retry process. If false, the original call was successful, and no retries will happen.
     * @param shouldRetry allows an external entity to gracefully stop these retries, invoking the actionListener's #onFailure method.
     *                    Note that external entities are still required to deregister this listener from the Scheduler.
     * @param context the transform's context object. This listener will update the StartUpFailureCount information in the context as it
     *                encounters errors and retries.
     */
    TransformRetryableStartUpListener(
        String transformId,
        Consumer<ActionListener<Response>> action,
        ActionListener<Response> actionListener,
        ActionListener<Boolean> retryScheduledListener,
        Supplier<Boolean> shouldRetry,
        TransformContext context
    ) {
        this.transformId = transformId;
        this.action = action;
        this.actionListener = actionListener;
        this.retryScheduledListener = retryScheduledListener;
        this.shouldRetry = shouldRetry;
        this.context = context;
        this.isFirstRun = new AtomicBoolean(true);
        this.shouldRunAction = new AtomicBoolean(true);
    }

    @Override
    public void triggered(TransformScheduler.Event event) {
        if (transformId.equals(event.transformId()) && shouldRunAction.compareAndSet(true, false)) {
            action.accept(ActionListener.wrap(this::actionSucceeded, this::actionFailed));
        }
    }

    private void actionSucceeded(Response r) {
        maybeNotifyRetryListener(false);
        markDone();
        actionListener.onResponse(r);
    }

    private void markDone() {
        synchronized (context) {
            context.resetStartUpFailureCount();
        }
    }

    private void maybeNotifyRetryListener(boolean response) {
        if (isFirstRun.compareAndSet(true, false)) {
            retryScheduledListener.onResponse(response);
        }
    }

    private void actionFailed(Exception e) {
        if (shouldRetry.get()) {
            maybeNotifyRetryListener(true);
            recordError(e);
            shouldRunAction.set(true);
        } else {
            maybeNotifyRetryListener(false);
            markDone();
            actionListener.onFailure(e);
        }
    }

    private void recordError(Exception e) {
        synchronized (context) {
            context.incrementAndGetStartUpFailureCount(e);
        }
    }
}
