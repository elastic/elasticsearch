/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.pytorch.PriorityProcessWorkerExecutorService;

import java.util.concurrent.atomic.AtomicBoolean;

public class ShutdownTracker {

    private final ActionListener<AcknowledgedResponse> everythingStoppedListener;

    private final Scheduler.Cancellable timeoutHandler;
    private final Runnable onWorkerQueueCompletedCallback;
    private final Runnable onTimeoutCallback;
    private final Object monitor = new Object();
    private final AtomicBoolean timedOutOrCompleted = new AtomicBoolean();

    private static final TimeValue COMPLETION_TIMEOUT = TimeValue.timeValueMinutes(5);

    public ShutdownTracker(
        Runnable onTimeoutCallback,
        Runnable onWorkerQueueCompletedCallback,
        ThreadPool threadPool,
        PriorityProcessWorkerExecutorService workerQueue,
        ActionListener<AcknowledgedResponse> everythingStoppedListener
    ) {
        this.onTimeoutCallback = onTimeoutCallback;
        this.onWorkerQueueCompletedCallback = onWorkerQueueCompletedCallback;
        this.everythingStoppedListener = ActionListener.notifyOnce(everythingStoppedListener);

        // initiate the worker shutdown and add this as a callback when completed
        workerQueue.shutdownWithCallback(this::workerQueueCompleted);
        // start the race with the timeout and the worker completing
        this.timeoutHandler = threadPool.schedule(
            this::onTimeout,
            COMPLETION_TIMEOUT,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
    }

    private void onTimeout() {
        synchronized (monitor) { // TODO remove the lock as the atomic should be sufficient
            if (timedOutOrCompleted.compareAndSet(false, true) == false) {
                // already completed
                return;
            }
            onTimeoutCallback.run();
            everythingStoppedListener.onResponse(AcknowledgedResponse.FALSE);
        }
    }

    private void workerQueueCompleted() {
        synchronized (monitor) { // TODO remove the lock as the atomic should be sufficient
            if (timedOutOrCompleted.compareAndSet(false, true) == false) {
                // already completed
                return;
            }
            timeoutHandler.cancel();
            onWorkerQueueCompletedCallback.run();
            everythingStoppedListener.onResponse(AcknowledgedResponse.TRUE);
        }
    }
}
