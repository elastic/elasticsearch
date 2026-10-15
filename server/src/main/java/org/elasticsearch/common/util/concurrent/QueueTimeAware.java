/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.util.concurrent;

/**
 * Implemented by a task submitted to a {@link TaskExecutionTimeTrackingEsThreadPoolExecutor} that wants to be told how long it waited in
 * the queue. This lets queue time be attributed to something the executor cannot see (for example an index) while keeping the executor
 * itself generic.
 * <p>
 * The executor only inspects the runnable that was passed to {@code execute}, so the implementation must be on that outermost runnable,
 * not on something it wraps. Executors that do not track execution time never call it.
 */
public interface QueueTimeAware {

    /**
     * Called once on the executing thread, just before the task runs. Must be cheap and must not throw. It is not called if the task is
     * rejected or otherwise never dequeued.
     *
     * @param queueTimeNanos time the task spent between creation and being handed to a thread
     */
    void onQueueTime(long queueTimeNanos);
}
