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
 * Lets a task submitted to a {@link TaskExecutionTimeTrackingEsThreadPoolExecutor} be told how long it waited in the queue. It must be
 * implemented by the runnable passed to {@code execute}, since the executor does not look inside wrappers. Only pools that track
 * execution time call it, and an exception it throws is logged and ignored.
 */
public interface QueueTimeAware {

    /**
     * Called once on the executing thread just before the task runs, never for a rejected task.
     *
     * @param queueTimeMicros how long the task waited in the queue, in microseconds
     */
    void onQueueTime(long queueTimeMicros);
}
