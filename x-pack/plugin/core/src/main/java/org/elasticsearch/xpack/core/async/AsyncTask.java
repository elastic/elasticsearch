/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskManager;

import java.util.Map;

/**
 * A task that supports asynchronous execution and provides information necessary for safe temporary storage of results
 */
public interface AsyncTask {
    /**
     * Transport version that added {@code keep_alive} to async task status payloads.
     */
    TransportVersion ASYNC_TASK_KEEP_ALIVE_STATUS = TransportVersion.fromName("async_task_keep_alive_status");

    /**
     * Returns all of the request contexts headers
     */
    Map<String, String> getOriginHeaders();

    /**
     * Returns the {@link AsyncExecutionId} of the task
     */
    AsyncExecutionId getExecutionId();

    /**
     * Returns true if the task is cancelled
     */
    boolean isCancelled();

    /**
     * Update the expiration time of the (partial) response.
     */
    void setExpirationTime(long expirationTimeMillis, TimeValue keepAlive);

    /**
     * Returns the currently effective keep-alive for this task.
     */
    TimeValue getKeepAlive();

    /**
     * Performs necessary checks, cancels the task and calls the runnable upon completion
     */
    void cancelTask(TaskManager taskManager, Runnable runnable, String reason);
}
