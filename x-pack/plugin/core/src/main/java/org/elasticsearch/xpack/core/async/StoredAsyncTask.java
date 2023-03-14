/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class StoredAsyncTask<Response extends ActionResponse> extends CancellableTask implements AsyncTask {

    private final AsyncExecutionId asyncExecutionId;
    private final Map<String, String> originHeaders;
    private volatile long expirationTimeMillis;
    private final List<ActionListener<Response>> completionListeners;

    public StoredAsyncTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        Map<String, String> originHeaders,
        AsyncExecutionId asyncExecutionId,
        TimeValue keepAlive
    ) {
        super(id, type, action, description, parentTaskId, headers);
        this.asyncExecutionId = asyncExecutionId;
        this.originHeaders = originHeaders;
        this.expirationTimeMillis = getStartTime() + keepAlive.getMillis();
        this.completionListeners = new ArrayList<>();
    }

    @Override
    public Map<String, String> getOriginHeaders() {
        return originHeaders;
    }

    @Override
    public AsyncExecutionId getExecutionId() {
        return asyncExecutionId;
    }

    /**
     * Update the expiration time of the (partial) response.
     */
    @Override
    public void setExpirationTime(long expirationTime) {
        this.expirationTimeMillis = expirationTime;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    public synchronized void addCompletionListener(ActionListener<Response> listener) {
        completionListeners.add(listener);
    }

    public synchronized void removeCompletionListener(ActionListener<Response> listener) {
        completionListeners.remove(listener);
    }

    /**
     * This method is called when the task is finished successfully before unregistering the task and storing the results
     */
    public synchronized void onResponse(Response response) {
        for (ActionListener<Response> listener : completionListeners) {
            listener.onResponse(response);
        }
    }

    /**
     * This method is called when the task failed before unregistering the task and storing the results
     */
    public synchronized void onFailure(Exception e) {
        for (ActionListener<Response> listener : completionListeners) {
            listener.onFailure(e);
        }
    }

    /**
     * Return currently available partial or the final results
     */
    public abstract Response getCurrentResult();

    @Override
    public void cancelTask(TaskManager taskManager, Runnable runnable, String reason) {
        taskManager.cancelTaskAndDescendants(this, reason, true, ActionListener.running(runnable));
    }
}
