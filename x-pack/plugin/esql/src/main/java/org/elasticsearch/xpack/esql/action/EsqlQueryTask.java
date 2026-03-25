/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public abstract class EsqlQueryTask extends StoredAsyncTask<EsqlQueryResponse> {

    private EsqlExecutionInfo executionInfo;
    private final AtomicReference<Scheduler.ScheduledCancellable> scheduledCancellation = new AtomicReference<>();

    public EsqlQueryTask(
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
        super(id, type, action, description, parentTaskId, headers, originHeaders, asyncExecutionId, keepAlive);
        this.executionInfo = null;
    }

    public void setExecutionInfo(EsqlExecutionInfo executionInfo) {
        this.executionInfo = executionInfo;
    }

    public EsqlExecutionInfo executionInfo() {
        return executionInfo;
    }

    @Override
    public EsqlQueryResponse getCurrentResult() {
        // TODO it'd be nice to have the number of documents we've read from completed drivers here
        return new EsqlQueryResponse(
            List.of(),
            List.of(),
            0,
            0,
            null,
            false,
            getExecutionId().getEncoded(),
            true,
            true,
            getStartTime(),
            getExpirationTimeMillis(),
            executionInfo
        );
    }

    @Override
    public void onResponse(EsqlQueryResponse response) {
        removeScheduledCancellation();
        super.onResponse(response);
    }

    @Override
    public void onFailure(Exception e) {
        removeScheduledCancellation();
        super.onFailure(e);
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        super.setExpirationTime(expirationTime);
        rescheduleCancellationOnExpiry();
    }

    private void removeScheduledCancellation() {
        var prev = scheduledCancellation.getAndSet(null);
        if (prev != null) {
            prev.cancel();
        }
    }

    /**
     * Schedules task cancellation at the given expiration time
     */
    protected abstract Scheduler.ScheduledCancellable scheduleCancellationOnExpiry(long expirationTimeMillis);

    public void rescheduleCancellationOnExpiry() {
        var prev = scheduledCancellation.getAndSet(scheduleCancellationOnExpiry(getExpirationTimeMillis()));
        if (prev != null) {
            prev.cancel();
        }
    }
}
