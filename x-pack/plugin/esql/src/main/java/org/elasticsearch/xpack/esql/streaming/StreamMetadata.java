/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record StreamMetadata(
    String streamId,
    TaskId taskId,
    Instant expiration,
    List<ColumnInfo> columns,
    Map<String, String> headers,
    List<String> warnings,
    Status status,
    @Nullable StreamCompletionInfo completionInfo
) {

    public enum Status {
        RUNNING,
        COMPLETE,
        FAILED,
        CANCELED
    }

    record StreamCompletionInfo(boolean partial, long tookMillis, Long pages, Long rows, @Nullable StreamFailure failure) {}

    record StreamFailure(String type, @Nullable String reason, RestStatus status) {

        static StreamFailure of(Exception exception) {
            Throwable cause = ExceptionsHelper.unwrapCause(exception);
            return new StreamFailure(ElasticsearchException.getExceptionName(cause), cause.getMessage(), ExceptionsHelper.status(cause));
        }
    }

    public static StreamMetadata started(
        String streamId,
        TaskId taskId,
        Instant expiration,
        List<ColumnInfo> columns,
        Map<String, String> headers,
        List<String> warnings
    ) {
        return new StreamMetadata(streamId, taskId, expiration, columns, headers, warnings, Status.RUNNING, null);
    }

    public StreamMetadata withExpiration(Instant newExpiration) {
        return new StreamMetadata(streamId, taskId, newExpiration, columns, headers, warnings, status, completionInfo);
    }

    public StreamMetadata complete(boolean partial, long tookMillis, long pages, long rows) {
        return new StreamMetadata(
            streamId,
            taskId,
            expiration,
            columns,
            headers,
            warnings,
            Status.COMPLETE,
            new StreamCompletionInfo(partial, tookMillis, pages, rows, null)
        );
    }

    public StreamMetadata failed(boolean partial, long tookMillis, Exception exception) {
        return new StreamMetadata(
            streamId,
            taskId,
            expiration,
            columns,
            headers,
            warnings,
            Status.FAILED,
            new StreamCompletionInfo(partial, tookMillis, null, null, StreamFailure.of(exception))
        );
    }

    public StreamMetadata canceled() {
        return new StreamMetadata(streamId, taskId, expiration, columns, headers, warnings, Status.CANCELED, null);
    }
}
