/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Request to read a stored stream result: the rows from {@link #offset()} on, limited by {@link #maxRows()} and {@link #maxBytes()} when
 * they are set.
 * <p>
 * The request runs on the node that receives it and is never sent to another node, so it is not serializable. Its task is cancellable so
 * that a client disconnect stops a wait for rows.
 */
public class GetStreamResultRequest extends UntypedActionRequest {

    /** Value of {@link #maxRows()} and {@link #maxBytes()} when the limit was not provided. */
    public static final long NO_LIMIT = -1L;

    private final String streamId;
    private final long offset;
    private final long maxRows;
    private final long maxBytes;
    private final TimeValue waitTimeout;

    /**
     * @param streamId    the stream id
     * @param offset      first row to return, {@code >= 0}
     * @param maxRows     stop after this many rows, {@code >= 0}, or {@link #NO_LIMIT}. {@code 0} returns the status only.
     * @param maxBytes    stop after about this many bytes of rows, {@code >= 0}, or {@link #NO_LIMIT}. {@code 0} returns the status only.
     * @param waitTimeout how long to wait for rows when there are none yet. {@code null}, zero or negative means do not wait,
     *                    as an unset {@code wait_for_completion_timeout} does for the async API.
     */
    public GetStreamResultRequest(String streamId, long offset, long maxRows, long maxBytes, TimeValue waitTimeout) {
        this.streamId = streamId;
        if (offset < 0) {
            throw new IllegalArgumentException("[offset] must be greater than or equal to 0 but was [" + offset + "]");
        }
        if (maxRows < 0 && maxRows != NO_LIMIT) {
            throw new IllegalArgumentException("[max_rows] must be greater than or equal to 0 but was [" + maxRows + "]");
        }
        if (maxBytes < 0 && maxBytes != NO_LIMIT) {
            throw new IllegalArgumentException("[max_bytes] must be greater than or equal to 0 but was [" + maxBytes + "]");
        }
        this.offset = offset;
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.waitTimeout = waitTimeout == null ? TimeValue.MINUS_ONE : waitTimeout;
    }

    public String streamId() {
        return streamId;
    }

    public long offset() {
        return offset;
    }

    public long maxRows() {
        return maxRows;
    }

    public long maxBytes() {
        return maxBytes;
    }

    public TimeValue waitTimeout() {
        return waitTimeout;
    }

    public boolean hasRowLimit() {
        return maxRows != NO_LIMIT;
    }

    public boolean hasByteLimit() {
        return maxBytes != NO_LIMIT;
    }

    /** {@code max_rows=0} or {@code max_bytes=0}: only the status is returned, no pages are read and there is no wait. */
    public boolean statusOnly() {
        return maxRows == 0 || maxBytes == 0;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (streamId == null || streamId.isEmpty()) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("id is missing");
            return e;
        }
        return null;
    }

    @Override
    public Task createTask(TaskId taskId, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(taskId.getId(), type, action, "stream get result", parentTaskId, headers);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetStreamResultRequest that = (GetStreamResultRequest) o;
        return offset == that.offset
            && maxRows == that.maxRows
            && maxBytes == that.maxBytes
            && Objects.equals(streamId, that.streamId)
            && waitTimeout.equals(that.waitTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, offset, maxRows, maxBytes, waitTimeout);
    }
}
