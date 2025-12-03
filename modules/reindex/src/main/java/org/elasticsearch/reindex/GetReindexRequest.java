/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to get a reindex task. This ensures that the task is validated as a reindex task.
 */
public class GetReindexRequest extends ActionRequest {
    private TaskId taskId = TaskId.EMPTY_TASK_ID;
    private boolean waitForCompletion = false;
    private TimeValue timeout = null;

    public GetReindexRequest() {}

    public GetReindexRequest(StreamInput in) throws IOException {
        super(in);
        taskId = TaskId.readFromStream(in);
        timeout = in.readOptionalTimeValue();
        waitForCompletion = in.readBoolean();
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public void setTaskId(TaskId taskId) {
        this.taskId = taskId;
    }

    public boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (taskId.isSet() == false) {
            validationException = addValidationError("id is required", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        taskId.writeTo(out);
        out.writeOptionalTimeValue(timeout);
        out.writeBoolean(waitForCompletion);
    }
}
