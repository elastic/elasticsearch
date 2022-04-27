/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.get;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to get node tasks
 */
public class GetTaskRequest extends ActionRequest {
    private TaskId taskId = TaskId.EMPTY_TASK_ID;
    private boolean waitForCompletion = false;
    private TimeValue timeout = null;

    /**
     * Get the TaskId to look up.
     */
    public GetTaskRequest() {}

    public GetTaskRequest(StreamInput in) throws IOException {
        super(in);
        taskId = TaskId.readFromStream(in);
        timeout = in.readOptionalTimeValue();
        waitForCompletion = in.readBoolean();
    }

    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * Set the TaskId to look up. Required.
     */
    public GetTaskRequest setTaskId(TaskId taskId) {
        this.taskId = taskId;
        return this;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public GetTaskRequest setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Timeout to wait for any async actions this request must take. It must take anywhere from 0 to 2.
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * Timeout to wait for any async actions this request must take. It must take anywhere from 0 to 2.
     */
    public GetTaskRequest setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    GetTaskRequest nodeRequest(String thisNodeId, long thisTaskId) {
        GetTaskRequest copy = new GetTaskRequest();
        copy.setParentTask(thisNodeId, thisTaskId);
        copy.setTaskId(taskId);
        copy.setTimeout(timeout);
        copy.setWaitForCompletion(waitForCompletion);
        return copy;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (false == getTaskId().isSet()) {
            validationException = addValidationError("task id is required", validationException);
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
