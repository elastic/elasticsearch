/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/** A request to cancel an ongoing reindex task. */
public class CancelReindexRequest extends ActionRequest {

    private final TaskId taskId;
    private final boolean waitForCompletion;

    public CancelReindexRequest(final TaskId taskId, final boolean waitForCompletion) {
        this.taskId = Objects.requireNonNull(taskId, "task id cannot be null");
        this.waitForCompletion = waitForCompletion;
    }

    public CancelReindexRequest(final StreamInput in) throws IOException {
        super(in);
        this.taskId = TaskId.readFromStream(in);
        this.waitForCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        taskId.writeTo(out);
        out.writeBoolean(waitForCompletion);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (taskId.isSet() == false) {
            validationException = addValidationError("task id must be provided", validationException);
        }
        return validationException;
    }

    @Override
    public String getDescription() {
        return "taskId[" + taskId + "], waitForCompletion[" + waitForCompletion + "]";
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public boolean waitForCompletion() {
        return waitForCompletion;
    }
}
