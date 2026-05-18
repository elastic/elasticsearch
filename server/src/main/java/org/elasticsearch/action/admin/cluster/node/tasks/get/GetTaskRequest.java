/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.get;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to get node tasks
 */
public class GetTaskRequest extends LegacyActionRequest {

    private static final TransportVersion FOLLOW_RELOCATIONS_VERSION = TransportVersion.fromName("get_task_follow_relocations");

    private TaskId taskId = TaskId.EMPTY_TASK_ID;
    private boolean waitForCompletion = false;
    private TimeValue timeout = null;
    private boolean followRelocations = true;

    /**
     * Get the TaskId to look up.
     */
    public GetTaskRequest() {}

    public GetTaskRequest(StreamInput in) throws IOException {
        super(in);
        taskId = TaskId.readFromStream(in);
        timeout = in.readOptionalTimeValue();
        waitForCompletion = in.readBoolean();
        if (in.getTransportVersion().supports(FOLLOW_RELOCATIONS_VERSION)) {
            followRelocations = in.readBoolean();
        }
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

    /**
     * Whether to follow the relocation chain for relocated reindex tasks. Defaults to {@code true}.
     * When set to {@code false}, the raw task result is returned without following relocations,
     * which is useful for debugging individual tasks in a relocation chain.
     */
    public boolean getFollowRelocations() {
        return followRelocations;
    }

    /**
     * Set whether to follow the relocation chain for relocated reindex tasks.
     */
    public GetTaskRequest setFollowRelocations(boolean followRelocations) {
        this.followRelocations = followRelocations;
        return this;
    }

    GetTaskRequest nodeRequest(String thisNodeId, long thisTaskId) {
        GetTaskRequest copy = new GetTaskRequest();
        copy.setParentTask(thisNodeId, thisTaskId);
        copy.setTaskId(taskId);
        copy.setTimeout(timeout);
        copy.setWaitForCompletion(waitForCompletion);
        copy.setFollowRelocations(followRelocations);
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
        if (out.getTransportVersion().supports(FOLLOW_RELOCATIONS_VERSION)) {
            out.writeBoolean(followRelocations);
        }
    }
}
