/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to cancel tasks
 */
public class CancelTasksRequest extends BaseTasksRequest<CancelTasksRequest> {

    public static final String DEFAULT_REASON = "by user request";
    public static final boolean DEFAULT_WAIT_FOR_COMPLETION = false;

    private String reason = DEFAULT_REASON;
    private boolean waitForCompletion = DEFAULT_WAIT_FOR_COMPLETION;

    public CancelTasksRequest() {}

    public CancelTasksRequest(StreamInput in) throws IOException {
        super(in);
        this.reason = in.readString();
        waitForCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(reason);
        out.writeBoolean(waitForCompletion);
    }

    @Override
    public boolean match(Task task) {
        return super.match(task) && task instanceof CancellableTask;
    }

    /**
     * Set the reason for canceling the task.
     */
    public CancelTasksRequest setReason(String reason) {
        this.reason = reason;
        return this;
    }

    /**
     * The reason for canceling the task.
     */
    public String getReason() {
        return reason;
    }

    /**
     * If {@code true}, the request blocks until the cancellation of the task and its descendant tasks is completed.
     * Otherwise, the request can return soon after the cancellation is started. Defaults to {@code false}.
     */
    public void setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public String getDescription() {
        return "reason[" + reason +
            "], waitForCompletion[" + waitForCompletion +
            "], taskId[" + getTaskId() +
            "], parentTaskId[" + getParentTaskId() +
            "], nodes" + Arrays.toString(getNodes()) +
            ", actions" + Arrays.toString(getActions());
    }
}
