/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;

import java.io.IOException;

/**
 * A request to cancel tasks
 */
public class CancelTasksRequest extends BaseTasksRequest<CancelTasksRequest> {

    public static final String DEFAULT_REASON = "by user request";
    public static final boolean DEFAULT_WAIT_FOR_COMPLETION = false;

    private static final int MAX_DESCRIPTION_ITEMS = 10;

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
        StringBuilder sb = new StringBuilder();

        sb.append("reason[")
            .append(reason)
            .append("], waitForCompletion[")
            .append(waitForCompletion)
            .append("], targetTaskId[")
            .append(getTargetTaskId())
            .append("], targetParentTaskId[")
            .append(getTargetParentTaskId())
            .append("], nodes[");
        appendBoundedArray(sb, getNodes());
        sb.append(']');
        sb.append(", actions[");
        appendBoundedArray(sb, getActions());
        sb.append(']');

        return sb.toString();
    }

    private static void appendBoundedArray(StringBuilder sb, String[] values) {
        if (values == null || values.length == 0) {
            return;
        }

        var collector = new Strings.BoundedDelimitedStringCollector(sb, ", ", 1024);
        for (String v : values) {
            collector.appendItem(v);
        }
        collector.finish();
    }
}
