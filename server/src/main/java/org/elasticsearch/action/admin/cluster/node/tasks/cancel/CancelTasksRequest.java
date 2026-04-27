/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.TransportVersion;
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

    static final TransportVersion CANCEL_TASKS_PARENT_TASK_ONLY = TransportVersion.fromName("cancel_tasks_parent_task_only");

    private String reason = DEFAULT_REASON;
    private boolean waitForCompletion = DEFAULT_WAIT_FOR_COMPLETION;
    /**
     * When {@code true}, restricts matching to tasks that have no parent. Useful for callers (e.g. cancel-reindex) that want to operate
     * only on top-level user-initiated tasks and treat sub-tasks as if they didn't exist.
     */
    private boolean parentTaskOnly = false;

    public CancelTasksRequest() {}

    public CancelTasksRequest(StreamInput in) throws IOException {
        super(in);
        this.reason = in.readString();
        waitForCompletion = in.readBoolean();
        if (in.getTransportVersion().supports(CANCEL_TASKS_PARENT_TASK_ONLY)) {
            parentTaskOnly = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(reason);
        out.writeBoolean(waitForCompletion);
        if (out.getTransportVersion().supports(CANCEL_TASKS_PARENT_TASK_ONLY)) {
            out.writeBoolean(parentTaskOnly);
        }
    }

    @Override
    public boolean match(Task task) {
        // Matching by target task ID is deferred to TransportCancelTasksAction
        if ((task instanceof CancellableTask) == false || matchesActionAndParent(task) == false) {
            return false;
        }
        if (parentTaskOnly && task.getParentTaskId().isSet()) {
            return false;
        }
        return true;
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

    /**
     * If {@code true}, the request rejects target tasks that have a parent (i.e. only matches top-level tasks). Defaults to {@code false}.
     * <p>
     * Implemented as part of {@link #match(Task)}; combined with an explicit {@code targetTaskId}, this causes {@code processTasks} to
     * report {@link IllegalArgumentException} for sub-tasks (mirroring the existing "doesn't support this operation" behaviour for any
     * other filter mismatch).
     */
    public CancelTasksRequest setParentTaskOnly(boolean parentTaskOnly) {
        this.parentTaskOnly = parentTaskOnly;
        return this;
    }

    public boolean parentTaskOnly() {
        return parentTaskOnly;
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
