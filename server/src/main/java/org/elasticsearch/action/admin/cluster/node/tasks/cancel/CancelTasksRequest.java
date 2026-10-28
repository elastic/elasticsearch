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

    static final TransportVersion CANCEL_TASKS_EXCLUDE_CHILD_TASKS = TransportVersion.fromName("cancel_tasks_exclude_child_tasks");

    private String reason = DEFAULT_REASON;
    private boolean waitForCompletion = DEFAULT_WAIT_FOR_COMPLETION;
    /**
     * When {@code true}, child tasks (tasks whose {@link Task#getParentTaskId()} is set) are excluded from matching. Useful for callers
     * (e.g. cancel-reindex) that want to operate only on top-level user-initiated tasks and treat sub-tasks as if they didn't exist.
     */
    private boolean excludeChildTasks = false;

    public CancelTasksRequest() {}

    public CancelTasksRequest(StreamInput in) throws IOException {
        super(in);
        this.reason = in.readString();
        waitForCompletion = in.readBoolean();
        if (in.getTransportVersion().supports(CANCEL_TASKS_EXCLUDE_CHILD_TASKS)) {
            excludeChildTasks = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(reason);
        out.writeBoolean(waitForCompletion);
        if (out.getTransportVersion().supports(CANCEL_TASKS_EXCLUDE_CHILD_TASKS)) {
            out.writeBoolean(excludeChildTasks);
        }
    }

    @Override
    public boolean match(Task task) {
        // Matching by target task ID is deferred to TransportCancelTasksAction
        if ((task instanceof CancellableTask) == false || matchesActionAndParent(task) == false) {
            return false;
        }
        if (excludeChildTasks && task.getParentTaskId().isSet()) {
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
     * If {@code true}, child tasks are excluded from matching, leaving only top-level (parentless) tasks. Defaults to {@code false}.
     * <p>
     * Implemented as part of {@link #match(Task)}; combined with an explicit {@code targetTaskId}, this causes {@code processTasks} to
     * report {@link IllegalArgumentException} for child tasks (mirroring the existing "doesn't support this operation" behaviour for any
     * other filter mismatch).
     */
    public CancelTasksRequest setExcludeChildTasks(boolean excludeChildTasks) {
        this.excludeChildTasks = excludeChildTasks;
        return this;
    }

    public boolean excludeChildTasks() {
        return excludeChildTasks;
    }

    /// Copies all fields from `request` into this request, including the cancel-specific fields. Useful when transparently forwarding a
    /// request through a wrapper action (e.g. the double-broadcast path in [TransportCancelTasksAction]).
    public CancelTasksRequest copyFieldsFrom(final CancelTasksRequest request) {
        super.copyFieldsFrom(request);
        this.reason = request.reason;
        this.waitForCompletion = request.waitForCompletion;
        this.excludeChildTasks = request.excludeChildTasks;
        return this;
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
