/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A base class for task requests
 */
public class BaseTasksRequest<Request extends BaseTasksRequest<Request>> extends ActionRequest {

    public static final String[] ALL_ACTIONS = Strings.EMPTY_ARRAY;

    public static final String[] ALL_NODES = Strings.EMPTY_ARRAY;

    private String[] nodes = ALL_NODES;

    private TimeValue timeout;

    private String[] actions = ALL_ACTIONS;

    private TaskId targetParentTaskId = TaskId.EMPTY_TASK_ID;

    private TaskId targetTaskId = TaskId.EMPTY_TASK_ID;

    // NOTE: This constructor is only needed, because the setters in this class,
    // otherwise it can be removed and above fields can be made final.
    public BaseTasksRequest() {}

    protected BaseTasksRequest(StreamInput in) throws IOException {
        super(in);
        targetTaskId = TaskId.readFromStream(in);
        targetParentTaskId = TaskId.readFromStream(in);
        nodes = in.readStringArray();
        actions = in.readStringArray();
        timeout = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        targetTaskId.writeTo(out);
        targetParentTaskId.writeTo(out);
        out.writeStringArrayNullable(nodes);
        out.writeStringArrayNullable(actions);
        out.writeOptionalTimeValue(timeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (targetTaskId.isSet() && nodes.length > 0) {
            validationException = addValidationError("task id cannot be used together with node ids", validationException);
        }
        return validationException;
    }

    /**
     * Sets the list of action masks for the actions that should be returned
     */
    @SuppressWarnings("unchecked")
    public final Request setActions(String... actions) {
        this.actions = actions;
        return (Request) this;
    }

    /**
     * Return the list of action masks for the actions that should be returned
     */
    public String[] getActions() {
        return actions;
    }

    public final String[] getNodes() {
        return nodes;
    }

    @SuppressWarnings("unchecked")
    public final Request setNodes(String... nodes) {
        this.nodes = nodes;
        return (Request) this;
    }

    /**
     * Returns the id of the task that should be processed.
     *
     * By default tasks with any ids are returned.
     */
    public TaskId getTargetTaskId() {
        return targetTaskId;
    }

    @SuppressWarnings("unchecked")
    public final Request setTargetTaskId(TaskId targetTaskId) {
        this.targetTaskId = targetTaskId;
        return (Request) this;
    }

    /**
     * @deprecated Use {@link #getTargetTaskId()}
     */
    @Deprecated
    public TaskId getTaskId() {
        return getTargetTaskId();
    }

    /**
     * @deprecated Use {@link #setTargetTaskId(TaskId)}
     */
    @Deprecated
    public final Request setTaskId(TaskId taskId) {
        return setTargetTaskId(taskId);
    }

    /**
     * Returns the parent task id that tasks should be filtered by
     */
    public TaskId getTargetParentTaskId() {
        return targetParentTaskId;
    }

    @SuppressWarnings("unchecked")
    public Request setTargetParentTaskId(TaskId targetParentTaskId) {
        this.targetParentTaskId = targetParentTaskId;
        return (Request) this;
    }

    /**
     * @deprecated Use {@link #getTargetParentTaskId()}
     */
    @Deprecated
    public TaskId getParentTaskId() {
        return getTargetParentTaskId();
    }

    /**
     * @deprecated Use {@link #setTargetParentTaskId(TaskId)}
     */
    @Deprecated
    public Request setParentTaskId(TaskId parentTaskId) {
        return setTargetParentTaskId(parentTaskId);
    }

    public TimeValue getTimeout() {
        return this.timeout;
    }

    @SuppressWarnings("unchecked")
    public final Request setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    @SuppressWarnings("unchecked")
    public final Request setTimeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout");
        return (Request) this;
    }

    public boolean match(Task task) {
        if (CollectionUtils.isEmpty(getActions()) == false && Regex.simpleMatch(getActions(), task.getAction()) == false) {
            return false;
        }
        if (getTargetTaskId().isSet()) {
            if (getTargetTaskId().getId() != task.getId()) {
                return false;
            }
        }
        if (targetParentTaskId.isSet()) {
            if (targetParentTaskId.equals(task.getParentTaskId()) == false) {
                return false;
            }
        }
        return true;
    }
}
