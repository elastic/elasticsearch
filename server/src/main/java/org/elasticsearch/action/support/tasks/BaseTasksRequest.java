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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
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

    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    private TaskId taskId = TaskId.EMPTY_TASK_ID;

    // NOTE: This constructor is only needed, because the setters in this class,
    // otherwise it can be removed and above fields can be made final.
    public BaseTasksRequest() {
    }

    protected BaseTasksRequest(StreamInput in) throws IOException {
        super(in);
        taskId = TaskId.readFromStream(in);
        parentTaskId = TaskId.readFromStream(in);
        nodes = in.readStringArray();
        actions = in.readStringArray();
        timeout = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        taskId.writeTo(out);
        parentTaskId.writeTo(out);
        out.writeStringArrayNullable(nodes);
        out.writeStringArrayNullable(actions);
        out.writeOptionalTimeValue(timeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (taskId.isSet() && nodes.length > 0) {
            validationException = addValidationError("task id cannot be used together with node ids",
                validationException);
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
    public TaskId getTaskId() {
        return taskId;
    }

    @SuppressWarnings("unchecked")
    public final Request setTaskId(TaskId taskId) {
        this.taskId = taskId;
        return (Request) this;
    }


    /**
     * Returns the parent task id that tasks should be filtered by
     */
    public TaskId getParentTaskId() {
        return parentTaskId;
    }

    @SuppressWarnings("unchecked")
    public Request setParentTaskId(TaskId parentTaskId) {
        this.parentTaskId = parentTaskId;
        return (Request) this;
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
        if (getTaskId().isSet()) {
            if(getTaskId().getId() != task.getId()) {
                return false;
            }
        }
        if (parentTaskId.isSet()) {
            if (parentTaskId.equals(task.getParentTaskId()) == false) {
                return false;
            }
        }
        return true;
    }
}
