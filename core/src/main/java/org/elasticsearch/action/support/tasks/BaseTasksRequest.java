/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A base class for task requests
 */
public class BaseTasksRequest<T extends BaseTasksRequest> extends ActionRequest<T> {

    public static final String[] ALL_ACTIONS = Strings.EMPTY_ARRAY;

    public static final String[] ALL_NODES = Strings.EMPTY_ARRAY;

    public static final long ALL_TASKS = -1L;

    private String[] nodesIds = ALL_NODES;

    private TimeValue timeout;

    private String[] actions = ALL_ACTIONS;

    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    private TaskId taskId = TaskId.EMPTY_TASK_ID;

    public BaseTasksRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (taskId.isSet() && nodesIds.length > 0) {
            validationException = addValidationError("task id cannot be used together with node ids",
                validationException);
        }
        return validationException;
    }

    /**
     * Sets the list of action masks for the actions that should be returned
     */
    @SuppressWarnings("unchecked")
    public final T setActions(String... actions) {
        this.actions = actions;
        return (T) this;
    }

    /**
     * Return the list of action masks for the actions that should be returned
     */
    public String[] getActions() {
        return actions;
    }

    public final String[] getNodesIds() {
        return nodesIds;
    }

    @SuppressWarnings("unchecked")
    public final T setNodesIds(String... nodesIds) {
        this.nodesIds = nodesIds;
        return (T) this;
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
    public final T setTaskId(TaskId taskId) {
        this.taskId = taskId;
        return (T) this;
    }


    /**
     * Returns the parent task id that tasks should be filtered by
     */
    public TaskId getParentTaskId() {
        return parentTaskId;
    }

    @SuppressWarnings("unchecked")
    public T setParentTaskId(TaskId parentTaskId) {
        this.parentTaskId = parentTaskId;
        return (T) this;
    }


    public TimeValue getTimeout() {
        return this.timeout;
    }

    @SuppressWarnings("unchecked")
    public final T setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public final T setTimeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout");
        return (T) this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        taskId = new TaskId(in);
        parentTaskId = new TaskId(in);
        nodesIds = in.readStringArray();
        actions = in.readStringArray();
        if (in.readBoolean()) {
            timeout = TimeValue.readTimeValue(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        taskId.writeTo(out);
        parentTaskId.writeTo(out);
        out.writeStringArrayNullable(nodesIds);
        out.writeStringArrayNullable(actions);
        out.writeOptionalStreamable(timeout);
    }

    public boolean match(Task task) {
        if (getActions() != null && getActions().length > 0 && Regex.simpleMatch(getActions(), task.getAction()) == false) {
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
