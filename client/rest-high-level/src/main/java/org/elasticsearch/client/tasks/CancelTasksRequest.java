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
package org.elasticsearch.client.tasks;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.Objects;

public class CancelTasksRequest implements Validatable {

    public static final String[] EMPTY_ARRAY = new String[0];
    public static final String[] ALL_ACTIONS = EMPTY_ARRAY;
    public static final String[] ALL_NODES = EMPTY_ARRAY;
    private String[] nodes = ALL_NODES;
    private TimeValue timeout;
    private String[] actions = ALL_ACTIONS;
    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;
    private TaskId taskId = TaskId.EMPTY_TASK_ID;
    private String reason = "";

    public final CancelTasksRequest setNodes(String... nodes) {
        this.nodes = nodes;
        return this;
    }

    public String[] getNodes() {
        return nodes;
    }

    public CancelTasksRequest setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public final CancelTasksRequest setTimeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout");
        return this;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public CancelTasksRequest setActions(String... actions) {
        this.actions = actions;
        return this;
    }

    public String[] getActions() {
        return actions;
    }

    public CancelTasksRequest setParentTaskId(TaskId parentTaskId) {
        this.parentTaskId = parentTaskId;
        return this;
    }

    public TaskId getParentTaskId() {
        return parentTaskId;
    }

    public CancelTasksRequest setTaskId(TaskId taskId) {
        this.taskId = taskId;
        return this;
    }

    public TaskId getTaskId() {
        return taskId;
    }


    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CancelTasksRequest)) return false;
        CancelTasksRequest that = (CancelTasksRequest) o;
        return Arrays.equals(getNodes(), that.getNodes()) &&
            Objects.equals(getTimeout(), that.getTimeout()) &&
            Arrays.equals(getActions(), that.getActions()) &&
            Objects.equals(getParentTaskId(), that.getParentTaskId()) &&
            Objects.equals(getTaskId(), that.getTaskId()) &&
            Objects.equals(getReason(), that.getReason());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getTimeout(), getParentTaskId(), getTaskId(), getReason());
        result = 31 * result + Arrays.hashCode(getNodes());
        result = 31 * result + Arrays.hashCode(getActions());
        return result;
    }

    @Override
    public String toString() {
        return "CancelTasksRequest{" +
            "nodes=" + Arrays.toString(nodes) +
            ", timeout=" + timeout +
            ", actions=" + Arrays.toString(actions) +
            ", parentTaskId=" + parentTaskId +
            ", taskId=" + taskId +
            ", reason='" + reason + '\'' +
            '}';
    }
}
