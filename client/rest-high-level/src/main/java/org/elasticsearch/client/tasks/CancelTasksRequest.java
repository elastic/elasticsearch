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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CancelTasksRequest implements Validatable {

    private final List<String> nodes = new ArrayList<>();
    private final List<String> actions = new ArrayList<>();
    private Optional<TimeValue> timeout = Optional.empty();
    private Optional<TaskId> parentTaskId = Optional.empty();
    private Optional<TaskId> taskId = Optional.empty();
    private Boolean waitForCompletion;

    CancelTasksRequest(){}

    void setNodes(List<String> nodes) {
        this.nodes.addAll(nodes);
    }

    public List<String> getNodes() {
        return nodes;
    }

    void setTimeout(TimeValue timeout) {
        this.timeout = Optional.of(timeout);
    }

    public Optional<TimeValue> getTimeout() {
        return timeout;
    }

    void setActions(List<String> actions) {
        this.actions.addAll(actions);
    }

    public List<String> getActions() {
        return actions;
    }

    void setParentTaskId(TaskId parentTaskId) {
        this.parentTaskId = Optional.of(parentTaskId);
    }

    public Optional<TaskId> getParentTaskId() {
        return parentTaskId;
    }

    void setTaskId(TaskId taskId) {
        this.taskId = Optional.of(taskId);
    }

    public Optional<TaskId> getTaskId() {
        return taskId;
    }

    public Boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CancelTasksRequest)) return false;
        CancelTasksRequest that = (CancelTasksRequest) o;
        return Objects.equals(getNodes(), that.getNodes()) &&
            Objects.equals(getActions(), that.getActions()) &&
            Objects.equals(getTimeout(), that.getTimeout()) &&
            Objects.equals(getParentTaskId(), that.getParentTaskId()) &&
            Objects.equals(getTaskId(), that.getTaskId()) &&
            Objects.equals(waitForCompletion, that.waitForCompletion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodes(), getActions(), getTimeout(), getParentTaskId(), getTaskId(), waitForCompletion);
    }

    @Override
    public String toString() {
        return "CancelTasksRequest{" +
            "nodes=" + nodes +
            ", actions=" + actions +
            ", timeout=" + timeout +
            ", parentTaskId=" + parentTaskId +
            ", taskId=" + taskId +
            ", waitForCompletion=" + waitForCompletion +
            '}';
    }

    public static class Builder {
        private Optional<TimeValue> timeout = Optional.empty();
        private Optional<TaskId> taskId = Optional.empty();
        private Optional<TaskId> parentTaskId = Optional.empty();
        private List<String> actionsFilter = new ArrayList<>();
        private List<String> nodesFilter = new ArrayList<>();
        private Boolean waitForCompletion;

        public Builder withTimeout(TimeValue timeout){
            this.timeout = Optional.of(timeout);
            return this;
        }

        public Builder withTaskId(TaskId taskId){
            this.taskId = Optional.of(taskId);
            return this;
        }

        public Builder withParentTaskId(TaskId taskId){
            this.parentTaskId = Optional.of(taskId);
            return this;
        }

        public Builder withActionsFiltered(List<String> actions){
            this.actionsFilter.clear();
            this.actionsFilter.addAll(actions);
            return this;
        }

        public Builder withNodesFiltered(List<String> nodes){
            this.nodesFilter.clear();
            this.nodesFilter.addAll(nodes);
            return this;
        }

        public Builder withWaitForCompletion(boolean waitForCompletion) {
            this.waitForCompletion = waitForCompletion;
            return this;
        }

        public CancelTasksRequest build() {
            CancelTasksRequest request = new CancelTasksRequest();
            timeout.ifPresent(request::setTimeout);
            taskId.ifPresent(request::setTaskId);
            parentTaskId.ifPresent(request::setParentTaskId);
            request.setNodes(nodesFilter);
            request.setActions(actionsFilter);
            if (waitForCompletion != null) {
                request.setWaitForCompletion(waitForCompletion);
            }
            return request;
        }
    }
}
