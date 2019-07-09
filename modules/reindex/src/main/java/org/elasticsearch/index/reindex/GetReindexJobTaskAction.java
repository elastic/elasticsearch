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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;

public class GetReindexJobTaskAction extends ActionType<GetReindexJobTaskAction.Responses> {

    // TODO: Name
    public static final String NAME = "cluster:monitor/reindex/get";
    public static final GetReindexJobTaskAction INSTANCE = new GetReindexJobTaskAction();

    private GetReindexJobTaskAction() {
        super(NAME, GetReindexJobTaskAction.Responses::new);
    }

    public static class Request extends BaseTasksRequest<Request> {

        private final String persistentTaskId;

        public Request(StreamInput input) throws IOException {
            super(input);
            persistentTaskId = input.readString();
        }

        public Request(String nodeId, String persistentTaskId) {
            this.persistentTaskId = persistentTaskId;
            setNodes(nodeId);
        }

        @Override
        public boolean match(Task task) {
            if (task instanceof AllocatedPersistentTask) {
                AllocatedPersistentTask persistentTask = (AllocatedPersistentTask) task;
                return persistentTaskId.equals(persistentTask.getPersistentTaskId());
            } else {
                return false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(persistentTaskId);
        }
    }

    public static class Response implements Writeable {

        private final TaskId taskId;

        public Response(StreamInput input) throws IOException {
            taskId = TaskId.readFromStream(input);
        }

        public Response(TaskId taskId) {
            this.taskId = taskId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            taskId.writeTo(out);
        }

        public TaskId getTaskId() {
            return taskId;
        }
    }

    public static class Responses extends BaseTasksResponse {

        private final List<Response> tasks;

        Responses(StreamInput input) throws IOException {
            super(input);
            tasks = input.readList(Response::new);
        }

        Responses(List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
                  List<FailedNodeException> failedNodeExceptions) {
            super(taskOperationFailures, failedNodeExceptions);
            this.tasks = tasks;
        }

        public List<Response> getTasks() {
            return tasks;
        }
    }
}
