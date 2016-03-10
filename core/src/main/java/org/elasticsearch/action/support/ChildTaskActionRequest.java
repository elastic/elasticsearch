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

package org.elasticsearch.action.support;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

/**
 * Base class for action requests that can have associated child tasks
 */
public abstract class ChildTaskActionRequest<Request extends ActionRequest<Request>> extends ActionRequest<Request> {

    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    protected ChildTaskActionRequest() {

    }

    protected ChildTaskActionRequest(ActionRequest request) {
        super(request);
    }

    public void setParentTask(String parentTaskNode, long parentTaskId) {
        this.parentTaskId = new TaskId(parentTaskNode, parentTaskId);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().onOrAfter(Version.V_2_3_0)) {
            parentTaskId = new TaskId(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_3_0)) {
            parentTaskId.writeTo(out);
        }
    }

    @Override
    public final Task createTask(long id, String type, String action) {
        return createTask(id, type, action, parentTaskId);
    }

    public Task createTask(long id, String type, String action, TaskId parentTaskId) {
        return new Task(id, type, action, getDescription(), parentTaskId);
    }

}
