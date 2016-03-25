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

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Information about a currently running task.
 * <p>
 * Tasks are used for communication with transport actions. As a result, they can contain callback
 * references as well as mutable state. That makes it impractical to send tasks over transport channels
 * and use in APIs. Instead, immutable and streamable TaskInfo objects are used to represent
 * snapshot information about currently running tasks.
 */
public class TaskInfo implements Writeable<TaskInfo>, ToXContent {

    private final DiscoveryNode node;

    private final TaskId taskId;

    private final String type;

    private final String action;

    private final String description;

    private final long startTime;

    private final long runningTimeNanos;

    private final Task.Status status;

    private final TaskId parentTaskId;

    public TaskInfo(DiscoveryNode node, long id, String type, String action, String description, Task.Status status, long startTime,
                    long runningTimeNanos, TaskId parentTaskId) {
        this.node = node;
        this.taskId = new TaskId(node.getId(), id);
        this.type = type;
        this.action = action;
        this.description = description;
        this.status = status;
        this.startTime = startTime;
        this.runningTimeNanos = runningTimeNanos;
        this.parentTaskId = parentTaskId;
    }

    public TaskInfo(StreamInput in) throws IOException {
        node = DiscoveryNode.readNode(in);
        taskId = new TaskId(node.getId(), in.readLong());
        type = in.readString();
        action = in.readString();
        description = in.readOptionalString();
        if (in.readBoolean()) {
            status = in.readTaskStatus();
        } else {
            status = null;
        }
        startTime = in.readLong();
        runningTimeNanos = in.readLong();
        parentTaskId = new TaskId(in);
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public DiscoveryNode getNode() {
        return node;
    }

    public long getId() {
        return taskId.getId();
    }

    public String getType() {
        return type;
    }

    public String getAction() {
        return action;
    }

    public String getDescription() {
        return description;
    }

    /**
     * The status of the running task. Only available if TaskInfos were build
     * with the detailed flag.
     */
    public Task.Status getStatus() {
        return status;
    }

    /**
     * Returns the task start time
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the task running time
     */
    public long getRunningTimeNanos() {
        return runningTimeNanos;
    }

    /**
     * Returns the parent task id
     */
    public TaskId getParentTaskId() {
        return parentTaskId;
    }

    @Override
    public TaskInfo readFrom(StreamInput in) throws IOException {
        return new TaskInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        out.writeLong(taskId.getId());
        out.writeString(type);
        out.writeString(action);
        out.writeOptionalString(description);
        if (status != null) {
            out.writeBoolean(true);
            out.writeTaskStatus(status);
        } else {
            out.writeBoolean(false);
        }
        out.writeLong(startTime);
        out.writeLong(runningTimeNanos);
        parentTaskId.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node", node.getId());
        builder.field("id", taskId.getId());
        builder.field("type", type);
        builder.field("action", action);
        if (status != null) {
            builder.field("status", status, params);
        }
        if (description != null) {
            builder.field("description", description);
        }
        builder.dateValueField("start_time_in_millis", "start_time", startTime);
        builder.timeValueField("running_time_in_nanos", "running_time", runningTimeNanos, TimeUnit.NANOSECONDS);
        if (parentTaskId.isSet()) {
            builder.field("parent_task_id", parentTaskId.toString());
        }
        return builder;
    }
}
