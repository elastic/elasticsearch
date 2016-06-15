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

package org.elasticsearch.tasks;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;


/**
 * Information about a currently running task.
 * <p>
 * Tasks are used for communication with transport actions. As a result, they can contain callback
 * references as well as mutable state. That makes it impractical to send tasks over transport channels
 * and use in APIs. Instead, immutable and streamable TaskInfo objects are used to represent
 * snapshot information about currently running tasks.
 */
public final class TaskInfo implements Writeable, ToXContent {
    private final TaskId taskId;

    private final String type;

    private final String action;

    private final String description;

    private final long startTime;

    private final long runningTimeNanos;

    private final Task.Status status;

    private final boolean cancellable;

    private final TaskId parentTaskId;

    public TaskInfo(TaskId taskId, String type, String action, String description, Task.Status status, long startTime,
                    long runningTimeNanos, boolean cancellable, TaskId parentTaskId) {
        this.taskId = taskId;
        this.type = type;
        this.action = action;
        this.description = description;
        this.status = status;
        this.startTime = startTime;
        this.runningTimeNanos = runningTimeNanos;
        this.cancellable = cancellable;
        this.parentTaskId = parentTaskId;
    }

    /**
     * Read from a stream.
     */
    public TaskInfo(StreamInput in) throws IOException {
        taskId = TaskId.readFromStream(in);
        type = in.readString();
        action = in.readString();
        description = in.readOptionalString();
        status = in.readOptionalNamedWriteable(Task.Status.class);
        startTime = in.readLong();
        runningTimeNanos = in.readLong();
        cancellable = in.readBoolean();
        parentTaskId = TaskId.readFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskId.writeTo(out);
        out.writeString(type);
        out.writeString(action);
        out.writeOptionalString(description);
        out.writeOptionalNamedWriteable(status);
        out.writeLong(startTime);
        out.writeLong(runningTimeNanos);
        out.writeBoolean(cancellable);
        parentTaskId.writeTo(out);
    }

    public TaskId getTaskId() {
        return taskId;
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
     * Returns true if the task supports cancellation
     */
    public boolean isCancellable() {
        return cancellable;
    }

    /**
     * Returns the parent task id
     */
    public TaskId getParentTaskId() {
        return parentTaskId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node", taskId.getNodeId());
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
        builder.field("cancellable", cancellable);
        if (parentTaskId.isSet()) {
            builder.field("parent_task_id", parentTaskId.toString());
        }
        return builder;
    }

    public static final ConstructingObjectParser<TaskInfo, ParseFieldMatcherSupplier> PARSER = new ConstructingObjectParser<>(
            "task_info", a -> {
                int i = 0;
                TaskId id = new TaskId((String) a[i++], (Long) a[i++]);
                String type = (String) a[i++];
                String action = (String) a[i++];
                String description = (String) a[i++];
                BytesReference statusBytes = (BytesReference) a[i++];
                long startTime = (Long) a[i++];
                long runningTimeNanos = (Long) a[i++];
                boolean cancellable = (Boolean) a[i++];
                String parentTaskIdString = (String) a[i++];

                RawTaskStatus status = statusBytes == null ? null : new RawTaskStatus(statusBytes);
                TaskId parentTaskId = parentTaskIdString == null ? TaskId.EMPTY_TASK_ID : new TaskId((String) parentTaskIdString);
                return new TaskInfo(id, type, action, description, status, startTime, runningTimeNanos, cancellable, parentTaskId);
            });
    static {
        // Note for the future: this has to be backwards compatible with all changes to the task persistence format
        PARSER.declareString(constructorArg(), new ParseField("node"));
        PARSER.declareLong(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("type"));
        PARSER.declareString(constructorArg(), new ParseField("action"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
        PARSER.declareRawObject(optionalConstructorArg(), new ParseField("status"));
        PARSER.declareLong(constructorArg(), new ParseField("start_time_in_millis"));
        PARSER.declareLong(constructorArg(), new ParseField("running_time_in_nanos"));
        PARSER.declareBoolean(constructorArg(), new ParseField("cancellable"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("parent_task_id"));
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    // Implements equals and hashCode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskInfo.class) {
            return false;
        }
        TaskInfo other = (TaskInfo) obj;
        return Objects.equals(taskId, other.taskId)
                && Objects.equals(type, other.type)
                && Objects.equals(action, other.action)
                && Objects.equals(description, other.description)
                && Objects.equals(startTime, other.startTime)
                && Objects.equals(runningTimeNanos, other.runningTimeNanos)
                && Objects.equals(parentTaskId, other.parentTaskId)
                && Objects.equals(cancellable, other.cancellable)
                && Objects.equals(status, other.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, type, action, description, startTime, runningTimeNanos, parentTaskId, cancellable, status);
    }
}
