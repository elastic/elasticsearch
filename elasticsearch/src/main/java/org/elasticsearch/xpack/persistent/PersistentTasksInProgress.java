/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.Task.Status;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A cluster state record that contains a list of all running persistent tasks
 */
public final class PersistentTasksInProgress extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {
    public static final String TYPE = "persistent_tasks";

    // TODO: Implement custom Diff for tasks
    private final Map<Long, PersistentTaskInProgress<?>> tasks;

    private final long currentId;

    public PersistentTasksInProgress(long currentId, Map<Long, PersistentTaskInProgress<?>> tasks) {
        this.currentId = currentId;
        this.tasks = tasks;
    }

    public Collection<PersistentTaskInProgress<?>> tasks() {
        return this.tasks.values();
    }

    public Map<Long, PersistentTaskInProgress<?>> taskMap() {
        return this.tasks;
    }

    public PersistentTaskInProgress<?> getTask(long id) {
        return this.tasks.get(id);
    }

    public Collection<PersistentTaskInProgress<?>> findTasks(String actionName, Predicate<PersistentTaskInProgress<?>> predicate) {
        return this.tasks().stream()
                .filter(p -> actionName.equals(p.getAction()))
                .filter(predicate)
                .collect(Collectors.toList());
    }

    public boolean tasksExist(String actionName, Predicate<PersistentTaskInProgress<?>> predicate) {
        return this.tasks().stream()
                .filter(p -> actionName.equals(p.getAction()))
                .anyMatch(predicate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentTasksInProgress that = (PersistentTasksInProgress) o;
        return currentId == that.currentId &&
                Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tasks, currentId);
    }

    public long getNumberOfTasksOnNode(String nodeId, String action) {
        return tasks.values().stream().filter(task -> action.equals(task.action) && nodeId.equals(task.executorNode)).count();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_5_3_0_UNRELEASED;
    }

    /**
     * A record that represents a single running persistent task
     */
    public static class PersistentTaskInProgress<Request extends PersistentActionRequest> implements Writeable, ToXContent {
        private final long id;
        private final long allocationId;
        private final String action;
        private final Request request;
        @Nullable
        private final Status status;
        @Nullable
        private final String executorNode;


        public PersistentTaskInProgress(long id, String action, Request request, String executorNode) {
            this(id, 0L, action, request, null, executorNode);
        }

        public PersistentTaskInProgress(PersistentTaskInProgress<Request> persistentTaskInProgress, String newExecutorNode) {
            this(persistentTaskInProgress.id, persistentTaskInProgress.allocationId + 1L,
                    persistentTaskInProgress.action, persistentTaskInProgress.request, null, newExecutorNode);
        }

        public PersistentTaskInProgress(PersistentTaskInProgress<Request> persistentTaskInProgress, Status status) {
            this(persistentTaskInProgress.id, persistentTaskInProgress.allocationId,
                    persistentTaskInProgress.action, persistentTaskInProgress.request, status, persistentTaskInProgress.executorNode);
        }

        private PersistentTaskInProgress(long id, long allocationId, String action, Request request, Status status, String executorNode) {
            this.id = id;
            this.allocationId = allocationId;
            this.action = action;
            this.request = request;
            this.status = status;
            this.executorNode = executorNode;
            // Update parent request for starting tasks with correct parent task ID
            request.setParentTask("cluster", id);
        }

        @SuppressWarnings("unchecked")
        private PersistentTaskInProgress(StreamInput in) throws IOException {
            id = in.readLong();
            allocationId = in.readLong();
            action = in.readString();
            request = (Request) in.readNamedWriteable(PersistentActionRequest.class);
            status = in.readOptionalNamedWriteable(Task.Status.class);
            executorNode = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(id);
            out.writeLong(allocationId);
            out.writeString(action);
            out.writeNamedWriteable(request);
            out.writeOptionalNamedWriteable(status);
            out.writeOptionalString(executorNode);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PersistentTaskInProgress<?> that = (PersistentTaskInProgress<?>) o;
            return id == that.id &&
                    allocationId == that.allocationId &&
                    Objects.equals(action, that.action) &&
                    Objects.equals(request, that.request) &&
                    Objects.equals(status, that.status) &&
                    Objects.equals(executorNode, that.executorNode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, allocationId, action, request, status, executorNode);
        }

        public long getId() {
            return id;
        }

        public long getAllocationId() {
            return allocationId;
        }

        public String getAction() {
            return action;
        }

        public Request getRequest() {
            return request;
        }

        @Nullable
        public String getExecutorNode() {
            return executorNode;
        }

        @Nullable
        public Status getStatus() {
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("uuid", id);
                builder.field("action", action);
                builder.field("request");
                request.toXContent(builder, params);
                if (status != null) {
                    builder.field("status", status, params);
                }
                builder.field("executor_node", executorNode);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    public PersistentTasksInProgress(StreamInput in) throws IOException {
        currentId = in.readLong();
        tasks = in.readMap(StreamInput::readLong, PersistentTaskInProgress::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(currentId);
        out.writeMap(tasks, StreamOutput::writeLong, (stream, value) -> {
            value.writeTo(stream);
        });
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    public long getCurrentId() {
        return currentId;
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("current_id", currentId);
        builder.startArray("running_tasks");
        for (PersistentTaskInProgress<?> entry : tasks.values()) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

}