/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.Task.Status;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.MetaData.ALL_CONTEXTS;

/**
 * A cluster state record that contains a list of all running persistent tasks
 */
public final class PersistentTasksInProgress extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {
    public static final String TYPE = "persistent_tasks";

    private static final String API_CONTEXT = MetaData.XContentContext.API.toString();

    // TODO: Implement custom Diff for tasks
    private final Map<Long, PersistentTaskInProgress<?>> tasks;

    private final long currentId;

    public PersistentTasksInProgress(long currentId, Map<Long, PersistentTaskInProgress<?>> tasks) {
        this.currentId = currentId;
        this.tasks = tasks;
    }

    public static final ObjectParser<Builder, Void> PERSISTENT_TASKS_IN_PROGRESS_PARSER = new ObjectParser<>(TYPE,
            Builder::new);

    public static final ObjectParser<TaskBuilder<PersistentActionRequest>, Void> PERSISTENT_TASK_IN_PROGRESS_PARSER =
            new ObjectParser<>("running_tasks", TaskBuilder::new);

    public static final NamedObjectParser<ActionDescriptionBuilder<PersistentActionRequest>, Void> ACTION_PARSER;

    static {
        // Tasks parser initialization
        PERSISTENT_TASKS_IN_PROGRESS_PARSER.declareLong(Builder::setCurrentId, new ParseField("current_id"));
        PERSISTENT_TASKS_IN_PROGRESS_PARSER.declareObjectArray(Builder::setTasks, PERSISTENT_TASK_IN_PROGRESS_PARSER,
                new ParseField("running_tasks"));

        // Action parser initialization
        ObjectParser<ActionDescriptionBuilder<PersistentActionRequest>, String> parser = new ObjectParser<>("named");
        parser.declareObject(ActionDescriptionBuilder::setRequest,
                (p, c) -> p.namedObject(PersistentActionRequest.class, c, null), new ParseField("request"));
        parser.declareObject(ActionDescriptionBuilder::setStatus,
                (p, c) -> p.namedObject(Status.class, c, null), new ParseField("status"));
        ACTION_PARSER = (XContentParser p, Void c, String name) -> parser.parse(p, new ActionDescriptionBuilder<>(name), name);

        // Task parser initialization
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareLong(TaskBuilder::setId, new ParseField("id"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareLong(TaskBuilder::setAllocationId, new ParseField("allocation_id"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareBoolean(TaskBuilder::setRemoveOnCompletion, new ParseField("remove_on_completion"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareBoolean(TaskBuilder::setStopped, new ParseField("stopped"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareNamedObjects(
                (TaskBuilder<PersistentActionRequest> taskBuilder, List<ActionDescriptionBuilder<PersistentActionRequest>> objects) -> {
                    if (objects.size() != 1) {
                        throw new IllegalArgumentException("only one action description per task is allowed");
                    }
                    ActionDescriptionBuilder<PersistentActionRequest> builder = objects.get(0);
                    taskBuilder.setAction(builder.action);
                    taskBuilder.setRequest(builder.request);
                    taskBuilder.setStatus(builder.status);
                }, ACTION_PARSER, new ParseField("action"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareStringOrNull(TaskBuilder::setExecutorNode, new ParseField("executor_node"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareLong(TaskBuilder::setAllocationIdOnLastStatusUpdate,
                new ParseField("allocation_id_on_last_status_update"));
    }

    /**
     * Private builder used in XContent parser
     */
    private static class ActionDescriptionBuilder<Request extends PersistentActionRequest> {
        private final String action;
        private Request request;
        private Status status;

        private ActionDescriptionBuilder(String action) {
            this.action = action;
        }

        private ActionDescriptionBuilder setRequest(Request request) {
            this.request = request;
            return this;
        }

        private ActionDescriptionBuilder setStatus(Status status) {
            this.status = status;
            return this;
        }
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

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public long getNumberOfTasksOnNode(String nodeId, String action) {
        return tasks.values().stream().filter(task -> action.equals(task.action) && nodeId.equals(task.executorNode)).count();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_5_3_0_UNRELEASED;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    public static PersistentTasksInProgress fromXContent(XContentParser parser) throws IOException {
        return PERSISTENT_TASKS_IN_PROGRESS_PARSER.parse(parser, null).build();
    }

    /**
     * A record that represents a single running persistent task
     */
    public static class PersistentTaskInProgress<Request extends PersistentActionRequest> implements Writeable, ToXContent {
        private final long id;
        private final long allocationId;
        private final String action;
        private final Request request;
        private final boolean stopped;
        private final boolean removeOnCompletion;
        @Nullable
        private final Status status;
        @Nullable
        private final String executorNode;
        @Nullable
        private final Long allocationIdOnLastStatusUpdate;


        public PersistentTaskInProgress(long id, String action, Request request, boolean stopped, boolean removeOnCompletion,
                                        String executorNode) {
            this(id, 0L, action, request, stopped, removeOnCompletion, null, executorNode, null);
        }

        public PersistentTaskInProgress(PersistentTaskInProgress<Request> task, boolean stopped, String newExecutorNode) {
            this(task.id, task.allocationId + 1L, task.action, task.request, stopped, task.removeOnCompletion, task.status,
                    newExecutorNode, task.allocationId);
        }

        public PersistentTaskInProgress(PersistentTaskInProgress<Request> task, Status status) {
            this(task.id, task.allocationId, task.action, task.request, task.stopped, task.removeOnCompletion, status,
                    task.executorNode, task.allocationId);
        }

        private PersistentTaskInProgress(long id, long allocationId, String action, Request request,
                                         boolean stopped, boolean removeOnCompletion, Status status,
                                         String executorNode, Long allocationIdOnLastStatusUpdate) {
            this.id = id;
            this.allocationId = allocationId;
            this.action = action;
            this.request = request;
            this.status = status;
            this.stopped = stopped;
            this.removeOnCompletion = removeOnCompletion;
            this.executorNode = executorNode;
            this.allocationIdOnLastStatusUpdate = allocationIdOnLastStatusUpdate;
            // Update parent request for starting tasks with correct parent task ID
            request.setParentTask("cluster", id);
        }

        @SuppressWarnings("unchecked")
        private PersistentTaskInProgress(StreamInput in) throws IOException {
            id = in.readLong();
            allocationId = in.readLong();
            action = in.readString();
            request = (Request) in.readNamedWriteable(PersistentActionRequest.class);
            stopped = in.readBoolean();
            removeOnCompletion = in.readBoolean();
            status = in.readOptionalNamedWriteable(Task.Status.class);
            executorNode = in.readOptionalString();
            allocationIdOnLastStatusUpdate = in.readOptionalLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(id);
            out.writeLong(allocationId);
            out.writeString(action);
            out.writeNamedWriteable(request);
            out.writeBoolean(stopped);
            out.writeBoolean(removeOnCompletion);
            out.writeOptionalNamedWriteable(status);
            out.writeOptionalString(executorNode);
            out.writeOptionalLong(allocationIdOnLastStatusUpdate);
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
                    stopped == that.stopped &&
                    removeOnCompletion == that.removeOnCompletion &&
                    Objects.equals(status, that.status) &&
                    Objects.equals(executorNode, that.executorNode) &&
                    Objects.equals(allocationIdOnLastStatusUpdate, that.allocationIdOnLastStatusUpdate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, allocationId, action, request, stopped, removeOnCompletion, status, executorNode,
                    allocationIdOnLastStatusUpdate);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
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

        public boolean isStopped() {
            return stopped;
        }

        public boolean shouldRemoveOnCompletion() {
            return removeOnCompletion;
        }

        /**
         * @return Whether the task status isn't stale. When a task gets unassigned from the executor node or assigned
         * to a new executor node and the status hasn't been updated then the task status is stale.
         */
        public boolean isCurrentStatus() {
            return allocationIdOnLastStatusUpdate == allocationId;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("id", id);
                builder.startObject("action");
                {
                    builder.startObject(action);
                    {
                        builder.field("request");
                        request.toXContent(builder, params);
                        if (status != null) {
                            builder.field("status", status, params);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
                if (API_CONTEXT.equals(params.param(MetaData.CONTEXT_MODE_PARAM, API_CONTEXT))) {
                    // These are transient values that shouldn't be persisted to gateway cluster state or snapshot
                    builder.field("allocation_id", allocationId);
                    builder.field("executor_node", executorNode);
                    if (allocationIdOnLastStatusUpdate != null) {
                        builder.field("allocation_id_on_last_status_update", allocationIdOnLastStatusUpdate);
                    }
                }
                builder.field("stopped", stopped);
                builder.field("remove_on_completion", removeOnCompletion);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }

    private static class TaskBuilder<Request extends PersistentActionRequest> {
        private long id;
        private long allocationId;
        private String action;
        private Request request;
        private boolean stopped = true;
        private boolean removeOnCompletion;
        private Status status;
        private String executorNode;
        private Long allocationIdOnLastStatusUpdate;

        public TaskBuilder<Request> setId(long id) {
            this.id = id;
            return this;
        }

        public TaskBuilder<Request> setAllocationId(long allocationId) {
            this.allocationId = allocationId;
            return this;
        }

        public TaskBuilder<Request> setAction(String action) {
            this.action = action;
            return this;
        }

        public TaskBuilder<Request> setRequest(Request request) {
            this.request = request;
            return this;
        }

        public TaskBuilder<Request> setStatus(Status status) {
            this.status = status;
            return this;
        }


        public TaskBuilder<Request> setStopped(boolean stopped) {
            this.stopped = stopped;
            return this;
        }

        public TaskBuilder<Request> setRemoveOnCompletion(boolean removeOnCompletion) {
            this.removeOnCompletion = removeOnCompletion;
            return this;
        }

        public TaskBuilder<Request> setExecutorNode(String executorNode) {
            this.executorNode = executorNode;
            return this;
        }

        public TaskBuilder<Request> setAllocationIdOnLastStatusUpdate(Long allocationIdOnLastStatusUpdate) {
            this.allocationIdOnLastStatusUpdate = allocationIdOnLastStatusUpdate;
            return this;
        }

        public PersistentTaskInProgress<Request> build() {
            return new PersistentTaskInProgress<>(id, allocationId, action, request, stopped, removeOnCompletion, status,
                    executorNode, allocationIdOnLastStatusUpdate);
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

    public static NamedDiff<MetaData.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(MetaData.Custom.class, TYPE, in);
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

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(PersistentTasksInProgress tasks) {
        return new Builder(tasks);
    }

    public static class Builder {
        private final Map<Long, PersistentTaskInProgress<?>> tasks = new HashMap<>();
        private long currentId;
        private boolean changed;

        public Builder() {
        }

        public Builder(PersistentTasksInProgress tasksInProgress) {
            if (tasksInProgress != null) {
                tasks.putAll(tasksInProgress.tasks);
                currentId = tasksInProgress.currentId;
            } else {
                currentId = 0;
            }
        }

        private Builder setCurrentId(long currentId) {
            this.currentId = currentId;
            return this;
        }

        private <Request extends PersistentActionRequest> Builder setTasks(List<TaskBuilder<Request>> tasks) {
            for (TaskBuilder builder : tasks) {
                PersistentTaskInProgress<?> task = builder.build();
                this.tasks.put(task.getId(), task);
            }
            return this;
        }

        /**
         * Adds a new task to the builder
         * <p>
         * After the task is added its id can be found by calling {{@link #getCurrentId()}} method.
         */
        public <Request extends PersistentActionRequest> Builder addTask(String action, Request request, boolean stopped,
                                                                         boolean removeOnCompletion, String executorNode) {
            changed = true;
            currentId++;
            tasks.put(currentId, new PersistentTaskInProgress<>(currentId, action, request, stopped, removeOnCompletion,
                    executorNode));
            return this;
        }

        /**
         * Reassigns the task to another node if the task exist
         */
        public Builder reassignTask(long taskId, String executorNode) {
            PersistentTaskInProgress<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, false, executorNode));
            }
            return this;
        }

        /**
         * Assigns the task to another node  if the task exist and not currently assigned
         * <p>
         * The operation is only performed if the task is not currently assigned to any nodes. To force assignment use
         * {@link #reassignTask(long, BiFunction)} instead
         */
        @SuppressWarnings("unchecked")
        public <Request extends PersistentActionRequest> Builder assignTask(long taskId,
                                                                            BiFunction<String, Request, String> executorNodeFunc) {
            PersistentTaskInProgress<Request> taskInProgress = (PersistentTaskInProgress<Request>) tasks.get(taskId);
            if (taskInProgress != null && taskInProgress.getExecutorNode() == null) { // only assign unassigned tasks
                String executorNode = executorNodeFunc.apply(taskInProgress.action, taskInProgress.request);
                if (executorNode != null || taskInProgress.isStopped()) {
                    changed = true;
                    tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, false, executorNode));
                }
            }
            return this;
        }

        /**
         * Reassigns the task to another node if the task exist
         */
        @SuppressWarnings("unchecked")
        public <Request extends PersistentActionRequest> Builder reassignTask(long taskId,
                                                                              BiFunction<String, Request, String> executorNodeFunc) {
            PersistentTaskInProgress<Request> taskInProgress = (PersistentTaskInProgress<Request>) tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                String executorNode = executorNodeFunc.apply(taskInProgress.action, taskInProgress.request);
                tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, false, executorNode));
            }
            return this;
        }

        /**
         * Updates the task status if the task exist
         */
        public Builder updateTaskStatus(long taskId, Status status) {
            PersistentTaskInProgress<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, status));
            }
            return this;
        }

        /**
         * Removes the task if the task exist
         */
        public Builder removeTask(long taskId) {
            if (tasks.remove(taskId) != null) {
                changed = true;
            }
            return this;
        }

        /**
         * Finishes the task if the task exist.
         *
         * If the task is marked with removeOnCompletion flag, it is removed from the list, otherwise it is stopped.
         */
        public Builder finishTask(long taskId) {
            PersistentTaskInProgress<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                if (taskInProgress.removeOnCompletion) {
                    tasks.remove(taskId);
                } else {
                    tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, true, null));
                }
            }
            return this;
        }

        /**
         * Checks if the task is currently present in the list
         */
        public boolean hasTask(long taskId) {
            return tasks.containsKey(taskId);
        }

        /**
         * Returns the id of the last added task
         */
        public long getCurrentId() {
            return currentId;
        }

        /**
         * Returns true if any the task list was changed since the builder was created
         */
        public boolean isChanged() {
            return changed;
        }

        public PersistentTasksInProgress build() {
            return new PersistentTasksInProgress(currentId, Collections.unmodifiableMap(tasks));
        }
    }
}