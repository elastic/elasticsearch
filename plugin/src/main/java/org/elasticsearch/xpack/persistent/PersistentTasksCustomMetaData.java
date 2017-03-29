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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
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
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A cluster state record that contains a list of all running persistent tasks
 */
public final class PersistentTasksCustomMetaData extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {
    public static final String TYPE = "persistent_tasks";

    private static final String API_CONTEXT = MetaData.XContentContext.API.toString();

    // TODO: Implement custom Diff for tasks
    private final Map<Long, PersistentTask<?>> tasks;

    private final long currentId;

    public PersistentTasksCustomMetaData(long currentId, Map<Long, PersistentTask<?>> tasks) {
        this.currentId = currentId;
        this.tasks = tasks;
    }

    private static final ObjectParser<Builder, Void> PERSISTENT_TASKS_PARSER = new ObjectParser<>(TYPE, Builder::new);

    private static final ObjectParser<TaskBuilder<PersistentTaskRequest>, Void> PERSISTENT_TASK_PARSER =
            new ObjectParser<>("tasks", TaskBuilder::new);

    public static final ConstructingObjectParser<Assignment, Void> ASSIGNMENT_PARSER =
            new ConstructingObjectParser<>("assignment", objects -> new Assignment((String) objects[0], (String) objects[1]));

    private static final NamedObjectParser<PersistentTaskRequest, Void> REQUEST_PARSER =
            (XContentParser p, Void c, String name) -> p.namedObject(PersistentTaskRequest.class, name, null);
    private static final NamedObjectParser<Status, Void> STATUS_PARSER =
            (XContentParser p, Void c, String name) -> p.namedObject(Status.class, name, null);

    static {
        // Tasks parser initialization
        PERSISTENT_TASKS_PARSER.declareLong(Builder::setCurrentId, new ParseField("current_id"));
        PERSISTENT_TASKS_PARSER.declareObjectArray(Builder::setTasks, PERSISTENT_TASK_PARSER, new ParseField("tasks"));

        // Assignment parser
        ASSIGNMENT_PARSER.declareStringOrNull(constructorArg(), new ParseField("executor_node"));
        ASSIGNMENT_PARSER.declareStringOrNull(constructorArg(), new ParseField("explanation"));

        // Task parser initialization
        PERSISTENT_TASK_PARSER.declareLong(TaskBuilder::setId, new ParseField("id"));
        PERSISTENT_TASK_PARSER.declareString(TaskBuilder::setTaskName, new ParseField("name"));
        PERSISTENT_TASK_PARSER.declareLong(TaskBuilder::setAllocationId, new ParseField("allocation_id"));
        PERSISTENT_TASK_PARSER.declareNamedObjects(
                (TaskBuilder<PersistentTaskRequest> taskBuilder, List<PersistentTaskRequest> objects) -> {
                    if (objects.size() != 1) {
                        throw new IllegalArgumentException("only one request per task is allowed");
                    }
                    taskBuilder.setRequest(objects.get(0));
                }, REQUEST_PARSER, new ParseField("request"));

        PERSISTENT_TASK_PARSER.declareNamedObjects(
                (TaskBuilder<PersistentTaskRequest> taskBuilder, List<Status> objects) -> {
                    if (objects.size() != 1) {
                        throw new IllegalArgumentException("only one status per task is allowed");
                    }
                    taskBuilder.setStatus(objects.get(0));
                }, STATUS_PARSER, new ParseField("status"));


        PERSISTENT_TASK_PARSER.declareObject(TaskBuilder::setAssignment, ASSIGNMENT_PARSER, new ParseField("assignment"));
        PERSISTENT_TASK_PARSER.declareLong(TaskBuilder::setAllocationIdOnLastStatusUpdate,
                new ParseField("allocation_id_on_last_status_update"));
    }

    public Collection<PersistentTask<?>> tasks() {
        return this.tasks.values();
    }

    public Map<Long, PersistentTask<?>> taskMap() {
        return this.tasks;
    }

    public PersistentTask<?> getTask(long id) {
        return this.tasks.get(id);
    }

    public Collection<PersistentTask<?>> findTasks(String taskName, Predicate<PersistentTask<?>> predicate) {
        return this.tasks().stream()
                .filter(p -> taskName.equals(p.getTaskName()))
                .filter(predicate)
                .collect(Collectors.toList());
    }

    public boolean tasksExist(String taskName, Predicate<PersistentTask<?>> predicate) {
        return this.tasks().stream()
                .filter(p -> taskName.equals(p.getTaskName()))
                .anyMatch(predicate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentTasksCustomMetaData that = (PersistentTasksCustomMetaData) o;
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

    public long getNumberOfTasksOnNode(String nodeId, String taskName) {
        return tasks.values().stream().filter(
                task -> taskName.equals(task.taskName) && nodeId.equals(task.assignment.executorNode)).count();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_5_4_0_UNRELEASED;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    public static PersistentTasksCustomMetaData fromXContent(XContentParser parser) throws IOException {
        return PERSISTENT_TASKS_PARSER.parse(parser, null).build();
    }

    public static class Assignment {
        @Nullable
        private final String executorNode;
        private final String explanation;

        public Assignment(String executorNode, String explanation) {
            this.executorNode = executorNode;
            assert explanation != null;
            this.explanation = explanation;
        }

        @Nullable
        public String getExecutorNode() {
            return executorNode;
        }

        public String getExplanation() {
            return explanation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Assignment that = (Assignment) o;
            return Objects.equals(executorNode, that.executorNode) &&
                    Objects.equals(explanation, that.explanation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executorNode, explanation);
        }

        public boolean isAssigned() {
            return executorNode != null;
        }

        @Override
        public String toString() {
            return "node: [" + executorNode + "], explanation: [" + explanation +"]";
        }
    }

    public static final Assignment INITIAL_ASSIGNMENT = new Assignment(null, "waiting for initial assignment");

    public static final Assignment FINISHED_TASK_ASSIGNMENT = new Assignment(null, "task has finished");

    /**
     * A record that represents a single running persistent task
     */
    public static class PersistentTask<Request extends PersistentTaskRequest> implements Writeable, ToXContent {
        private final long id;
        private final long allocationId;
        private final String taskName;
        private final Request request;
        @Nullable
        private final Status status;
        private final Assignment assignment;
        @Nullable
        private final Long allocationIdOnLastStatusUpdate;


        public PersistentTask(long id, String taskName, Request request, Assignment assignment) {
            this(id, 0L, taskName, request, null, assignment, null);
        }

        public PersistentTask(PersistentTask<Request> task, Assignment assignment) {
            this(task.id, task.allocationId + 1L, task.taskName, task.request, task.status,
                    assignment, task.allocationId);
        }

        public PersistentTask(PersistentTask<Request> task, Status status) {
            this(task.id, task.allocationId, task.taskName, task.request, status,
                    task.assignment, task.allocationId);
        }

        private PersistentTask(long id, long allocationId, String taskName, Request request,
                               Status status, Assignment assignment, Long allocationIdOnLastStatusUpdate) {
            this.id = id;
            this.allocationId = allocationId;
            this.taskName = taskName;
            this.request = request;
            this.status = status;
            this.assignment = assignment;
            this.allocationIdOnLastStatusUpdate = allocationIdOnLastStatusUpdate;
            // Update parent request for starting tasks with correct parent task ID
            request.setParentTask("cluster", id);
        }

        @SuppressWarnings("unchecked")
        private PersistentTask(StreamInput in) throws IOException {
            id = in.readLong();
            allocationId = in.readLong();
            taskName = in.readString();
            request = (Request) in.readNamedWriteable(PersistentTaskRequest.class);
            status = in.readOptionalNamedWriteable(Task.Status.class);
            assignment = new Assignment(in.readOptionalString(), in.readString());
            allocationIdOnLastStatusUpdate = in.readOptionalLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(id);
            out.writeLong(allocationId);
            out.writeString(taskName);
            out.writeNamedWriteable(request);
            out.writeOptionalNamedWriteable(status);
            out.writeOptionalString(assignment.executorNode);
            out.writeString(assignment.explanation);
            out.writeOptionalLong(allocationIdOnLastStatusUpdate);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PersistentTask<?> that = (PersistentTask<?>) o;
            return id == that.id &&
                    allocationId == that.allocationId &&
                    Objects.equals(taskName, that.taskName) &&
                    Objects.equals(request, that.request) &&
                    Objects.equals(status, that.status) &&
                    Objects.equals(assignment, that.assignment) &&
                    Objects.equals(allocationIdOnLastStatusUpdate, that.allocationIdOnLastStatusUpdate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, allocationId, taskName, request, status, assignment,
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

        public String getTaskName() {
            return taskName;
        }

        public Request getRequest() {
            return request;
        }

        @Nullable
        public String getExecutorNode() {
            return assignment.executorNode;
        }

        public Assignment getAssignment() {
            return assignment;
        }

        public boolean isAssigned() {
            return assignment.isAssigned();
        }

        /**
         * Returns true if the tasks is not stopped and unassigned or assigned to a non-existing node.
         */
        public boolean needsReassignment(DiscoveryNodes nodes) {
            return (assignment.isAssigned() == false || nodes.nodeExists(assignment.getExecutorNode()) == false);
        }

        @Nullable
        public Status getStatus() {
            return status;
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
                builder.field("name", taskName);
                builder.startObject("request");
                {
                    builder.field(request.getWriteableName(), request, params);
                }
                builder.endObject();
                if (status != null) {
                    builder.startObject("status");
                    {
                        builder.field(status.getWriteableName(), status, params);
                    }
                    builder.endObject();
                }

                if (API_CONTEXT.equals(params.param(MetaData.CONTEXT_MODE_PARAM, API_CONTEXT))) {
                    // These are transient values that shouldn't be persisted to gateway cluster state or snapshot
                    builder.field("allocation_id", allocationId);
                    builder.startObject("assignment");
                    {
                        builder.field("executor_node", assignment.executorNode);
                        builder.field("explanation", assignment.explanation);
                    }
                    builder.endObject();
                    if (allocationIdOnLastStatusUpdate != null) {
                        builder.field("allocation_id_on_last_status_update", allocationIdOnLastStatusUpdate);
                    }
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }

    private static class TaskBuilder<Request extends PersistentTaskRequest> {
        private long id;
        private long allocationId;
        private String taskName;
        private Request request;
        private Status status;
        private Assignment assignment = INITIAL_ASSIGNMENT;
        private Long allocationIdOnLastStatusUpdate;

        public TaskBuilder<Request> setId(long id) {
            this.id = id;
            return this;
        }

        public TaskBuilder<Request> setAllocationId(long allocationId) {
            this.allocationId = allocationId;
            return this;
        }

        public TaskBuilder<Request> setTaskName(String taskName) {
            this.taskName = taskName;
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


        public TaskBuilder<Request> setAssignment(Assignment assignment) {
            this.assignment = assignment;
            return this;
        }

        public TaskBuilder<Request> setAllocationIdOnLastStatusUpdate(Long allocationIdOnLastStatusUpdate) {
            this.allocationIdOnLastStatusUpdate = allocationIdOnLastStatusUpdate;
            return this;
        }

        public PersistentTask<Request> build() {
            return new PersistentTask<>(id, allocationId, taskName, request, status,
                    assignment, allocationIdOnLastStatusUpdate);
        }
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    public PersistentTasksCustomMetaData(StreamInput in) throws IOException {
        currentId = in.readLong();
        tasks = in.readMap(StreamInput::readLong, PersistentTask::new);
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
        builder.startArray("tasks");
        for (PersistentTask<?> entry : tasks.values()) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(PersistentTasksCustomMetaData tasks) {
        return new Builder(tasks);
    }

    public static class Builder {
        private final Map<Long, PersistentTask<?>> tasks = new HashMap<>();
        private long currentId;
        private boolean changed;

        public Builder() {
        }

        public Builder(PersistentTasksCustomMetaData tasksInProgress) {
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

        private <Request extends PersistentTaskRequest> Builder setTasks(List<TaskBuilder<Request>> tasks) {
            for (TaskBuilder builder : tasks) {
                PersistentTask<?> task = builder.build();
                this.tasks.put(task.getId(), task);
            }
            return this;
        }

        /**
         * Adds a new task to the builder
         * <p>
         * After the task is added its id can be found by calling {{@link #getCurrentId()}} method.
         */
        public <Request extends PersistentTaskRequest> Builder addTask(String taskName, Request request, Assignment assignment) {
            changed = true;
            currentId++;
            tasks.put(currentId, new PersistentTask<>(currentId, taskName, request, assignment));
            return this;
        }

        /**
         * Reassigns the task to another node if the task exist
         */
        public Builder reassignTask(long taskId, Assignment assignment) {
            PersistentTask<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTask<>(taskInProgress, assignment));
            }
            return this;
        }

        /**
         * Assigns the task to another node  if the task exist and not currently assigned
         * <p>
         * The operation is only performed if the task is not currently assigned to any nodes.
         */
        @SuppressWarnings("unchecked")
        public <Request extends PersistentTaskRequest> Builder assignTask(long taskId,
                                                                          BiFunction<String, Request, Assignment> executorNodeFunc) {
            PersistentTask<Request> taskInProgress = (PersistentTask<Request>) tasks.get(taskId);
            if (taskInProgress != null && taskInProgress.assignment.isAssigned() == false) { // only assign unassigned tasks
                Assignment assignment = executorNodeFunc.apply(taskInProgress.taskName, taskInProgress.request);
                if (assignment.isAssigned()) {
                    changed = true;
                    tasks.put(taskId, new PersistentTask<>(taskInProgress, assignment));
                }
            }
            return this;
        }


        /**
         * Updates the task status if the task exist
         */
        public Builder updateTaskStatus(long taskId, Status status) {
            PersistentTask<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTask<>(taskInProgress, status));
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
            PersistentTask<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.remove(taskId);
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

        public PersistentTasksCustomMetaData build() {
            return new PersistentTasksCustomMetaData(currentId, Collections.unmodifiableMap(tasks));
        }
    }
}