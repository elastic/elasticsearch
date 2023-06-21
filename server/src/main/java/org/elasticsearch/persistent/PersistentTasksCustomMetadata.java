/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A cluster state record that contains a list of all running persistent tasks
 */
public final class PersistentTasksCustomMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "persistent_tasks";
    private static final String API_CONTEXT = Metadata.XContentContext.API.toString();
    static final Assignment LOST_NODE_ASSIGNMENT = new Assignment(null, "awaiting reassignment after node loss");

    // TODO: Implement custom Diff for tasks
    private final Map<String, PersistentTask<?>> tasks;
    private final long lastAllocationId;

    public PersistentTasksCustomMetadata(long lastAllocationId, Map<String, PersistentTask<?>> tasks) {
        this.lastAllocationId = lastAllocationId;
        this.tasks = tasks;
    }

    private static final ObjectParser<Builder, Void> PERSISTENT_TASKS_PARSER = new ObjectParser<>(TYPE, Builder::new);

    private static final ObjectParser<TaskBuilder<PersistentTaskParams>, Void> PERSISTENT_TASK_PARSER = new ObjectParser<>(
        "tasks",
        TaskBuilder::new
    );

    public static final ConstructingObjectParser<Assignment, Void> ASSIGNMENT_PARSER = new ConstructingObjectParser<>(
        "assignment",
        objects -> new Assignment((String) objects[0], (String) objects[1])
    );

    private static final NamedObjectParser<TaskDescriptionBuilder<PersistentTaskParams>, Void> TASK_DESCRIPTION_PARSER;

    static {
        // Tasks parser initialization
        PERSISTENT_TASKS_PARSER.declareLong(Builder::setLastAllocationId, new ParseField("last_allocation_id"));
        PERSISTENT_TASKS_PARSER.declareObjectArray(Builder::setTasks, PERSISTENT_TASK_PARSER, new ParseField("tasks"));

        // Task description parser initialization
        ObjectParser<TaskDescriptionBuilder<PersistentTaskParams>, String> parser = new ObjectParser<>("named");
        parser.declareObject(
            TaskDescriptionBuilder::setParams,
            (p, c) -> p.namedObject(PersistentTaskParams.class, c, null),
            new ParseField("params")
        );
        parser.declareObject(
            TaskDescriptionBuilder::setState,
            (p, c) -> p.namedObject(PersistentTaskState.class, c, null),
            new ParseField("state", "status")
        );
        TASK_DESCRIPTION_PARSER = (XContentParser p, Void c, String name) -> parser.parse(p, new TaskDescriptionBuilder<>(name), name);

        // Assignment parser
        ASSIGNMENT_PARSER.declareStringOrNull(constructorArg(), new ParseField("executor_node"));
        ASSIGNMENT_PARSER.declareStringOrNull(constructorArg(), new ParseField("explanation"));

        // Task parser initialization
        PERSISTENT_TASK_PARSER.declareString(TaskBuilder::setId, new ParseField("id"));
        PERSISTENT_TASK_PARSER.declareString(TaskBuilder::setTaskName, new ParseField("name"));
        PERSISTENT_TASK_PARSER.declareLong(TaskBuilder::setAllocationId, new ParseField("allocation_id"));

        PERSISTENT_TASK_PARSER.declareNamedObjects(
            (TaskBuilder<PersistentTaskParams> taskBuilder, List<TaskDescriptionBuilder<PersistentTaskParams>> objects) -> {
                if (objects.size() != 1) {
                    throw new IllegalArgumentException("only one task description per task is allowed");
                }
                TaskDescriptionBuilder<PersistentTaskParams> builder = objects.get(0);
                taskBuilder.setTaskName(builder.taskName);
                taskBuilder.setParams(builder.params);
                taskBuilder.setState(builder.state);
            },
            TASK_DESCRIPTION_PARSER,
            new ParseField("task")
        );
        PERSISTENT_TASK_PARSER.declareObject(TaskBuilder::setAssignment, ASSIGNMENT_PARSER, new ParseField("assignment"));
        PERSISTENT_TASK_PARSER.declareLong(
            TaskBuilder::setAllocationIdOnLastStatusUpdate,
            new ParseField("allocation_id_on_last_status_update")
        );
    }

    public static PersistentTasksCustomMetadata getPersistentTasksCustomMetadata(ClusterState clusterState) {
        return clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
    }

    /**
     * Private builder used in XContent parser to build task-specific portion (params and state)
     */
    private static class TaskDescriptionBuilder<Params extends PersistentTaskParams> {

        private final String taskName;
        private Params params;
        private PersistentTaskState state;

        private TaskDescriptionBuilder(String taskName) {
            this.taskName = taskName;
        }

        private TaskDescriptionBuilder<Params> setParams(Params params) {
            this.params = params;
            return this;
        }

        private TaskDescriptionBuilder<Params> setState(PersistentTaskState state) {
            this.state = state;
            return this;
        }
    }

    public Collection<PersistentTask<?>> tasks() {
        return this.tasks.values();
    }

    public Map<String, PersistentTask<?>> taskMap() {
        return this.tasks;
    }

    public PersistentTask<?> getTask(String id) {
        return this.tasks.get(id);
    }

    public Collection<PersistentTask<?>> findTasks(String taskName, Predicate<PersistentTask<?>> predicate) {
        return this.tasks().stream().filter(p -> taskName.equals(p.getTaskName())).filter(predicate).toList();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentTasksCustomMetadata that = (PersistentTasksCustomMetadata) o;
        return lastAllocationId == that.lastAllocationId && Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tasks, lastAllocationId);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public long getNumberOfTasksOnNode(String nodeId, String taskName) {
        return tasks.values()
            .stream()
            .filter(task -> taskName.equals(task.taskName) && nodeId.equals(task.assignment.executorNode))
            .count();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.MINIMUM_COMPATIBLE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    public static PersistentTasksCustomMetadata fromXContent(XContentParser parser) {
        return PERSISTENT_TASKS_PARSER.apply(parser, null).build();
    }

    @SuppressWarnings("unchecked")
    public static <Params extends PersistentTaskParams> PersistentTask<Params> getTaskWithId(ClusterState clusterState, String taskId) {
        PersistentTasksCustomMetadata tasks = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasks != null) {
            return (PersistentTask<Params>) tasks.getTask(taskId);
        }
        return null;
    }

    /**
     * Unassign any persistent tasks executing on nodes that are no longer in
     * the cluster. If the task's assigment has a non-null executor node and that
     * node is no longer in the cluster then the assignment is set to
     * {@link #LOST_NODE_ASSIGNMENT}
     *
     * @param clusterState The clusterstate
     * @return If no changes the argument {@code clusterState} is returned else
     *          a copy with the modified tasks
     */
    public static ClusterState disassociateDeadNodes(ClusterState clusterState) {
        PersistentTasksCustomMetadata tasks = getPersistentTasksCustomMetadata(clusterState);
        if (tasks == null) {
            return clusterState;
        }

        PersistentTasksCustomMetadata.Builder taskBuilder = PersistentTasksCustomMetadata.builder(tasks);
        for (PersistentTask<?> task : tasks.tasks()) {
            if (task.getAssignment().getExecutorNode() != null
                && clusterState.nodes().nodeExists(task.getAssignment().getExecutorNode()) == false) {
                taskBuilder.reassignTask(task.getId(), LOST_NODE_ASSIGNMENT);
            }
        }

        if (taskBuilder.isChanged() == false) {
            return clusterState;
        }

        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.putCustom(TYPE, taskBuilder.build());
        return ClusterState.builder(clusterState).metadata(metadataBuilder).build();
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
            return Objects.equals(executorNode, that.executorNode) && Objects.equals(explanation, that.explanation);
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
            return "node: [" + executorNode + "], explanation: [" + explanation + "]";
        }
    }

    public static final Assignment INITIAL_ASSIGNMENT = new Assignment(null, "waiting for initial assignment");

    /**
     * A record that represents a single running persistent task
     */
    public static class PersistentTask<P extends PersistentTaskParams> implements Writeable, ToXContentObject {

        private final String id;
        private final long allocationId;
        private final String taskName;
        private final P params;
        private final @Nullable PersistentTaskState state;
        private final Assignment assignment;
        private final @Nullable Long allocationIdOnLastStatusUpdate;

        public PersistentTask(final String id, final String name, final P params, final long allocationId, final Assignment assignment) {
            this(id, allocationId, name, params, null, assignment, null);
        }

        public PersistentTask(final PersistentTask<P> task, final long allocationId, final Assignment assignment) {
            this(task.id, allocationId, task.taskName, task.params, task.state, assignment, task.allocationId);
        }

        public PersistentTask(final PersistentTask<P> task, final PersistentTaskState state) {
            this(task.id, task.allocationId, task.taskName, task.params, state, task.assignment, task.allocationId);
        }

        private PersistentTask(
            final String id,
            final long allocationId,
            final String name,
            final P params,
            final PersistentTaskState state,
            final Assignment assignment,
            final Long allocationIdOnLastStatusUpdate
        ) {
            this.id = id;
            this.allocationId = allocationId;
            this.taskName = name;
            this.params = params;
            this.state = state;
            this.assignment = assignment;
            this.allocationIdOnLastStatusUpdate = allocationIdOnLastStatusUpdate;
            if (params != null) {
                if (params.getWriteableName().equals(taskName) == false) {
                    throw new IllegalArgumentException(
                        "params have to have the same writeable name as task. params: " + params.getWriteableName() + " task: " + taskName
                    );
                }
            }
            if (state != null) {
                if (state.getWriteableName().equals(taskName) == false) {
                    throw new IllegalArgumentException(
                        "status has to have the same writeable name as task. status: " + state.getWriteableName() + " task: " + taskName
                    );
                }
            }
        }

        @SuppressWarnings("unchecked")
        public PersistentTask(StreamInput in) throws IOException {
            id = in.readString();
            allocationId = in.readLong();
            taskName = in.readString();
            params = (P) in.readNamedWriteable(PersistentTaskParams.class);
            state = in.readOptionalNamedWriteable(PersistentTaskState.class);
            assignment = new Assignment(in.readOptionalString(), in.readString());
            allocationIdOnLastStatusUpdate = in.readOptionalLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeLong(allocationId);
            out.writeString(taskName);
            out.writeNamedWriteable(params);
            out.writeOptionalNamedWriteable(state);
            out.writeOptionalString(assignment.executorNode);
            out.writeString(assignment.explanation);
            out.writeOptionalLong(allocationIdOnLastStatusUpdate);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PersistentTask<?> that = (PersistentTask<?>) o;
            return Objects.equals(id, that.id)
                && allocationId == that.allocationId
                && Objects.equals(taskName, that.taskName)
                && Objects.equals(params, that.params)
                && Objects.equals(state, that.state)
                && Objects.equals(assignment, that.assignment)
                && Objects.equals(allocationIdOnLastStatusUpdate, that.allocationIdOnLastStatusUpdate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, allocationId, taskName, params, state, assignment, allocationIdOnLastStatusUpdate);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public String getId() {
            return id;
        }

        public long getAllocationId() {
            return allocationId;
        }

        public String getTaskName() {
            return taskName;
        }

        @Nullable
        public P getParams() {
            return params;
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

        @Nullable
        public PersistentTaskState getState() {
            return state;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params xParams) throws IOException {
            builder.startObject();
            {
                builder.field("id", id);
                builder.startObject("task");
                {
                    builder.startObject(taskName);
                    {
                        if (params != null) {
                            builder.field("params", params, xParams);
                        }
                        if (state != null) {
                            builder.field("state", state, xParams);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();

                if (API_CONTEXT.equals(xParams.param(Metadata.CONTEXT_MODE_PARAM, API_CONTEXT))) {
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
    }

    private static class TaskBuilder<Params extends PersistentTaskParams> {
        private String id;
        private long allocationId;
        private String taskName;
        private Params params;
        private PersistentTaskState state;
        private Assignment assignment = INITIAL_ASSIGNMENT;
        private Long allocationIdOnLastStatusUpdate;

        public TaskBuilder<Params> setId(String id) {
            this.id = id;
            return this;
        }

        public TaskBuilder<Params> setAllocationId(long allocationId) {
            this.allocationId = allocationId;
            return this;
        }

        public TaskBuilder<Params> setTaskName(String taskName) {
            this.taskName = taskName;
            return this;
        }

        public TaskBuilder<Params> setParams(Params params) {
            this.params = params;
            return this;
        }

        public TaskBuilder<Params> setState(PersistentTaskState state) {
            this.state = state;
            return this;
        }

        public TaskBuilder<Params> setAssignment(Assignment assignment) {
            this.assignment = assignment;
            return this;
        }

        public TaskBuilder<Params> setAllocationIdOnLastStatusUpdate(Long allocationIdOnLastStatusUpdate) {
            this.allocationIdOnLastStatusUpdate = allocationIdOnLastStatusUpdate;
            return this;
        }

        public PersistentTask<Params> build() {
            return new PersistentTask<>(id, allocationId, taskName, params, state, assignment, allocationIdOnLastStatusUpdate);
        }
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    public PersistentTasksCustomMetadata(StreamInput in) throws IOException {
        lastAllocationId = in.readLong();
        tasks = in.readMap(PersistentTask::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(lastAllocationId);
        Map<String, PersistentTask<?>> filteredTasks = tasks.values()
            .stream()
            .filter(t -> VersionedNamedWriteable.shouldSerialize(out, t.getParams()))
            .collect(Collectors.toMap(PersistentTask::getId, Function.identity()));
        out.writeMap(filteredTasks, StreamOutput::writeString, (stream, value) -> value.writeTo(stream));
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.field("last_allocation_id", lastAllocationId)),
            ChunkedToXContentHelper.array("tasks", tasks.values().iterator())
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(PersistentTasksCustomMetadata tasks) {
        return new Builder(tasks);
    }

    public static class Builder {
        private final Map<String, PersistentTask<?>> tasks = new HashMap<>();
        private long lastAllocationId;
        private boolean changed;

        private Builder() {}

        private Builder(PersistentTasksCustomMetadata tasksInProgress) {
            if (tasksInProgress != null) {
                tasks.putAll(tasksInProgress.tasks);
                lastAllocationId = tasksInProgress.lastAllocationId;
            } else {
                lastAllocationId = 0;
            }
        }

        public long getLastAllocationId() {
            return lastAllocationId;
        }

        private Builder setLastAllocationId(long currentId) {
            this.lastAllocationId = currentId;
            return this;
        }

        private <Params extends PersistentTaskParams> Builder setTasks(List<TaskBuilder<Params>> tasks) {
            for (TaskBuilder<Params> builder : tasks) {
                PersistentTask<?> task = builder.build();
                this.tasks.put(task.getId(), task);
            }
            return this;
        }

        private long getNextAllocationId() {
            lastAllocationId++;
            return lastAllocationId;
        }

        /**
         * Adds a new task to the builder
         * <p>
         * After the task is added its id can be found by calling {{@link #getLastAllocationId()}} method.
         */
        public <Params extends PersistentTaskParams> Builder addTask(String taskId, String taskName, Params params, Assignment assignment) {
            changed = true;
            PersistentTask<?> previousTask = tasks.put(
                taskId,
                new PersistentTask<>(taskId, taskName, params, getNextAllocationId(), assignment)
            );
            if (previousTask != null) {
                throw new ResourceAlreadyExistsException("Trying to override task with id {" + taskId + "}");
            }
            return this;
        }

        /**
         * Reassigns the task to another node
         */
        public Builder reassignTask(String taskId, Assignment assignment) {
            PersistentTask<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTask<>(taskInProgress, getNextAllocationId(), assignment));
            } else {
                throw new ResourceNotFoundException("cannot reassign task with id {" + taskId + "}, the task no longer exists");
            }
            return this;
        }

        /**
         * Updates the task state
         */
        public Builder updateTaskState(final String taskId, final PersistentTaskState taskState) {
            PersistentTask<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTask<>(taskInProgress, taskState));
            } else {
                throw new ResourceNotFoundException("cannot update task with id {" + taskId + "}, the task no longer exists");
            }
            return this;
        }

        /**
         * Removes the task
         */
        public Builder removeTask(String taskId) {
            if (tasks.remove(taskId) != null) {
                changed = true;
            } else {
                throw new ResourceNotFoundException("cannot remove task with id {" + taskId + "}, the task no longer exists");
            }
            return this;
        }

        /**
         * Checks if the task is currently present in the list
         */
        public boolean hasTask(String taskId) {
            return tasks.containsKey(taskId);
        }

        /**
         * Checks if the task is currently present in the list and has the right allocation id
         */
        public boolean hasTask(String taskId, long allocationId) {
            PersistentTask<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                return taskInProgress.getAllocationId() == allocationId;
            }
            return false;
        }

        Set<String> getCurrentTaskIds() {
            return tasks.keySet();
        }

        /**
         * Returns true if any the task list was changed since the builder was created
         */
        public boolean isChanged() {
            return changed;
        }

        public PersistentTasksCustomMetadata build() {
            return new PersistentTasksCustomMetadata(lastAllocationId, Collections.unmodifiableMap(tasks));
        }
    }
}
