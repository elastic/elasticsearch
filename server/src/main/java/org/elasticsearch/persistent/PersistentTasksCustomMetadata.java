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
import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.Explanation.GENERIC_REASON;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A cluster state record that contains a list of all running persistent tasks
 */
public final class PersistentTasksCustomMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "persistent_tasks";
    private static final String API_CONTEXT = Metadata.XContentContext.API.toString();
    static final Assignment LOST_NODE_ASSIGNMENT = new Assignment(
        null,
        "awaiting reassignment after node loss",
        Explanation.AWAITING_REASSIGNMENT
    );

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

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Assignment, Void> ASSIGNMENT_PARSER = new ConstructingObjectParser<>(
        "assignment",
        a -> new Assignment(
            (String) a[0],
            (String) a[1],
            a[2] == null ? new String[] { GENERIC_REASON.name() } : ((List<String>) a[2]).toArray(String[]::new)
        )
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
        ASSIGNMENT_PARSER.declareStringArray(optionalConstructorArg(), new ParseField("explanation_codes"));

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
     * Explanation why the persistent task has the current assignment.
     * <p>
     * More details can be found in the {@code Assignment.explanationDetails}.
     * Callers might need to interpret an assignment failure to e.g. return a HTTP status code.
     * A string is hard to interpret and therefore this an {@code Explanation} enum is provided.
     */
    public enum Explanation {
        /**
         * Persistent task assignment to this node was successful.
         */
        ASSIGNMENT_SUCCESSFUL,

        /**
         * Persistent task is waiting for initial assignment. This is the default when creating a new task.
         */
        WAITING_FOR_INITIAL_ASSIGNMENT,

        /**
         * Persistent task is awaiting reassignment after node loss.
         */
        AWAITING_REASSIGNMENT,

        /**
         * Persistent task assignments are not allowed due to cluster settings.
         */
        ASSIGNMENTS_NOT_ALLOWED,

        /**
         * Persistent task could not find appropriate nodes.
         */
        NO_NODES_FOUND,

        /**
         * Persistent task assignment successful but source index was removed. Task will fail during node operation.
         */
        SOURCE_INDEX_REMOVED,

        /**
         * Persistent task cannot be assigned during an upgrade.
         */
        AWAITING_UPGRADE,

        /**
         * Persistent task cannot be assigned during a feature reset.
         */
        FEATURE_RESET_IN_PROGRESS,

        /**
         * Persistent task was aborted locally.
         */
        ABORTED_LOCALLY,

        /**
         * Persistent datafeed job task state is not OPENING or OPENED.
         */
        DATAFEED_JOB_STATE_NOT_OPEN,

        /**
         * Persistent datafeed job task is stale.
         */
        DATAFEED_JOB_STALE,

        /**
         * Persistent datafeed job task index not found.
         */
        DATAFEED_INDEX_NOT_FOUND,

        /**
         * Persistent datafeed job task cannot start because resolving the index threw an exception.
         */
        DATAFEED_RESOLVING_INDEX_THREW_EXCEPTION,

        /**
         * Persistent task cannot start because indices do not have all primary shards active yet.
         */
        PRIMARY_SHARDS_NOT_ACTIVE,

        /**
         * Persistent task is awaiting lazy node assignment.
         */
        AWAITING_LAZY_ASSIGNMENT,

        /**
         * Persistent task cannot be started because job memory requirements are stale.
         */
        MEMORY_REQUIREMENTS_STALE,

        /**
         * Persistent task requires a node with a higher config version.
         */
        CONFIG_VERSION_TOO_LOW,

        /**
         * Persistent task cannot be started on a node which is not compatible with jobs of this type.
         */
        NODE_NOT_COMPATIBLE,

        /**
         * Persistent task cannot be started because an error occurred while detecting the load for this node.
         */
        ERROR_DETECTING_LOAD,

        /**
         * Persistent task cannot be started on a node that exceeds the maximum number of jobs allowed in opening state.
         */
        MAX_CONCURRENT_EXECUTIONS_EXCEEDED,

        /**
         * Persistent task cannot be started on a node that is full.
         */
        NODE_FULL,

        /**
         * Persistent task cannot be started on a node that is not providing accurate information to determine its load by memory.
         */
        NODE_MEMORY_LOAD_UNKNOWN,

        /**
         * Persistent task cannot be started ona node that is indicating that it has no native memory for machine learning.
         */
        NO_NATIVE_MEMORY_FOR_ML,

        /**
         * Persistent task cannot be started on a node that has insufficient available memory.
         */
        INSUFFICIENT_MEMORY,

        /**
         * Persistent task is not waiting for node assignment as estimated job size is greater than the largest possible job size.
         */
        LARGEST_POSSIBLE_JOB_SIZE_EXCEEDED,

        /**
         * Persistent task requires a remote connection but the node does not have the remote_cluster_client role.
         */
        REMOTE_NOT_ENABLED,

        /**
         * Persistent task cannot not be assigned because of a generic reason.
         * This is mostly used in testing and for backwards compatibility.
         */
        GENERIC_REASON,
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
        return TransportVersions.MINIMUM_COMPATIBLE;
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
        private final Set<PersistentTasksCustomMetadata.Explanation> explanationCodes = new HashSet<>();

        public Assignment(String executorNode, PersistentTasksCustomMetadata.Explanation explanationCode) {
            this(executorNode, "", explanationCode);
        }

        public Assignment(String executorNode, String explanation, PersistentTasksCustomMetadata.Explanation explanationCode) {
            this(executorNode, explanation, Collections.singleton(explanationCode));
        }

        private Assignment(String executorNode, String explanation, String[] explanationCodes) {
            this(executorNode, explanation, Arrays.stream(explanationCodes).map(Explanation::valueOf).collect(Collectors.toSet()));
        }

        public Assignment(String executorNode, String explanation, Set<PersistentTasksCustomMetadata.Explanation> explanationCodes) {
            this.executorNode = executorNode;
            assert explanation != null;
            this.explanation = explanation;
            assert explanationCodes != null;
            assert explanationCodes.isEmpty() == false;
            this.explanationCodes.addAll(explanationCodes);
        }

        @Nullable
        public String getExecutorNode() {
            return executorNode;
        }

        public Set<PersistentTasksCustomMetadata.Explanation> getExplanationCodes() {
            return explanationCodes;
        }

        public String getExplanationCodesAndExplanation() {
            return String.join("|", explanationCodes.stream().map(Enum::name).toList()) + ", details: " + explanation;
        }

        public String getExplanation() {
            return explanation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Assignment that = (Assignment) o;
            return Objects.equals(executorNode, that.executorNode)
                && Objects.equals(explanation, that.explanation)
                && Objects.equals(explanationCodes, that.explanationCodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executorNode, explanation, explanationCodes);
        }

        public boolean isAssigned() {
            return executorNode != null;
        }

        @Override
        public String toString() {
            return "node: ["
                + executorNode
                + "], explanation: ["
                + explanation
                + "], explanationCodes: ["
                + String.join("|", explanationCodes.stream().map(Enum::name).toList())
                + "]";
        }

    }

    public static final Assignment INITIAL_ASSIGNMENT = new Assignment(
        null,
        "waiting for initial assignment",
        Explanation.WAITING_FOR_INITIAL_ASSIGNMENT
    );

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
            if (in.getTransportVersion().before(TransportVersions.PERSISTENT_TASK_CUSTOM_METADATA_ASSIGNMENT_REASON_ENUM)) {
                assignment = new Assignment(in.readOptionalString(), in.readString(), GENERIC_REASON);
            } else {
                assignment = new Assignment(
                    in.readOptionalString(),
                    in.readString(),
                    in.readCollectionAsSet(nested_in -> nested_in.readEnum(Explanation.class))
                );
            }
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.PERSISTENT_TASK_CUSTOM_METADATA_ASSIGNMENT_REASON_ENUM)) {
                out.writeCollection(assignment.explanationCodes, StreamOutput::writeEnum);
            }
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
                        if (builder.getRestApiVersion() == RestApiVersion.V_7) {
                            builder.field("explanation", assignment.explanation);
                        } else {
                            builder.stringListField("explanation_codes", assignment.explanationCodes.stream().map(Enum::name).toList());
                            builder.field("explanation", assignment.explanation == null ? "" : assignment.explanation);
                        }
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
        out.writeMap(filteredTasks, StreamOutput::writeWriteable);
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
