/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.persistent;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;
import static org.elasticsearch.persistent.PersistentTasks.Parsers.PERSISTENT_TASK_PARSER;

/**
 * A cluster state record that contains a list of all running persistent tasks from a project
 */
@FixForMultiProject(description = "Consider renaming it to ProjectPersistentTasksCustomMetadata")
public final class PersistentTasksCustomMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom>
    implements
        Metadata.ProjectCustom,
        PersistentTasks {

    public static final String TYPE = "persistent_tasks";

    // TODO: Implement custom Diff for tasks
    private final Map<String, PersistentTask<?>> tasks;
    private final long lastAllocationId;

    public PersistentTasksCustomMetadata(long lastAllocationId, Map<String, PersistentTask<?>> tasks) {
        this.lastAllocationId = lastAllocationId;
        this.tasks = tasks;
    }

    private static final ObjectParser<Builder, Void> PERSISTENT_TASKS_PARSER = new ObjectParser<>(TYPE, Builder::new);

    static {
        // Tasks parser initialization
        PERSISTENT_TASKS_PARSER.declareLong(Builder::setLastAllocationId, new ParseField("last_allocation_id"));
        PERSISTENT_TASKS_PARSER.declareObjectArray(Builder::setTasks, PERSISTENT_TASK_PARSER, new ParseField("tasks"));
    }

    @Deprecated(forRemoval = true)
    public static PersistentTasksCustomMetadata getPersistentTasksCustomMetadata(ClusterState clusterState) {
        return get(clusterState.metadata().getProject());
    }

    public static PersistentTasksCustomMetadata get(ProjectMetadata projectMetadata) {
        return projectMetadata.custom(TYPE);
    }

    @Override
    public long getLastAllocationId() {
        return lastAllocationId;
    }

    @Override
    public Map<String, PersistentTask<?>> taskMap() {
        return this.tasks;
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

    @Deprecated(forRemoval = true)
    public static <Params extends PersistentTaskParams> PersistentTask<Params> getTaskWithId(ClusterState clusterState, String taskId) {
        return getTaskWithId(clusterState.metadata().getProject(), taskId);
    }

    @SuppressWarnings("unchecked")
    public static <Params extends PersistentTaskParams> PersistentTask<Params> getTaskWithId(ProjectMetadata project, String taskId) {
        PersistentTasksCustomMetadata tasks = get(project);
        if (tasks != null) {
            return (PersistentTask<Params>) tasks.getTask(taskId);
        }
        return null;
    }

    /**
     * Unassign any persistent tasks executing on nodes that are no longer in
     * the cluster. If the task's assigment has a non-null executor node and that
     * node is no longer in the cluster then the assignment is set to
     * {@link PersistentTasks#LOST_NODE_ASSIGNMENT}
     *
     * @param clusterState The clusterstate
     * @return If no changes the argument {@code clusterState} is returned else
     *          a copy with the modified tasks
     */
    public static ClusterState disassociateDeadNodes(ClusterState clusterState) {
        var updatedClusterState = clusterState;
        updatedClusterState = disassociateDeadNodesForClusterOrSingleProject(updatedClusterState, null);
        for (var projectId : clusterState.metadata().projects().keySet()) {
            updatedClusterState = disassociateDeadNodesForClusterOrSingleProject(updatedClusterState, projectId);
        }
        return updatedClusterState;
    }

    private static ClusterState disassociateDeadNodesForClusterOrSingleProject(ClusterState clusterState, @Nullable ProjectId projectId) {
        final var tasks = PersistentTasks.getTasks(clusterState, projectId);
        if (tasks == null) {
            return clusterState;
        }

        var taskBuilder = tasks.toBuilder().setLastAllocationId(clusterState);
        for (PersistentTask<?> task : tasks.tasks()) {
            if (task.getAssignment().getExecutorNode() != null
                && clusterState.nodes().nodeExists(task.getAssignment().getExecutorNode()) == false) {
                taskBuilder.reassignTask(task.getId(), LOST_NODE_ASSIGNMENT);
            }
        }

        if (taskBuilder.isChanged() == false) {
            return clusterState;
        }

        return taskBuilder.buildAndUpdate(clusterState, projectId);
    }

    @FixForMultiProject(description = "Consider moving it to PersistentTasks")
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

    /**
     * A record that represents a single running persistent task
     */
    @FixForMultiProject(description = "Consider moving it to PersistentTasks")
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

        PersistentTask(
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
        doWriteTo(out);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return doToXContentChunked();
    }

    @Override
    public Builder toBuilder() {
        return builder(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(PersistentTasksCustomMetadata tasks) {
        return new Builder(tasks);
    }

    public static class Builder extends PersistentTasks.Builder<Builder> {

        protected Builder() {
            super();
        }

        protected Builder(PersistentTasks tasksInProgress) {
            super(tasksInProgress);
        }

        @Override
        public PersistentTasksCustomMetadata build() {
            return new PersistentTasksCustomMetadata(getLastAllocationId(), Collections.unmodifiableMap(getCurrentTasks()));
        }

        @Override
        protected ClusterState doBuildAndUpdate(ClusterState currentState, ProjectId projectId) {
            return ClusterState.builder(currentState)
                .putProjectMetadata(
                    ProjectMetadata.builder(currentState.metadata().getProject(projectId))
                        .putCustom(PersistentTasksCustomMetadata.TYPE, build())
                )
                .build();
        }
    }

    /**
     * A helper method for handling wire BWC. An old node sends metadata without the notion of separate
     * cluster and project persistent tasks. The new node needs to separate them and store them
     * in different locations. This method does the split for the old metadata (read as project scoped) from the old node.
     */
    public Tuple<ClusterPersistentTasksCustomMetadata, PersistentTasksCustomMetadata> split() {
        final var clusterTasks = new HashMap<String, PersistentTask<?>>();
        final var projectTasks = new HashMap<String, PersistentTask<?>>();
        for (var entry : tasks.entrySet()) {
            final var task = entry.getValue();
            if (PersistentTasksExecutorRegistry.isClusterScopedTask(task.getTaskName())) {
                clusterTasks.put(entry.getKey(), task);
            } else {
                projectTasks.put(entry.getKey(), task);
            }
        }
        return new Tuple<>(
            new ClusterPersistentTasksCustomMetadata(lastAllocationId, Map.copyOf(clusterTasks)),
            new PersistentTasksCustomMetadata(lastAllocationId, Map.copyOf(projectTasks))
        );
    }

    /**
     * A helper method for handling wire BWC. A new node with separate cluster and project scoped
     * persistent tasks needs to send the metadata an old node. It must combine these persistent tasks
     * and send over as one (use the project-scoped class). This method does the combination.
     */
    @Nullable
    public static PersistentTasksCustomMetadata combine(
        @Nullable ClusterPersistentTasksCustomMetadata clusterTasksMetadata,
        @Nullable PersistentTasksCustomMetadata projectTasksMetadata
    ) {
        if (clusterTasksMetadata == null && projectTasksMetadata == null) {
            return null;
        } else if (clusterTasksMetadata == null) {
            return projectTasksMetadata;
        } else if (projectTasksMetadata == null) {
            return new PersistentTasksCustomMetadata(clusterTasksMetadata.getLastAllocationId(), clusterTasksMetadata.taskMap());
        } else {
            final long allocationId = Math.max(clusterTasksMetadata.getLastAllocationId(), projectTasksMetadata.getLastAllocationId());
            final var allTasks = new HashMap<>(clusterTasksMetadata.taskMap());
            allTasks.putAll(projectTasksMetadata.taskMap());
            final var combinedTasksMetadata = new PersistentTasksCustomMetadata(allocationId, Map.copyOf(allTasks));
            assert assertAllocationIdsConsistencyForOnePersistentTasks(combinedTasksMetadata);
            return combinedTasksMetadata;
        }
    }

    static boolean assertAllocationIdsConsistencyForOnePersistentTasks(PersistentTasks persistentTasks) {
        if (persistentTasks == null) {
            return true;
        }
        final List<Long> allocationIds = getNonZeroAllocationIds(persistentTasks);
        assert allocationIds.size() == Set.copyOf(allocationIds).size()
            : persistentTasks.getClass().getSimpleName() + ": duplicated allocationIds [" + persistentTasks + "]";
        assert persistentTasks.getLastAllocationId() >= allocationIds.stream().max(Long::compare).orElse(0L)
            : persistentTasks.getClass().getSimpleName()
                + ": lastAllocationId is less than one of the allocationId for individual tasks ["
                + persistentTasks
                + "]";
        return true;
    }

    static List<Long> getNonZeroAllocationIds(PersistentTasks persistentTasks) {
        return persistentTasks.tasks()
            .stream()
            .map(PersistentTask::getAllocationId)
            .filter(id -> id != 0L) // filter out 0 since it is used for unassigned tasks (on node restart or restored from snapshot)
            .toList();
    }
}
