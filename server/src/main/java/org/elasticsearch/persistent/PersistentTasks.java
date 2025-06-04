/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.persistent.PersistentTasksClusterService.assertAllocationIdsConsistency;
import static org.elasticsearch.persistent.PersistentTasksClusterService.assertUniqueTaskIdForClusterScopeTasks;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The common data structure for persistent tasks, both cluster-scoped and project-scoped.
 * It is meant to be used in places where both type of tasks are processed similarly to achieve better code reuse.
 */
public interface PersistentTasks {

    String API_CONTEXT = Metadata.XContentContext.API.toString();
    Assignment LOST_NODE_ASSIGNMENT = new Assignment(null, "awaiting reassignment after node loss");
    Assignment INITIAL_ASSIGNMENT = new Assignment(null, "waiting for initial assignment");

    /**
     * @return The last allocation id for the tasks.
     */
    long getLastAllocationId();

    /**
     * @return The map of actual tasks keyed by task ID.
     */
    Map<String, PersistentTask<?>> taskMap();

    /**
     * @return A collection of all tasks
     */
    default Collection<PersistentTask<?>> tasks() {
        return taskMap().values();
    }

    /**
     * @param id The task ID
     * @return The task with the specified task ID
     */
    default PersistentTask<?> getTask(String id) {
        return taskMap().get(id);
    }

    /**
     * @param taskName The task name, see also {@link PersistentTasksExecutor#getTaskName()}
     * @param predicate The filter for the matching tasks
     * @return A collection of tasks matching the specified taskName and predicate
     */
    default Collection<PersistentTask<?>> findTasks(String taskName, Predicate<PersistentTask<?>> predicate) {
        return tasks().stream().filter(p -> taskName.equals(p.getTaskName())).filter(predicate).toList();
    }

    /**
     * @param nodeId The node ID where the task runs
     * @param taskName The task name, see also {@link PersistentTasksExecutor#getTaskName()}
     * @return The number of tasks of the taskName currently running on the specified nodeId
     */
    default long getNumberOfTasksOnNode(String nodeId, String taskName) {
        return tasks().stream()
            .filter(task -> taskName.equals(task.getTaskName()) && nodeId.equals(task.getAssignment().getExecutorNode()))
            .count();
    }

    default void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(getLastAllocationId());
        Map<String, PersistentTask<?>> filteredTasks = tasks().stream()
            .filter(t -> t.getParams().supportsVersion(out.getTransportVersion()))
            .collect(Collectors.toMap(PersistentTask::getId, Function.identity()));
        out.writeMap(filteredTasks, StreamOutput::writeWriteable);
    }

    default Iterator<? extends ToXContent> doToXContentChunked() {
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.field("last_allocation_id", getLastAllocationId())),
            ChunkedToXContentHelper.array("tasks", tasks().iterator())
        );
    }

    /**
     * Convert the PersistentTasks object into the corresponding builder
     */
    Builder<?> toBuilder();

    /**
     * Retrieve the persistent tasks for the specified project or the cluster.
     *
     * @param clusterState The cluster state
     * @param projectId The project for which the persistent tasks should be retrieved. {@code null} for cluster-scoped tasks.
     * @return The persistent tasks associated to the specific project or the cluster if the projectId is null.
     */
    @Nullable
    static PersistentTasks getTasks(ClusterState clusterState, @Nullable ProjectId projectId) {
        if (projectId != null) {
            return PersistentTasksCustomMetadata.get(clusterState.metadata().getProject(projectId));
        } else {
            return ClusterPersistentTasksCustomMetadata.get(clusterState.metadata());
        }
    }

    static Stream<Tuple<ProjectId, PersistentTasks>> getAllTasks(final ClusterState currentState) {
        final Stream<Tuple<ProjectId, PersistentTasks>> streamOfProjectTasks = currentState.metadata()
            .projects()
            .keySet()
            .stream()
            .filter(projectId -> getTasks(currentState, projectId) != null)
            .map(projectId -> new Tuple<>(projectId, getTasks(currentState, projectId)));

        final var clusterTasks = getTasks(currentState, null);
        if (clusterTasks == null) {
            return streamOfProjectTasks;
        } else {
            return Stream.concat(Stream.of(new Tuple<>(null, clusterTasks)), streamOfProjectTasks);
        }
    }

    static String taskTypeString(@Nullable ProjectId projectId) {
        return taskTypeString(projectId == null ? null : projectId.id());
    }

    static String taskTypeString(@Nullable String projectId) {
        return projectId == null ? "cluster" : "project [" + projectId + "]";
    }

    class Parsers {
        public static final ConstructingObjectParser<Assignment, Void> ASSIGNMENT_PARSER = new ConstructingObjectParser<>(
            "assignment",
            objects -> new Assignment((String) objects[0], (String) objects[1])
        );
        static final ObjectParser<TaskBuilder<PersistentTaskParams>, Void> PERSISTENT_TASK_PARSER = new ObjectParser<>(
            "tasks",
            TaskBuilder::new
        );
        private static final ObjectParser.NamedObjectParser<TaskDescriptionBuilder<PersistentTaskParams>, Void> TASK_DESCRIPTION_PARSER;

        static {
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
    }

    /**
     * Private builder used in XContent parser to build task-specific portion (params and state)
     */
    class TaskDescriptionBuilder<Params extends PersistentTaskParams> {

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

    class TaskBuilder<Params extends PersistentTaskParams> {
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

    static long getMaxLastAllocationId(ClusterState clusterState) {
        return getAllTasks(clusterState).map(tuple -> tuple.v2().getLastAllocationId()).max(Long::compare).orElse(0L);
    }

    abstract class Builder<T extends Builder<T>> {
        private final Map<String, PersistentTask<?>> tasks = new HashMap<>();
        private long lastAllocationId;
        private boolean changed;

        protected Builder() {}

        protected Builder(PersistentTasks tasksInProgress) {
            if (tasksInProgress != null) {
                tasks.putAll(tasksInProgress.taskMap());
                lastAllocationId = tasksInProgress.getLastAllocationId();
            } else {
                lastAllocationId = 0;
            }
        }

        public long getLastAllocationId() {
            return lastAllocationId;
        }

        @SuppressWarnings("unchecked")
        protected T setLastAllocationId(long currentId) {
            this.lastAllocationId = currentId;
            return (T) this;
        }

        protected T setLastAllocationId(ClusterState clusterState) {
            return setLastAllocationId(getMaxLastAllocationId(clusterState));
        }

        @SuppressWarnings("unchecked")
        protected <Params extends PersistentTaskParams> T setTasks(List<TaskBuilder<Params>> tasks) {
            for (TaskBuilder<Params> builder : tasks) {
                PersistentTask<?> task = builder.build();
                this.tasks.put(task.getId(), task);
            }
            return (T) this;
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
        @SuppressWarnings("unchecked")
        public <Params extends PersistentTaskParams> T addTask(String taskId, String taskName, Params params, Assignment assignment) {
            changed = true;
            PersistentTask<?> previousTask = tasks.put(
                taskId,
                new PersistentTask<>(taskId, taskName, params, getNextAllocationId(), assignment)
            );
            if (previousTask != null) {
                throw new ResourceAlreadyExistsException("Trying to override task with id {" + taskId + "}");
            }
            return (T) this;
        }

        /**
         * Reassigns the task to another node
         */
        @SuppressWarnings("unchecked")
        public T reassignTask(String taskId, Assignment assignment) {
            PersistentTask<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTask<>(taskInProgress, getNextAllocationId(), assignment));
            } else {
                throw new ResourceNotFoundException("cannot reassign task with id {" + taskId + "}, the task no longer exists");
            }
            return (T) this;
        }

        /**
         * Updates the task state
         */
        @SuppressWarnings("unchecked")
        public T updateTaskState(final String taskId, final PersistentTaskState taskState) {
            PersistentTask<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTask<>(taskInProgress, taskState));
            } else {
                throw new ResourceNotFoundException("cannot update task with id {" + taskId + "}, the task no longer exists");
            }
            return (T) this;
        }

        /**
         * Removes the task
         */
        @SuppressWarnings("unchecked")
        public T removeTask(String taskId) {
            if (tasks.remove(taskId) != null) {
                changed = true;
            } else {
                throw new ResourceNotFoundException("cannot remove task with id {" + taskId + "}, the task no longer exists");
            }
            return (T) this;
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

        Map<String, PersistentTask<?>> getCurrentTasks() {
            return tasks;
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

        public abstract PersistentTasks build();

        public final ClusterState buildAndUpdate(ClusterState currentState, ProjectId projectId) {
            if (isChanged()) {
                final ClusterState updatedClusterState = doBuildAndUpdate(currentState, projectId);
                assert assertAllocationIdsConsistency(updatedClusterState);
                assert assertUniqueTaskIdForClusterScopeTasks(updatedClusterState);
                return updatedClusterState;
            } else {
                return currentState;
            }
        }

        protected abstract ClusterState doBuildAndUpdate(ClusterState currentState, ProjectId projectId);
    }
}
