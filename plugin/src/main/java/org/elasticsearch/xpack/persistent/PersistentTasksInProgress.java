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

    private static final ObjectParser<Builder, Void> PERSISTENT_TASKS_IN_PROGRESS_PARSER = new ObjectParser<>(TYPE, Builder::new);

    private static final ObjectParser<TaskBuilder<PersistentActionRequest>, Void> PERSISTENT_TASK_IN_PROGRESS_PARSER =
            new ObjectParser<>("running_tasks", TaskBuilder::new);

    public static final ConstructingObjectParser<Assignment, Void> ASSIGNMENT_PARSER =
            new ConstructingObjectParser<>("assignment", objects -> new Assignment((String) objects[0], (String) objects[1]));

    private static final NamedObjectParser<PersistentActionRequest, Void> REQUEST_PARSER =
            (XContentParser p, Void c, String name) -> p.namedObject(PersistentActionRequest.class, name, null);
    private static final NamedObjectParser<Status, Void> STATUS_PARSER =
            (XContentParser p, Void c, String name) -> p.namedObject(Status.class, name, null);

    static {
        // Tasks parser initialization
        PERSISTENT_TASKS_IN_PROGRESS_PARSER.declareLong(Builder::setCurrentId, new ParseField("current_id"));
        PERSISTENT_TASKS_IN_PROGRESS_PARSER.declareObjectArray(Builder::setTasks, PERSISTENT_TASK_IN_PROGRESS_PARSER,
                new ParseField("running_tasks"));


        // Assignment parser
        ASSIGNMENT_PARSER.declareStringOrNull(constructorArg(), new ParseField("executor_node"));
        ASSIGNMENT_PARSER.declareStringOrNull(constructorArg(), new ParseField("explanation"));

        // Task parser initialization
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareLong(TaskBuilder::setId, new ParseField("id"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareString(TaskBuilder::setAction, new ParseField("action"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareLong(TaskBuilder::setAllocationId, new ParseField("allocation_id"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareBoolean(TaskBuilder::setRemoveOnCompletion, new ParseField("remove_on_completion"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareBoolean(TaskBuilder::setStopped, new ParseField("stopped"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareNamedObjects(
                (TaskBuilder<PersistentActionRequest> taskBuilder, List<PersistentActionRequest> objects) -> {
                    if (objects.size() != 1) {
                        throw new IllegalArgumentException("only one action request per task is allowed");
                    }
                    taskBuilder.setRequest(objects.get(0));
                }, REQUEST_PARSER, new ParseField("request"));

        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareNamedObjects(
                (TaskBuilder<PersistentActionRequest> taskBuilder, List<Status> objects) -> {
                    if (objects.size() != 1) {
                        throw new IllegalArgumentException("only one status per task is allowed");
                    }
                    taskBuilder.setStatus(objects.get(0));
                }, STATUS_PARSER, new ParseField("status"));


        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareObject(TaskBuilder::setAssignment, ASSIGNMENT_PARSER, new ParseField("assignment"));
        PERSISTENT_TASK_IN_PROGRESS_PARSER.declareLong(TaskBuilder::setAllocationIdOnLastStatusUpdate,
                new ParseField("allocation_id_on_last_status_update"));
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
        return tasks.values().stream().filter(task -> action.equals(task.action) && nodeId.equals(task.assignment.executorNode)).count();
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
    public static class PersistentTaskInProgress<Request extends PersistentActionRequest> implements Writeable, ToXContent {
        private final long id;
        private final long allocationId;
        private final String action;
        private final Request request;
        private final boolean stopped;
        private final boolean removeOnCompletion;
        @Nullable
        private final Status status;
        private final Assignment assignment;
        @Nullable
        private final Long allocationIdOnLastStatusUpdate;


        public PersistentTaskInProgress(long id, String action, Request request, boolean stopped, boolean removeOnCompletion,
                                        Assignment assignment) {
            this(id, 0L, action, request, stopped, removeOnCompletion, null, assignment, null);
        }

        public PersistentTaskInProgress(PersistentTaskInProgress<Request> task, boolean stopped, Assignment assignment) {
            this(task.id, task.allocationId + 1L, task.action, task.request, stopped, task.removeOnCompletion, task.status,
                    assignment, task.allocationId);
        }

        public PersistentTaskInProgress(PersistentTaskInProgress<Request> task, Status status) {
            this(task.id, task.allocationId, task.action, task.request, task.stopped, task.removeOnCompletion, status,
                    task.assignment, task.allocationId);
        }

        private PersistentTaskInProgress(long id, long allocationId, String action, Request request,
                                         boolean stopped, boolean removeOnCompletion, Status status,
                                         Assignment assignment, Long allocationIdOnLastStatusUpdate) {
            this.id = id;
            this.allocationId = allocationId;
            this.action = action;
            this.request = request;
            this.status = status;
            this.stopped = stopped;
            this.removeOnCompletion = removeOnCompletion;
            this.assignment = assignment;
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
            assignment = new Assignment(in.readOptionalString(), in.readString());
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
            out.writeOptionalString(assignment.executorNode);
            out.writeString(assignment.explanation);
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
                    Objects.equals(assignment, that.assignment) &&
                    Objects.equals(allocationIdOnLastStatusUpdate, that.allocationIdOnLastStatusUpdate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, allocationId, action, request, stopped, removeOnCompletion, status, assignment,
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
            return isStopped() == false && (assignment.isAssigned() == false || nodes.nodeExists(assignment.getExecutorNode()) == false);
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
                builder.field("action", action);
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

        public TaskBuilder<Request> setAssignment(Assignment assignment) {
            this.assignment = assignment;
            return this;
        }

        public TaskBuilder<Request> setAllocationIdOnLastStatusUpdate(Long allocationIdOnLastStatusUpdate) {
            this.allocationIdOnLastStatusUpdate = allocationIdOnLastStatusUpdate;
            return this;
        }

        public PersistentTaskInProgress<Request> build() {
            return new PersistentTaskInProgress<>(id, allocationId, action, request, stopped, removeOnCompletion, status,
                    assignment, allocationIdOnLastStatusUpdate);
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
                                                                         boolean removeOnCompletion, Assignment assignment) {
            changed = true;
            currentId++;
            tasks.put(currentId, new PersistentTaskInProgress<>(currentId, action, request, stopped, removeOnCompletion, assignment));
            return this;
        }

        /**
         * Reassigns the task to another node if the task exist
         */
        public Builder reassignTask(long taskId, Assignment assignment) {
            PersistentTaskInProgress<?> taskInProgress = tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, false, assignment));
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
                                                                            BiFunction<String, Request, Assignment> executorNodeFunc) {
            PersistentTaskInProgress<Request> taskInProgress = (PersistentTaskInProgress<Request>) tasks.get(taskId);
            if (taskInProgress != null && taskInProgress.assignment.isAssigned() == false) { // only assign unassigned tasks
                Assignment assignment = executorNodeFunc.apply(taskInProgress.action, taskInProgress.request);
                if (assignment.isAssigned() || taskInProgress.isStopped()) {
                    changed = true;
                    tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, false, assignment));
                }
            }
            return this;
        }

        /**
         * Reassigns the task to another node if the task exist
         */
        @SuppressWarnings("unchecked")
        public <Request extends PersistentActionRequest> Builder reassignTask(long taskId,
                                                                              BiFunction<String, Request, Assignment> executorNodeFunc) {
            PersistentTaskInProgress<Request> taskInProgress = (PersistentTaskInProgress<Request>) tasks.get(taskId);
            if (taskInProgress != null) {
                changed = true;
                Assignment assignment = executorNodeFunc.apply(taskInProgress.action, taskInProgress.request);
                tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, false, assignment));
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
                    tasks.put(taskId, new PersistentTaskInProgress<>(taskInProgress, true, FINISHED_TASK_ASSIGNMENT));
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