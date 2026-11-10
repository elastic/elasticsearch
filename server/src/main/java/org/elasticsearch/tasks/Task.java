/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Current task information
 */
public class Task {

    /**
     * The request header to mark tasks with specific ids
     */
    public static final String X_OPAQUE_ID_HTTP_HEADER = "X-Opaque-Id";

    /**
     * A request header that indicates the origin of the request from Elastic stack. The value will stored in ThreadContext
     * and emitted to ES logs
     */
    public static final String X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER = "X-elastic-product-origin";

    public static final String X_ELASTIC_PROJECT_ID_HTTP_HEADER = "X-Elastic-Project-Id";

    /**
     * The W3C {@code traceparent} request header. It always identifies the span that is current in the thread context: before a local
     * root span is started it holds the remote parent (if any), afterwards the id of the local span. Being a request header it is
     * propagated to other nodes. We also parse {@link #TRACE_ID} from the incoming HTTP request's header.
     * https://www.w3.org/TR/trace-context-1/#traceparent-header
     */
    public static final String TRACE_PARENT_HTTP_HEADER = "traceparent";

    /**
     * Parsed part of traceparent. It is stored in thread context and emitted in logs.
     * Has to be declared as a header copied over for tasks.
     */
    public static final String TRACE_ID = "trace.id";

    /**
     * Optional request header carrying vendor-specific trace information. Always kept in sync with {@link #TRACE_PARENT_HTTP_HEADER}.
     * https://www.w3.org/TR/trace-context-1/#tracestate-header
     */
    public static final String TRACE_STATE = "tracestate";

    /**
     * Optional transient header allowing to override the start time of the local root span. Ignored for any other span.
     */
    public static final String LOCAL_TRACE_START_TIME = "trace.starttime";

    /**
     * Transient header holding the tracing context of the current span, used to parent local spans. Unlike other transient headers it
     * can be overwritten, see {@link org.elasticsearch.common.util.concurrent.ThreadContext#putTransientAllowOverwrite}.
     */
    public static final String APM_TRACE_CONTEXT = "apm.local.context";

    public static final Set<String> HEADERS_TO_COPY = Set.of(
        X_OPAQUE_ID_HTTP_HEADER,
        TRACE_PARENT_HTTP_HEADER,
        TRACE_STATE,
        TRACE_ID,
        X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER,
        X_ELASTIC_PROJECT_ID_HTTP_HEADER
    );

    private static final VarHandle SPAN;

    static {
        try {
            SPAN = MethodHandles.lookup().findVarHandle(Task.class, "span", Span.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * The span that traces this task, a no-op span unless the task is traced. It is started by the {@link TaskManager} when the task is
     * registered and ended when the task is unregistered, which may happen on a different thread. It is therefore handed over by way of
     * the task, and it is not made current in any thread context.
     */
    @SuppressWarnings("unused")
    private volatile Span span = Span.noop();

    private final long id;

    private final String type;

    private final String action;

    private final String description;

    private final TaskId parentTask;

    private final Map<String, String> headers;

    /**
     * The task's start time as a wall clock time since epoch ({@link System#currentTimeMillis()} style).
     */
    protected final long startTime;

    /**
     * The task's start time as a relative time ({@link System#nanoTime()} style).
     */
    private final long startTimeNanos;

    public Task(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        this(id, type, action, description, parentTask, System.currentTimeMillis(), System.nanoTime(), headers);
    }

    public Task(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        long startTime,
        long startTimeNanos,
        Map<String, String> headers
    ) {
        this.id = id;
        this.type = type;
        this.action = action;
        this.description = description;
        this.parentTask = parentTask;
        this.startTime = startTime;
        this.startTimeNanos = startTimeNanos;
        this.headers = headers;
    }

    /**
     * Build a version of the task status you can throw over the wire and back
     * to the user.
     *
     * @param localNodeId
     *            the id of the node this task is running on
     * @param detailed
     *            should the information include detailed, potentially slow to
     *            generate data?
     */
    public final TaskInfo taskInfo(String localNodeId, boolean detailed) {
        String description = null;
        Task.Status status = null;
        if (detailed) {
            description = getDescription();
            status = getStatus();
        }
        return taskInfo(localNodeId, description, status);
    }

    /**
     * Build a proper {@link TaskInfo} for this task.
     */
    protected final TaskInfo taskInfo(String localNodeId, String description, Status status) {
        TaskId taskId = new TaskId(localNodeId, getId());
        Optional<OriginalTaskInfo> originalTaskInfo = getOriginalTaskInfo();
        return new TaskInfo(
            taskId,
            getType(),
            localNodeId,
            getAction(),
            description,
            status,
            startTime,
            System.nanoTime() - startTimeNanos,
            this instanceof CancellableTask,
            this instanceof CancellableTask && ((CancellableTask) this).isCancelled(),
            parentTask,
            headers,
            originalTaskInfo.map(OriginalTaskInfo::originalTaskId).orElse(taskId),
            originalTaskInfo.map(OriginalTaskInfo::originalStartTimeMillis).orElse(startTime)
        );
    }

    /**
     * Returns task id
     */
    public long getId() {
        return id;
    }

    /**
     * Returns task channel type (netty, transport, direct)
     */
    public String getType() {
        return type;
    }

    /**
     * Returns task action
     */
    public String getAction() {
        return action;
    }

    /**
     * Generates task description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the task's start time as a wall clock time since epoch ({@link System#currentTimeMillis()} style).
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the task's start time in nanoseconds ({@link System#nanoTime()} style).
     */
    public long getStartTimeNanos() {
        return startTimeNanos;
    }

    /**
     * Returns id of the parent task or NO_PARENT_ID if the task doesn't have any parent tasks
     */
    public TaskId getParentTaskId() {
        return parentTask;
    }

    /**
     * Build a status for this task or null if this task doesn't have status.
     * Since most tasks don't have status this defaults to returning null. While
     * this can never perform IO it might be a costly operation, requiring
     * collating lists of results, etc. So only use it if you need the value.
     */
    public Status getStatus() {
        return null;
    }

    @Override
    public String toString() {
        return "Task{id="
            + id
            + ", type='"
            + type
            + "', action='"
            + action
            + "', description='"
            + description
            + "', parentTask="
            + parentTask
            + ", startTime="
            + startTime
            + ", headers="
            + headers
            + ", startTimeNanos="
            + startTimeNanos
            + '}';
    }

    /**
     * Report of the internal status of a task. These can vary wildly from task
     * to task because each task is implemented differently but we should try
     * to keep each task consistent from version to version where possible.
     * That means each implementation of {@linkplain Task.Status#toXContent}
     * should avoid making backwards incompatible changes to the rendered
     * result. But if we change the way a request is implemented it might not
     * be possible to preserve backwards compatibility. In that case, we
     * <b>can</b> change this on version upgrade but we should be careful
     * because some statuses (reindex) have become defacto standardized because
     * they are used by systems like Kibana.
     */
    public interface Status extends ToXContentObject, NamedWriteable {}

    /**
     * Returns stored task header associated with the task
     */
    public String getHeader(String header) {
        return headers.get(header);
    }

    @Nullable
    public String getProjectId() {
        return getHeader(X_ELASTIC_PROJECT_ID_HTTP_HEADER);
    }

    public Map<String, String> headers() {
        return headers;
    }

    /**
     * Whether result storage should use create-if-absent semantics instead of unconditional overwrite. Subclasses override this to
     * prevent overwriting a result that was already stored.
     */
    public boolean useCreateSemanticsForResultStorage() {
        return false;
    }

    public TaskResult result(DiscoveryNode node, Exception error) throws IOException {
        return new TaskResult(taskInfo(node.getId(), true), error);
    }

    public TaskResult result(DiscoveryNode node, ActionResponse response) throws IOException {
        if (response instanceof ToXContent) {
            return new TaskResult(taskInfo(node.getId(), true), (ToXContent) response);
        } else {
            throw new IllegalStateException("response has to implement ToXContent to be able to store the results");
        }
    }

    protected record OriginalTaskInfo(TaskId originalTaskId, long originalStartTimeMillis) {

        public OriginalTaskInfo {
            requireNonNull(originalTaskId);
        }
    }

    /// If this task is continuing the work of another task on a node that was shut down, returns basic information about the original task.
    /// Otherwise, returns [Optional#empty()].
    protected Optional<OriginalTaskInfo> getOriginalTaskInfo() {
        return Optional.empty();
    }

    public Optional<TaskId> getOriginalTaskId() {
        return getOriginalTaskInfo().map(OriginalTaskInfo::originalTaskId);
    }

    void setSpan(Span span) {
        SPAN.setVolatile(this, span);
    }

    Span getSpan() {
        return (Span) SPAN.getVolatile(this);
    }

    Span popSpan() {
        return (Span) SPAN.getAndSet(this, Span.noop());
    }
}
