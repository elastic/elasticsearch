/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNullElse;
import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

public class TransportListTasksAction extends TransportTasksAction<Task, ListTasksRequest, ListTasksResponse, TaskInfo> {

    private static final Logger logger = LogManager.getLogger(TransportListTasksAction.class);

    public static final ActionType<ListTasksResponse> TYPE = new ActionType<>("cluster:monitor/tasks/lists");

    public static long waitForCompletionTimeout(TimeValue timeout) {
        if (timeout == null) {
            timeout = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;
        }
        return System.nanoTime() + timeout.nanos();
    }

    private static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = timeValueSeconds(30);

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportListTasksAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            ListTasksRequest::new,
            TaskInfo::from,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.client = new OriginSettingClient(Objects.requireNonNull(client, "client"), TASKS_ORIGIN);
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry, "xContentRegistry");
    }

    @Override
    protected ListTasksResponse newResponse(
        ListTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(CancellableTask actionTask, ListTasksRequest request, Task task, ActionListener<TaskInfo> listener) {
        listener.onResponse(task.taskInfo(clusterService.localNode().getId(), request.getDetailed()));
    }

    @Override
    protected void doExecute(final Task task, final ListTasksRequest request, final ActionListener<ListTasksResponse> listener) {
        assert task instanceof CancellableTask;
        // Double-list is only needed for relocatable tasks (problem description further down in function comment).
        if (!request.canMatchAction(ReindexAction.NAME)) {
            super.doExecute(task, request, listener);
            return;
        }
        // relocatable tasks might've relocated during listing, and depending on list fan-out response timing in realtime, might be missed.
        // example: Tasks are captured from the destination node before the relocation and from the source node after, so we miss both.
        // therefore, we double-list and de-dupe, which will ensure we see a task *at least* once.
        // we also prevent quick back-to-back relocations during shutdown, to make hitting the race twice vanishly unlikely.
        if (request.getWaitForCompletion()) {
            executeDoubleListWithWaitForCompletion(task, request, listener);
        } else {
            executeDoubleListWithoutWaitForCompletion(task, request, listener);
        }
    }

    /** WFC=false path: two quick lists, the second response takes precedence via insertion order + putIfAbsent deduplication. */
    private void executeDoubleListWithoutWaitForCompletion(
        final Task task,
        final ListTasksRequest request,
        final ActionListener<ListTasksResponse> listener
    ) {
        super.doExecute(
            task,
            request,
            listener.delegateFailureAndWrap(
                (l1, firstResponse) -> super.doExecute(task, request, l1.delegateFailureAndWrap((l2, secondResponse) -> {
                    l2.onResponse(deduplicateAndMerge(secondResponse, firstResponse)); // prefer second response
                }))
            )
        );
    }

    /**
     * WFC=true path: snapshot first (WFC=false), then WFC=true list, then reconcile parents missed due to relocation by looking them up
     * in the .tasks index.
     */
    private void executeDoubleListWithWaitForCompletion(
        final Task task,
        final ListTasksRequest request,
        final ActionListener<ListTasksResponse> listener
    ) {
        final ListTasksRequest snapshotRequest = copyWithoutWaitForCompletion(request);
        super.doExecute(
            task,
            snapshotRequest,
            listener.delegateFailureAndWrap(
                (l1, snapshot) -> super.doExecute(
                    task,
                    request,
                    l1.delegateFailureAndWrap((l2, wfcResponse) -> reconcileMissedRelocations(task, snapshot, wfcResponse, l2))
                )
            )
        );
    }

    /**
     * Finds reindex parent tasks that appeared in the snapshot but are missing from the WFC response (relocated between the two lists),
     * looks them up in the .tasks index for final status, and merges everything into a single response.
     */
    private void reconcileMissedRelocations(
        final Task thisTask,
        final ListTasksResponse snapshot,
        final ListTasksResponse wfcResponse,
        final ActionListener<ListTasksResponse> listener
    ) {
        final Set<TaskId> wfcOriginalTaskIds = new HashSet<>();
        for (final TaskInfo t : wfcResponse.getTasks()) {
            wfcOriginalTaskIds.add(t.originalTaskId());
        }

        final List<TaskInfo> missingParents = new ArrayList<>();
        for (final TaskInfo t : snapshot.getTasks()) {
            if (wfcOriginalTaskIds.contains(t.originalTaskId()) == false
                && t.parentTaskId().isSet() == false
                && ReindexAction.NAME.equals(t.action())) {
                missingParents.add(t);
            }
        }

        if (missingParents.isEmpty()) {
            listener.onResponse(deduplicateAndMerge(wfcResponse, snapshot));
            return;
        }

        final Set<TaskId> missingParentTaskIds = new HashSet<>();
        for (final TaskInfo p : missingParents) {
            missingParentTaskIds.add(p.taskId());
        }

        final List<TaskInfo> missingChildren = new ArrayList<>();
        for (final TaskInfo t : snapshot.getTasks()) {
            if (missingParentTaskIds.contains(t.parentTaskId())) {
                missingChildren.add(t);
            }
        }

        resolveParentsFromIndex(thisTask, missingParents, listener.delegateFailureAndWrap((l, resolvedParents) -> {
            final List<TaskInfo> extraTasks = new ArrayList<>(resolvedParents.size() + missingChildren.size());
            extraTasks.addAll(resolvedParents);
            extraTasks.addAll(missingChildren);
            final ListTasksResponse extraResponse = new ListTasksResponse(
                extraTasks,
                snapshot.getTaskFailures(),
                snapshot.getNodeFailures()
            );
            l.onResponse(deduplicateAndMerge(wfcResponse, extraResponse));
        }));
    }

    /**
     * Looks up each missing parent task in the .tasks index in parallel, falling back to the snapshot TaskInfo on any failure.
     */
    private void resolveParentsFromIndex(
        final Task thisTask,
        final List<TaskInfo> missingParents,
        final ActionListener<List<TaskInfo>> listener
    ) {
        final List<TaskInfo> resolved = Collections.synchronizedList(new ArrayList<>(missingParents.size()));
        try (var refs = new RefCountingRunnable(() -> listener.onResponse(resolved))) {
            for (final TaskInfo missingParent : missingParents) {
                final GetRequest get = new GetRequest(TaskResultsService.TASK_INDEX, missingParent.taskId().toString());
                get.setParentTask(clusterService.localNode().getId(), thisTask.getId());
                final Releasable ref = refs.acquire();
                client.get(get, ActionListener.runAfter(ActionListener.wrap(response -> {
                    final TaskInfo resolvedParent = parseTaskInfoFromIndexResponse(response).orElse(null);
                    if (resolvedParent == null) {
                        logger.info("failed to look up relocated task [{}] from .tasks, using snapshot", missingParent.taskId());
                    }
                    resolved.add(resolvedParent != null ? resolvedParent : missingParent);
                }, e -> {
                    logger.info("failed to look up relocated task [{}] from .tasks, using snapshot", missingParent.taskId());
                    resolved.add(missingParent);
                }), ref::close));
            }
        }
    }

    private Optional<TaskInfo> parseTaskInfoFromIndexResponse(final GetResponse response) {
        if (response.isExists() == false || response.isSourceEmpty()) {
            return Optional.empty();
        }
        try (
            XContentParser parser = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                response.getSourceAsBytesRef()
            )
        ) {
            return Optional.of(TaskResult.PARSER.apply(parser, null).getTask());
        } catch (Exception e) {
            logger.warn("failed to parse task result from .tasks index", e);
            return Optional.empty();
        }
    }

    /// Deduplicates and merges two list-tasks responses. Primary takes precedence uniformly.
    /// Within each list, the newer physical task wins (lower runningTimeNanos) when multiple physical tasks share an originalTaskId.
    /// Task failures whose physical taskId matches an originalTaskId of a captured task are excluded.
    static ListTasksResponse deduplicateAndMerge(final ListTasksResponse primary, final ListTasksResponse secondary) {
        final Map<TaskId, TaskInfo> tasksByOriginalId = deduplicateTasks(primary.getTasks(), secondary.getTasks());
        return new ListTasksResponse(
            List.copyOf(tasksByOriginalId.values()),
            deduplicateTaskFailures(tasksByOriginalId, primary.getTaskFailures(), secondary.getTaskFailures()),
            deduplicateNodeFailures(primary.getNodeFailures(), secondary.getNodeFailures())
        );
    }

    /// Deduplicates tasks from two lists by originalTaskId. Primary wins across lists, newer wins within each list
    /// (lower runningTimeNanos = more recently started physical task).
    static Map<TaskId, TaskInfo> deduplicateTasks(final List<TaskInfo> primary, final List<TaskInfo> secondary) {
        final Map<TaskId, TaskInfo> tasksByOriginalId = new LinkedHashMap<>(primary.size() + secondary.size());
        for (final TaskInfo t : primary) {
            tasksByOriginalId.merge(t.originalTaskId(), t, TransportListTasksAction::preferNewer);
        }
        final Map<TaskId, TaskInfo> secondaryBest = new LinkedHashMap<>();
        for (final TaskInfo t : secondary) {
            secondaryBest.merge(t.originalTaskId(), t, TransportListTasksAction::preferNewer);
        }
        for (final var entry : secondaryBest.entrySet()) {
            tasksByOriginalId.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(tasksByOriginalId);
    }

    /// Deduplicates task failures. Primary wins. Excludes failures whose physical taskId is an originalTaskId of a captured task.
    /// Gap: if we captured the non-relocated task (taskId==originalTaskId), and the failure is for the relocated physical task,
    /// we can't connect them because TaskOperationFailure doesn't carry the originalTaskId.
    static List<TaskOperationFailure> deduplicateTaskFailures(
        final Map<TaskId, TaskInfo> tasksByOriginalId,
        final List<TaskOperationFailure> primary,
        final List<TaskOperationFailure> secondary
    ) {
        final Map<String, TaskOperationFailure> taskFailures = new LinkedHashMap<>();
        for (final TaskOperationFailure f : primary) {
            final TaskId failureTaskId = new TaskId(f.getNodeId(), f.getTaskId());
            if (tasksByOriginalId.containsKey(failureTaskId) == false) {
                taskFailures.putIfAbsent(failureTaskId.toString(), f);
            }
        }
        for (final TaskOperationFailure f : secondary) {
            final TaskId failureTaskId = new TaskId(f.getNodeId(), f.getTaskId());
            if (tasksByOriginalId.containsKey(failureTaskId) == false) {
                taskFailures.putIfAbsent(failureTaskId.toString(), f);
            }
        }
        return List.copyOf(taskFailures.values());
    }

    /// Deduplicates node failures by nodeId (or message for non-FailedNodeException). Primary wins.
    static List<ElasticsearchException> deduplicateNodeFailures(
        final List<ElasticsearchException> primary,
        final List<ElasticsearchException> secondary
    ) {
        final Map<String, ElasticsearchException> nodeFailures = new LinkedHashMap<>();
        for (final ElasticsearchException f : primary) {
            final String key = f instanceof FailedNodeException fne ? fne.nodeId() : f.getMessage();
            nodeFailures.putIfAbsent(key, f);
        }
        for (final ElasticsearchException f : secondary) {
            final String key = f instanceof FailedNodeException fne ? fne.nodeId() : f.getMessage();
            nodeFailures.putIfAbsent(key, f);
        }
        return List.copyOf(nodeFailures.values());
    }

    // visible for testing
    static TaskInfo preferNewer(final TaskInfo existing, final TaskInfo candidate) {
        return candidate.runningTimeNanos() < existing.runningTimeNanos() ? candidate : existing;
    }

    /// Make a copy of {@link ListTasksRequest} but with `waitForCompletion=false`. Visible for testing.
    static ListTasksRequest copyWithoutWaitForCompletion(final ListTasksRequest request) {
        ListTasksRequest copy = new ListTasksRequest();
        copy.setActions(request.getActions());
        copy.setNodes(request.getNodes());
        copy.setTargetTaskId(request.getTargetTaskId());
        copy.setTargetParentTaskId(request.getTargetParentTaskId());
        copy.setTimeout(request.getTimeout());
        copy.setDetailed(request.getDetailed());
        copy.setWaitForCompletion(false);
        copy.setDescriptions(request.getDescriptions());
        copy.setParentTask(request.getParentTask());
        return copy;
    }

    @Override
    protected void processTasks(CancellableTask nodeTask, ListTasksRequest request, ActionListener<List<Task>> nodeOperation) {
        if (request.getWaitForCompletion()) {
            final ListenableActionFuture<List<Task>> future = new ListenableActionFuture<>();
            final List<Task> processedTasks = new ArrayList<>();
            final Set<Task> removedTasks = ConcurrentCollections.newConcurrentSet();
            final Set<Task> matchedTasks = ConcurrentCollections.newConcurrentSet();
            final RefCounted removalRefs = AbstractRefCounted.of(() -> {
                matchedTasks.removeAll(removedTasks);
                removedTasks.clear();
                if (matchedTasks.isEmpty()) {
                    future.onResponse(processedTasks);
                }
            });

            final AtomicBoolean collectionComplete = new AtomicBoolean();
            final RemovedTaskListener removedTaskListener = task -> {
                if (collectionComplete.get() == false && removalRefs.tryIncRef()) {
                    removedTasks.add(task);
                    removalRefs.decRef();
                } else {
                    matchedTasks.remove(task);
                    if (matchedTasks.isEmpty()) {
                        future.onResponse(processedTasks);
                    }
                }
            };
            taskManager.registerRemovedTaskListener(removedTaskListener);
            final ActionListener<List<Task>> allMatchedTasksRemovedListener = ActionListener.runBefore(
                nodeOperation,
                () -> taskManager.unregisterRemovedTaskListener(removedTaskListener)
            );
            try {
                for (final var task : processTasks(request)) {
                    if (task.getAction().startsWith(TYPE.name()) == false) {
                        // It doesn't make sense to wait for List Tasks and it can cause an infinite loop of the task waiting
                        // for itself or one of its child tasks
                        matchedTasks.add(task);
                    }
                    processedTasks.add(task);
                }
            } catch (Exception e) {
                allMatchedTasksRemovedListener.onFailure(e);
                return;
            }
            removalRefs.decRef();
            collectionComplete.set(true);

            final var threadPool = clusterService.threadPool();
            future.addListener(
                new ContextPreservingActionListener<>(
                    threadPool.getThreadContext().newRestorableContext(false),
                    allMatchedTasksRemovedListener
                ),
                threadPool.executor(ThreadPool.Names.MANAGEMENT),
                null
            );
            future.addTimeout(
                requireNonNullElse(request.getTimeout(), DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT),
                threadPool,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            nodeTask.addListener(() -> future.onFailure(new TaskCancelledException("task cancelled")));
        } else {
            super.processTasks(nodeTask, request, nodeOperation);
        }
    }
}
