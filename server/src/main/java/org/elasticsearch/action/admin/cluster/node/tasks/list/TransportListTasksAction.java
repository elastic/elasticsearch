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
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

public class TransportListTasksAction extends TransportTasksAction<Task, ListTasksRequest, ListTasksResponse, TaskInfo> {

    private static final Set<String> RELOCATABLE_ACTIONS = Set.of(ReindexAction.NAME);
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
        if (RELOCATABLE_ACTIONS.stream().noneMatch(request::canMatchAction)) {
            super.doExecute(task, request, listener);
            return;
        }
        // relocatable tasks might've relocated during listing, and depending on list fan-out response timing in realtime, might be missed.
        // example: Tasks are captured from the destination node before the relocation and from the source node after, so we miss both.
        // therefore, we double-list and de-dupe, which will ensure we see a task *at least* once.
        // elsewhere, we also prevent quick back-to-back relocations during shutdown, to make hitting the race twice vanishingly unlikely.
        if (request.getWaitForCompletion()) {
            executeDoubleListWithWaitForCompletion(task, request, listener);
        } else {
            executeDoubleListWithoutWaitForCompletion(task, request, listener);
        }
    }

    /// `WFC=false` path: two quick lists, the second response takes precedence in de-duplication.
    private void executeDoubleListWithoutWaitForCompletion(
        final Task task,
        final ListTasksRequest request,
        final ActionListener<ListTasksResponse> listener
    ) {
        super.doExecute(task, request, listener.delegateFailureAndWrap((l1, firstResponse) -> {
            super.doExecute(task, request, l1.delegateFailureAndWrap((l2, secondResponse) -> {
                l2.onResponse(deduplicateAndMerge(firstResponse, secondResponse));
            }));
        }));
    }

    /// `WFC=true` path: first pass snapshots (`WFC=false`), then second pass does `WFC=true`, then reconcile parents missed due to
    /// relocation by looking them up in the .tasks index.
    private void executeDoubleListWithWaitForCompletion(
        final Task task,
        final ListTasksRequest request,
        final ActionListener<ListTasksResponse> listener
    ) {
        final ListTasksRequest firstPassRequest = copyWithoutWaitForCompletion(request);
        super.doExecute(task, firstPassRequest, listener.delegateFailureAndWrap((l1, firstPass) -> {
            super.doExecute(task, request, l1.delegateFailureAndWrap((l2, secondPass) -> {
                reconcileMissedRelocations(task, firstPass, secondPass, l2);
            }));
        }));
    }

    /// Finds reindex tasks that appeared in the first pass but are missing from the second pass, perhaps because the operation relocated
    /// between the two passes. Looks missing non-child tasks up in the `.tasks` index for final status, to ensure that all the slices have
    /// been populated (which only happens when the child completes). Collects child tasks from the first pass response (because they do
    /// not get persisted in the `.tasks` index). Merges everything into a single response.
    private void reconcileMissedRelocations(
        final Task thisTask,
        final ListTasksResponse firstPass,
        final ListTasksResponse secondPass,
        final ActionListener<ListTasksResponse> listener
    ) {
        final MissedRelocations missed = findMissedRelocations(firstPass.getTasks(), secondPass.getTasks());

        if (missed.parents.isEmpty()) {
            listener.onResponse(deduplicateAndMerge(firstPass, secondPass));
            return;
        }

        resolveParentsFromIndex(thisTask, missed.parents, listener.delegateFailureAndWrap((l, resolvedParents) -> {
            final List<TaskInfo> extraTasks = new ArrayList<>(resolvedParents.size() + missed.children.size());
            extraTasks.addAll(resolvedParents);
            extraTasks.addAll(missed.children);
            final ListTasksResponse extraResponse = new ListTasksResponse(
                extraTasks,
                firstPass.getTaskFailures(),
                firstPass.getNodeFailures()
            );
            l.onResponse(deduplicateAndMerge(extraResponse, secondPass));
        }));
    }

    record MissedRelocations(List<TaskInfo> parents, List<TaskInfo> children) {
        MissedRelocations {
            Objects.requireNonNull(parents);
            Objects.requireNonNull(children);
        }
    }

    /// Finds reindex parent tasks present in the first pass but absent from the second pass (i.e. relocated between the two lists),
    /// together with their child tasks from the first pass.
    static MissedRelocations findMissedRelocations(final List<TaskInfo> firstPass, final List<TaskInfo> secondPass) {
        // collect all the original taskIDs that are present in second listing since those weren't missed and we can ignore those
        Set<TaskId> secondPassOriginalTaskIds = secondPass.stream().map(TaskInfo::originalTaskId).collect(toSet());

        // collect relocatable parents from the first listing that aren't in the second listing which we'll need to find.
        // de-dupe by originalTaskId since we can get two instances in the first list and none in the second list, so prefer the newest.
        final Map<TaskId, TaskInfo> missingParentsByOriginalId = new LinkedHashMap<>();
        for (final TaskInfo t : firstPass) {
            if (secondPassOriginalTaskIds.contains(t.originalTaskId()) == false
                && t.parentTaskId().isSet() == false
                && RELOCATABLE_ACTIONS.stream().anyMatch(a -> a.equals(t.action()))) {
                missingParentsByOriginalId.merge(t.originalTaskId(), t, TransportListTasksAction::preferNewer);
            }
        }

        if (missingParentsByOriginalId.isEmpty()) { // nothing missed
            return new MissedRelocations(List.of(), List.of());
        }

        // collect missing children, who we'll just have to include with stale information since children aren't persisted in `.tasks`,
        // so we can't look up the latest values.
        Set<TaskId> missingParentTaskIds = missingParentsByOriginalId.values().stream().map(TaskInfo::taskId).collect(toSet());
        List<TaskInfo> missingChildren = firstPass.stream().filter(t -> missingParentTaskIds.contains(t.parentTaskId())).toList();

        return new MissedRelocations(List.copyOf(missingParentsByOriginalId.values()), missingChildren);
    }

    /// Look up all missing parent tasks in the .tasks index via a single multi-get, falling back to the firstPass TaskInfo on any failure.
    private void resolveParentsFromIndex(
        final Task thisTask,
        final List<TaskInfo> missingParents,
        final ActionListener<List<TaskInfo>> listener
    ) {
        final MultiGetRequest mget = new MultiGetRequest();
        for (final TaskInfo missingParent : missingParents) {
            mget.add(TaskResultsService.TASK_INDEX, missingParent.taskId().toString());
        }
        mget.setParentTask(clusterService.localNode().getId(), thisTask.getId());

        client.multiGet(mget, listener.delegateFailureAndWrap((l, mgetResponse) -> {
            final MultiGetItemResponse[] responses = mgetResponse.getResponses();
            assert responses.length == missingParents.size() : "expected " + missingParents.size() + " responses, got " + responses.length;
            final List<TaskInfo> resolved = new ArrayList<>(missingParents.size());
            for (int i = 0; i < responses.length; i++) {
                final TaskInfo fallback = missingParents.get(i);
                final MultiGetItemResponse item = responses[i];
                if (item.isFailed()) {
                    logger.info("failed to look up relocated task [{}] from .tasks, using stale info", fallback.taskId());
                    resolved.add(fallback);
                } else {
                    final TaskInfo parsed = parseTaskInfoFromIndexResponse(xContentRegistry, item.getResponse()).orElse(null);
                    if (parsed == null) {
                        logger.info("failed to parse relocated task [{}] from .tasks, using stale info", fallback.taskId());
                    }
                    resolved.add(parsed != null ? parsed : fallback);
                }
            }
            l.onResponse(resolved);
        }));
    }

    // visible for testing
    static Optional<TaskInfo> parseTaskInfoFromIndexResponse(final NamedXContentRegistry registry, final GetResponse response) {
        if (response.isExists() == false || response.isSourceEmpty()) {
            return Optional.empty();
        }
        try (
            XContentParser parser = XContentHelper.createParser(
                registry,
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

    /// Deduplicates and merges two list-tasks responses. Second pass takes precedence uniformly.
    /// Within each list, the newer physical task wins (lower `runningTimeNanos`) when multiple physical tasks share an `originalTaskId`.
    /// Task failures whose physical `taskId` matches an `originalTaskId` of a captured task are excluded.
    static ListTasksResponse deduplicateAndMerge(final ListTasksResponse firstPass, final ListTasksResponse secondPass) {
        final Map<TaskId, TaskInfo> tasksByOriginalId = deduplicateTasksOnOriginalTaskId(firstPass.getTasks(), secondPass.getTasks());
        return new ListTasksResponse(
            List.copyOf(tasksByOriginalId.values()),
            deduplicateTaskFailures(tasksByOriginalId, firstPass.getTaskFailures(), secondPass.getTaskFailures()),
            deduplicateNodeFailures(firstPass.getNodeFailures(), secondPass.getNodeFailures())
        );
    }

    /// Finds reindex tasks that appeared in the first pass but are missing from the second pass, perhaps because the operation relocated
    /// between the two passes, and merges the results together.
    static Map<TaskId, TaskInfo> deduplicateTasksOnOriginalTaskId(final List<TaskInfo> firstPass, final List<TaskInfo> secondPass) {
        MissedRelocations missedRelocations = findMissedRelocations(firstPass, secondPass);

        // firstly, collect secondPass tasks, and de-dupe based on newest task,
        // since we could get a collision if list lists non-relocated and relocated.
        // n.b. that children have themselves as originalTaskId.
        final Map<TaskId, TaskInfo> dedupedTasksByOriginalTaskId = new LinkedHashMap<>(secondPass.size());
        for (final TaskInfo t : secondPass) {
            dedupedTasksByOriginalTaskId.merge(t.originalTaskId(), t, TransportListTasksAction::preferNewer);
        }

        // Add in firstPass tasks that were missing, both parents and children:
        missedRelocations.parents().forEach(t -> dedupedTasksByOriginalTaskId.put(t.originalTaskId(), t));
        missedRelocations.children().forEach(t -> dedupedTasksByOriginalTaskId.put(t.originalTaskId(), t));
        return Collections.unmodifiableMap(dedupedTasksByOriginalTaskId);
    }

    /// Deduplicates task failures. Second pass wins. Excludes failures whose physical `taskId` is an `originalTaskId` of a captured task.
    /// N.B. If we captured the non-relocated task (`taskId==originalTaskId`), and the failure is for the relocated physical task,
    /// we can't connect them because `TaskOperationFailure` doesn't carry the `originalTaskId`.
    static List<TaskOperationFailure> deduplicateTaskFailures(
        final Map<TaskId, TaskInfo> tasksByOriginalId,
        final List<TaskOperationFailure> firstPass,
        final List<TaskOperationFailure> secondPass
    ) {
        final Map<String, TaskOperationFailure> taskFailures = new LinkedHashMap<>();
        for (final TaskOperationFailure f : secondPass) {
            final TaskId failureTaskId = new TaskId(f.getNodeId(), f.getTaskId());
            if (tasksByOriginalId.containsKey(failureTaskId) == false) {
                taskFailures.putIfAbsent(failureTaskId.toString(), f);
            }
        }
        for (final TaskOperationFailure f : firstPass) {
            final TaskId failureTaskId = new TaskId(f.getNodeId(), f.getTaskId());
            if (tasksByOriginalId.containsKey(failureTaskId) == false) {
                taskFailures.putIfAbsent(failureTaskId.toString(), f);
            }
        }
        return List.copyOf(taskFailures.values());
    }

    /// Deduplicates node failures by nodeId (or message for non-FailedNodeException). Second pass wins.
    static List<ElasticsearchException> deduplicateNodeFailures(
        final List<ElasticsearchException> firstPass,
        final List<ElasticsearchException> secondPass
    ) {
        final Map<String, ElasticsearchException> nodeFailures = new LinkedHashMap<>();
        for (final ElasticsearchException f : secondPass) {
            final String key = f instanceof FailedNodeException fne ? fne.nodeId() : f.getMessage();
            nodeFailures.putIfAbsent(key, f);
        }
        for (final ElasticsearchException f : firstPass) {
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
        return new ListTasksRequest().copyFieldsFrom(request).setWaitForCompletion(false);
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
