/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class was created to extract out the logic from {@link Node#prepareForClose()} to facilitate testing.
 * <p>
 * Invokes hooks to prepare this node to be closed. This should be called when Elasticsearch receives a request to shut down
 * gracefully from the underlying operating system, before system resources are closed.
 * <p>
 * Note that this class is part of infrastructure to react to signals from the operating system - most graceful shutdown
 * logic should use Node Shutdown, see {@link org.elasticsearch.cluster.metadata.NodesShutdownMetadata}.
 */
public class ShutdownPrepareService {

    public static final String CANNOT_RELOCATE_REINDEX_CANCEL_REASON = "node shutting down";

    private record ShutdownHook(String name, Runnable action) {}

    public static final Setting<TimeValue> MAXIMUM_SHUTDOWN_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "node.maximum_shutdown_grace_period",
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MAXIMUM_REINDEXING_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "node.maximum_reindexing_grace_period",
        TimeValue.timeValueSeconds(10),
        Setting.Property.NodeScope
    );

    /**
     * How long we'll wait for the non-relocatable reindexing tasks to cancel before
     * we proceed with shutdown. This should not be very long because we've already timed out
     * waiting for the tasks to relocate.
     */
    private static final TimeValue REINDEXING_CANCELLATION_TIMEOUT = TimeValue.timeValueSeconds(10);

    private static final Set<String> REINDEX_ACTIONS = Set.of(ReindexAction.NAME);

    private static final Logger logger = LogManager.getLogger(ShutdownPrepareService.class);

    private final TimeValue maxTimeout;
    private final TerminationHandler terminationHandler;
    private final List<ShutdownHook> hooks = new ArrayList<>();
    private volatile boolean isShuttingDown = false;

    /**
     * Creates the service and registers the built-in shutdown hooks.
     *
     * @param actionsToAwait names of the actions whose tasks the node waits for during graceful shutdown; collected from
     *                       {@link ShutdownAwarePlugin#getActionsToAwaitOnShutdown()}
     */
    @SuppressWarnings(value = "this-escape")
    public ShutdownPrepareService(
        Settings settings,
        HttpServerTransport httpServerTransport,
        TransportService transportService,
        TerminationHandler terminationHandler,
        Collection<String> actionsToAwait
    ) {
        this.maxTimeout = MAXIMUM_SHUTDOWN_TIMEOUT_SETTING.get(settings);
        this.terminationHandler = terminationHandler;

        final var reindexTimeout = MAXIMUM_REINDEXING_TIMEOUT_SETTING.get(settings);
        final var awaitedActions = Set.copyOf(actionsToAwait);
        // Tasks of the awaited actions can keep arriving over the transport for as long as the node holds shards. With a termination
        // handler (stateless), the handler moves the shards away, so these arrivals dry up on their own and the tasks can be waited for
        // as they come; the handler holds the node open for the shard migration anyway. Without one (stateful), nothing moves the
        // shards unless the operator did so beforehand, so only the tasks in flight at the start of the shutdown are waited for, or the
        // wait might never end (the grace period is unbounded by default).
        final var awaitedTasksScope = terminationHandler != null ? TaskScope.ALL : TaskScope.INITIAL;
        addShutdownHook("http-server-transport-stop", httpServerTransport::close);
        addShutdownHook(
            "await-tasks-stop",
            () -> awaitTasksComplete(
                maxTimeout,
                new Sleeper(),
                awaitedActions,
                awaitedTasksScope,
                transportService.getTaskManager(),
                null,
                null
            )
        );
        addShutdownHook("reindex-stop", () -> relocateReindexTasksAndAwaitComplete(reindexTimeout, transportService.getTaskManager()));
        if (terminationHandler != null) {
            addShutdownHook("termination-handler-stop", terminationHandler::handleTermination);
        }
    }

    public void addShutdownHook(String name, Runnable action) {
        hooks.add(new ShutdownHook(name, action));
    }

    public boolean isShuttingDown() {
        return isShuttingDown;
    }

    /**
     * Invokes hooks to prepare this node to be closed. This should be called when Elasticsearch receives a request to shut down
     * gracefully from the underlying operating system, before system resources are closed. This method will block
     * until the node is ready to shut down.
     * <p>
     * Note that this class is part of infrastructure to react to signals from the operating system - most graceful shutdown
     * logic should use Node Shutdown, see {@link org.elasticsearch.cluster.metadata.NodesShutdownMetadata}.
     */
    public void prepareForShutdown() {
        assert isShuttingDown == false;
        isShuttingDown = true;

        // first make sure the node can safely be shutdown
        if (terminationHandler != null) {
            try {
                terminationHandler.blockTermination();
            } catch (RuntimeException e) {
                logger.warn("termination handler failed; proceeding with shutdown", e);
            }
        }

        record Stopper(String name, SubscribableListener<Void> listener) {
            boolean isIncomplete() {
                return listener().isDone() == false;
            }
        }

        final var stoppers = new ArrayList<Stopper>();
        final var allStoppersFuture = new PlainActionFuture<Void>();
        try (var listeners = new RefCountingListener(allStoppersFuture)) {
            for (var hook : hooks) {
                final var stopper = new Stopper(hook.name(), new SubscribableListener<>());
                stoppers.add(stopper);
                stopper.listener().addListener(listeners.acquire());
                new Thread(() -> {
                    try {
                        hook.action.run();
                    } catch (Exception ex) {
                        logger.warn("unexpected exception in shutdown task [" + stopper.name() + "]", ex);
                    } finally {
                        stopper.listener().onResponse(null);
                    }
                }, stopper.name()).start();
            }
        }

        final Supplier<String> incompleteStoppersDescriber = () -> stoppers.stream()
            .filter(Stopper::isIncomplete)
            .map(Stopper::name)
            .collect(Collectors.joining(", ", "[", "]"));

        try {
            if (TimeValue.ZERO.equals(maxTimeout)) {
                allStoppersFuture.get();
            } else {
                allStoppersFuture.get(maxTimeout.millis(), TimeUnit.MILLISECONDS);
            }
        } catch (ExecutionException e) {
            assert false : e; // listeners are never completed exceptionally
            logger.warn("failed during graceful shutdown tasks", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("interrupted while waiting for graceful shutdown tasks: " + incompleteStoppersDescriber.get(), e);
        } catch (TimeoutException e) {
            logger.warn("timed out while waiting for graceful shutdown tasks: " + incompleteStoppersDescriber.get());
        }
    }

    /// The polling interval used by [#awaitTasksCompleteInternal]. Chosen to allow short response times, but (since checking the tasks list
    /// is relatively expensive) not so short that we waste CPU time we could be spending on finishing those tasks.
    static final TimeValue AWAIT_TASKS_POLL_INTERVAL = TimeValue.timeValueMillis(500);

    // exists and package-private for testing
    protected static class Sleeper {

        void sleep(TimeValue interval) throws InterruptedException {
            Thread.sleep(interval.millis());
        }
    }

    /**
     * Which of the tasks matching the awaited actions a wait covers.
     */
    protected enum TaskScope {
        /**
         * Only the tasks running at the first poll. For when new tasks keep arriving for as long as the node is up, so that the wait is
         * bounded by the work in flight when it started.
         */
        INITIAL,
        /**
         * Also the tasks that start while others are still being waited for. For when the source of new tasks dries up on its own, so
         * that the wait covers the trailing ones too.
         */
        ALL
    }

    protected boolean awaitTasksComplete(
        TimeValue timeout,
        Sleeper sleeper,
        Set<String> actionNames,
        TaskScope scope,
        TaskManager taskManager,
        @Nullable Consumer<Task> taskNotifier,
        @Nullable Consumer<List<Task>> onTimeout
    ) {
        return awaitTasksCompleteInternal(timeout, sleeper, actionNames, scope, "finish", taskManager, taskNotifier, onTimeout);
    }

    protected boolean awaitTasksCancellation(TimeValue timeout, Sleeper sleeper, Set<String> actionNames, TaskManager taskManager) {
        return awaitTasksCompleteInternal(timeout, sleeper, actionNames, TaskScope.ALL, "complete", taskManager, null, null);
    }

    /// Repeatedly polls the `taskManager` to list tasks whose action name is any of `actionNames`, invoking `sleeper` to sleep for
    /// [#AWAIT_TASKS_POLL_INTERVAL] between each poll, until either no matching tasks are returned or the total time waited reaches
    /// `timeout`. Depending on `scope`, tasks that start after the first poll are either waited for as well or ignored. Invokes
    /// `taskNotifier` exactly once for each task waited for. Returns true if it found no matching tasks, false if it timed out or was
    /// interrupted. The `waitFor` parameter gives a verb describing what we're waiting for the task to do, to use in logging, e.g.
    /// `finish` or `cancel`.
    // package-private for testing
    static boolean awaitTasksCompleteInternal(
        TimeValue timeout,
        Sleeper sleeper,
        Set<String> actionNames,
        TaskScope scope,
        String waitFor,
        TaskManager taskManager,
        @Nullable Consumer<Task> taskNotifier,
        @Nullable Consumer<List<Task>> onTimeout
    ) {
        long millisWaited = 0;
        boolean firstPoll = true;
        Set<Long> tasksTracked = new HashSet<>(); // the ids of the tasks waited for, each notified once when first seen
        while (true) {
            List<Task> tasksRemaining = new ArrayList<>();
            for (Task task : taskManager.getTasks().values()) {
                if (actionNames.contains(task.getAction()) == false) {
                    continue;
                }
                if (tasksTracked.contains(task.getId())) {
                    tasksRemaining.add(task);
                } else if (firstPoll || scope == TaskScope.ALL) {
                    tasksTracked.add(task.getId());
                    if (taskNotifier != null) {
                        taskNotifier.accept(task);
                    }
                    tasksRemaining.add(task);
                }
            }
            firstPoll = false;
            if (tasksRemaining.isEmpty()) {
                logger.debug("all tasks of actions {} complete", actionNames);
                return true;
            } else {
                // Let the system work on those tasks for a while. We're on a dedicated thread to manage app shutdown, so we
                // literally just want to wait and not take up resources on this thread for now.
                millisWaited += AWAIT_TASKS_POLL_INTERVAL.millis();
                if (TimeValue.ZERO.equals(timeout) == false && millisWaited >= timeout.millis()) {
                    logger.warn(
                        "timed out after waiting [{}] for [{}] tasks to {}: {}",
                        timeout,
                        tasksRemaining.size(),
                        waitFor,
                        countPerAction(tasksRemaining)
                    );
                    if (onTimeout != null) {
                        onTimeout.accept(tasksRemaining);
                    }
                    return false;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "waiting for [{}] tasks to {}, next poll in [{}]: {}",
                        tasksRemaining.size(),
                        waitFor,
                        AWAIT_TASKS_POLL_INTERVAL,
                        countPerAction(tasksRemaining)
                    );
                }
                try {
                    sleeper.sleep(AWAIT_TASKS_POLL_INTERVAL);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    logger.warn(
                        "interrupted while waiting [{}] for [{}] tasks to {}: {}",
                        timeout,
                        tasksRemaining.size(),
                        waitFor,
                        countPerAction(tasksRemaining)
                    );
                    return false;
                }
            }
        }
    }

    private static Map<String, Long> countPerAction(List<Task> tasks) {
        return tasks.stream().collect(Collectors.groupingBy(Task::getAction, TreeMap::new, Collectors.counting()));
    }

    private void relocateReindexTasksAndAwaitComplete(TimeValue asyncReindexTimeout, TaskManager taskManager) {
        final var sleeper = new Sleeper();
        awaitTasksComplete(
            asyncReindexTimeout,
            sleeper,
            REINDEX_ACTIONS,
            TaskScope.ALL,
            taskManager,
            task -> maybeRequestRelocationForBulkByPaginatedSearch(task, taskManager),
            tasks -> {
                // Cancel any reindex tasks that could not be relocated, then wait a short time
                // for them to exit the task manager before proceeding with shutdown.
                tasks.forEach(t -> {
                    if (t instanceof CancellableTask cancellable) {
                        try {
                            // We know that BulkByPaginatedSearchTask implements ensureCancellable, so call it
                            // first to avoid cancelling actively relocating tasks
                            // TaskManager should probably do this (see https://github.com/elastic/elasticsearch/issues/155444)
                            cancellable.ensureCancellable();
                            taskManager.cancelTaskAndDescendants(
                                cancellable,
                                CANNOT_RELOCATE_REINDEX_CANCEL_REASON,
                                false,
                                ActionListener.noop()
                            );
                        } catch (ElasticsearchStatusException e) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(() -> Strings.format("Unable to cancel reindex task %s", t), e);
                            }
                        }
                    } else {
                        assert false : "Expected reindex tasks to be cancellable";
                    }
                });
                awaitTasksCancellation(REINDEXING_CANCELLATION_TIMEOUT, sleeper, REINDEX_ACTIONS, taskManager);
            }
        );
    }

    // package-private for tests
    static void maybeRequestRelocationForBulkByPaginatedSearch(Task task, TaskManager taskManager) {
        if (task instanceof BulkByPaginatedSearchTask bulkByPaginatedSearchTask) {
            TaskId parentTaskId = task.getParentTaskId();
            boolean hasLocalParent = parentTaskId.isSet() && parentTaskId.getNodeId().equals(bulkByPaginatedSearchTask.getNodeId());
            Task localParent = hasLocalParent ? taskManager.getTask(parentTaskId.getId()) : null;
            boolean isChildTaskOfSameType = localParent != null && localParent.getAction().equals(task.getAction());
            if (bulkByPaginatedSearchTask.isEligibleForRelocationOnShutdown()) {
                assert !bulkByPaginatedSearchTask.isRelocationRequested() : "Requested relocation multiple times for task " + task.getId();
                if (!isChildTaskOfSameType) {
                    logger.info("Requesting relocation for bulk-by-paginated-search task {}", bulkByPaginatedSearchTask.getId());
                } else {
                    logger.debug(
                        "Requesting relocation for child bulk-by-paginated-search task {} (parent: {})",
                        bulkByPaginatedSearchTask.getId(),
                        bulkByPaginatedSearchTask.getParentTaskId()
                    );
                }
                bulkByPaginatedSearchTask.requestRelocation();
            } else {
                if (!isChildTaskOfSameType) {
                    if (localParent != null) {
                        logger.info(
                            "Not requesting relocation for bulk-by-paginated-search task {} as not eligible (parent action: {})",
                            bulkByPaginatedSearchTask.getId(),
                            localParent.getAction()
                        );
                    } else if (hasLocalParent) {
                        logger.info(
                            "Not requesting relocation for bulk-by-paginated-search task {} as not eligible "
                                + "(parent task is local but not found)",
                            bulkByPaginatedSearchTask.getId()
                        );
                    } else if (parentTaskId.isSet()) {
                        logger.info(
                            "Not requesting relocation for bulk-by-paginated-search task {} as not eligible (parent task is not local)",
                            bulkByPaginatedSearchTask.getId()
                        );
                    } else {
                        logger.info(
                            "Not requesting relocation for bulk-by-paginated-search task {} as not eligible (no parent task)",
                            bulkByPaginatedSearchTask.getId()
                        );
                    }
                } else {
                    logger.debug(
                        "Not requesting relocation for child bulk-by-paginated-search task {} as not eligible (parent: {})",
                        bulkByPaginatedSearchTask.getId(),
                        bulkByPaginatedSearchTask.getParentTaskId()
                    );
                }
            }
        } else {
            logger.warn("Requested relocation task for non-bulk-by-paginated-search task {}", task);
        }
    }
}
