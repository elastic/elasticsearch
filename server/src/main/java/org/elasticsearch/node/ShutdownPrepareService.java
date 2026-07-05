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
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    private static final Logger logger = LogManager.getLogger(ShutdownPrepareService.class);

    private final TimeValue maxTimeout;
    private final TerminationHandler terminationHandler;
    private final List<ShutdownHook> hooks = new ArrayList<>();
    private volatile boolean isShuttingDown = false;

    @SuppressWarnings(value = "this-escape")
    public ShutdownPrepareService(
        Settings settings,
        HttpServerTransport httpServerTransport,
        TransportService transportService,
        TerminationHandler terminationHandler
    ) {
        this.maxTimeout = MAXIMUM_SHUTDOWN_TIMEOUT_SETTING.get(settings);
        this.terminationHandler = terminationHandler;

        final var reindexTimeout = MAXIMUM_REINDEXING_TIMEOUT_SETTING.get(settings);
        addShutdownHook("http-server-transport-stop", httpServerTransport::close);
        addShutdownHook("async-search-stop", () -> awaitSearchTasksComplete(maxTimeout, transportService.getTaskManager()));
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

    /// The polling interval used by [#awaitTasksComplete]. Chosen to allow short response times, but (since checking the tasks list is
    /// relatively expensive) not so short that we waste CPU time we could be spending on finishing those tasks.
    static final TimeValue AWAIT_TASKS_POLL_INTERVAL = TimeValue.timeValueMillis(500);

    // exists and package-private for testing
    static class Sleeper {

        void sleep(TimeValue interval) throws InterruptedException {
            Thread.sleep(interval.millis());
        }
    }

    /// Repeatedly polls the `taskManager` to list tasks whose action name is `taskName`, invoking `sleeper` to sleep for
    /// [#AWAIT_TASKS_POLL_INTERVAL] between each poll, until either no matching tasks are returned or the total time waited reaches
    /// `timeout`. Invokes `taskNotifier` exactly once for each matching task encountered. Returns true if it found no matching tasks, false
    /// if it timed out or was interrupted.
    // package-private for testing
    static boolean awaitTasksComplete(
        TimeValue timeout,
        Sleeper sleeper,
        String taskName,
        TaskManager taskManager,
        @Nullable Consumer<Task> taskNotifier
    ) {
        long millisWaited = 0;
        Set<Long> tasksNotified = new HashSet<>();
        while (true) {
            List<Task> tasksRemaining = taskManager.getTasks().values().stream().filter(task -> taskName.equals(task.getAction())).toList();
            if (tasksRemaining.isEmpty()) {
                logger.debug("all {} tasks complete", taskName);
                return true;
            } else {
                // Notify all remaining tasks that a shutdown is happening, if a notifier is provided and if we have not already done so.
                if (taskNotifier != null) {
                    for (Task task : tasksRemaining) {
                        if (tasksNotified.add(task.getId())) {
                            taskNotifier.accept(task);
                        }
                    }
                }
                // Let the system work on those tasks for a while. We're on a dedicated thread to manage app shutdown, so we
                // literally just want to wait and not take up resources on this thread for now.
                millisWaited += AWAIT_TASKS_POLL_INTERVAL.millis();
                if (TimeValue.ZERO.equals(timeout) == false && millisWaited >= timeout.millis()) {
                    logger.warn("timed out after waiting [{}] for [{}] {} tasks to finish", timeout, tasksRemaining.size(), taskName);
                    return false;
                }
                logger.debug(
                    "waiting for [{}] {} tasks to finish, next poll in [{}]",
                    tasksRemaining.size(),
                    taskName,
                    AWAIT_TASKS_POLL_INTERVAL
                );
                try {
                    sleeper.sleep(AWAIT_TASKS_POLL_INTERVAL);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    logger.warn("interrupted while waiting [{}] for [{}] {} tasks to finish", timeout, tasksRemaining.size(), taskName);
                    return false;
                }
            }
        }
    }

    private void awaitSearchTasksComplete(TimeValue asyncSearchTimeout, TaskManager taskManager) {
        awaitTasksComplete(asyncSearchTimeout, new Sleeper(), TransportSearchAction.NAME, taskManager, null);
    }

    private void relocateReindexTasksAndAwaitComplete(TimeValue asyncReindexTimeout, TaskManager taskManager) {
        awaitTasksComplete(
            asyncReindexTimeout,
            new Sleeper(),
            ReindexAction.NAME,
            taskManager,
            task -> maybeRequestRelocationForBulkByPaginatedSearch(task, taskManager)
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
