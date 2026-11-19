/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.apache.logging.log4j.Level;
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
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class was created to extract out the logic from {@link Node#prepareForClose()} to facilitate testing.
 * <p>
 * Invokes hooks to prepare this node to be closed. This should be called when Elasticsearch receives a request to shut down
 * gracefully from the underlying operating system, before system resources are closed.
 * <p>
 * The hooks run in parallel. On a node with a {@link TerminationHandler} (stateless), they are followed by a drain: once the handler
 * reports the node {@link TerminationHandler.Outcome#READY ready}, i.e. its shards are safe to serve elsewhere, the plugins drain the
 * work they accepted (see {@link ShutdownAwarePlugin#startDrainOnShutdown()}), then the node waits for the tasks of the awaited
 * actions. Everything is bounded by a single deadline, {@code node.maximum_shutdown_grace_period} after the shutdown starts, or by
 * nothing if that setting is zero.
 * <p>
 * Note that this class is part of infrastructure to react to signals from the operating system - most graceful shutdown
 * logic should use Node Shutdown, see {@link org.elasticsearch.cluster.metadata.NodesShutdownMetadata}.
 */
public class ShutdownPrepareService {

    public static final String CANNOT_RELOCATE_REINDEX_CANCEL_REASON = "node shutting down";

    private record ShutdownHook(String name, Runnable action) {}

    /**
     * A plugin's drain, see {@link ShutdownAwarePlugin#startDrainOnShutdown()}.
     *
     * @param name  identifies the drain in the logs
     * @param start starts the drain and returns its completion
     */
    public record PluginDrain(String name, Supplier<SubscribableListener<Void>> start) {}

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
    private final TimeValue reindexTimeout;
    private final HttpServerTransport httpServerTransport;
    private final TransportService transportService;
    private final TerminationHandler terminationHandler;
    private final Set<String> awaitedActions;
    private final List<PluginDrain> pluginDrains;
    private final Sleeper sleeper;
    private final List<ShutdownHook> hooks = new ArrayList<>();
    private volatile boolean isShuttingDown = false;

    /**
     * Creates the service.
     *
     * @param actionsToAwait names of the actions whose tasks the node waits for during graceful shutdown; collected from
     *                       {@link ShutdownAwarePlugin#getActionsToAwaitOnShutdown()}
     * @param pluginDrains   the drains started once the termination handler reports the node ready; collected from
     *                       {@link ShutdownAwarePlugin#startDrainOnShutdown()}
     */
    public ShutdownPrepareService(
        Settings settings,
        HttpServerTransport httpServerTransport,
        TransportService transportService,
        TerminationHandler terminationHandler,
        Collection<String> actionsToAwait,
        Collection<PluginDrain> pluginDrains
    ) {
        this(settings, httpServerTransport, transportService, terminationHandler, actionsToAwait, pluginDrains, new Sleeper());
    }

    /**
     * Allows tests to drive the waits: {@code sleeper} pauses between task polls and keeps the time that the deadline and the task
     * waits are measured on.
     */
    // package-private for testing
    ShutdownPrepareService(
        Settings settings,
        HttpServerTransport httpServerTransport,
        TransportService transportService,
        TerminationHandler terminationHandler,
        Collection<String> actionsToAwait,
        Collection<PluginDrain> pluginDrains,
        Sleeper sleeper
    ) {
        this.maxTimeout = MAXIMUM_SHUTDOWN_TIMEOUT_SETTING.get(settings);
        this.reindexTimeout = MAXIMUM_REINDEXING_TIMEOUT_SETTING.get(settings);
        this.httpServerTransport = httpServerTransport;
        this.transportService = transportService;
        this.terminationHandler = terminationHandler;
        this.awaitedActions = Set.copyOf(actionsToAwait);
        this.pluginDrains = List.copyOf(pluginDrains);
        this.sleeper = sleeper;
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

        final var deadline = Deadline.start(maxTimeout, sleeper::relativeTimeNanos);
        final var handlerOutcome = new AtomicReference<>(TerminationHandler.Outcome.NOT_READY);
        final var allHooks = builtInHooks(deadline, handlerOutcome);
        allHooks.addAll(hooks);
        runHooks(allHooks, deadline);
        if (terminationHandler != null) {
            drain(handlerOutcome.get(), deadline);
        }
    }

    private List<ShutdownHook> builtInHooks(Deadline deadline, AtomicReference<TerminationHandler.Outcome> handlerOutcome) {
        final var builtInHooks = new ArrayList<ShutdownHook>();
        builtInHooks.add(new ShutdownHook("http-server-transport-stop", httpServerTransport::close));
        if (terminationHandler == null) {
            // Without a termination handler (stateful), nothing moves the shards away unless the operator did so beforehand, so tasks of
            // the awaited actions can keep arriving for as long as the node is up. Only the tasks in flight at the start of the shutdown
            // are waited for, or the wait might never end (the grace period is unbounded by default). With a termination handler, the
            // tasks are waited for in the drain, after the handler.
            builtInHooks.add(
                new ShutdownHook(
                    "await-tasks-stop",
                    () -> awaitTasksComplete(
                        deadline.awaitTimeout(),
                        sleeper,
                        awaitedActions,
                        TaskScope.INITIAL,
                        transportService.getTaskManager(),
                        null,
                        null
                    )
                )
            );
        }
        builtInHooks.add(
            new ShutdownHook("reindex-stop", () -> relocateReindexTasksAndAwaitComplete(reindexTimeout, transportService.getTaskManager()))
        );
        if (terminationHandler != null) {
            builtInHooks.add(
                new ShutdownHook("termination-handler-stop", () -> handlerOutcome.set(terminationHandler.handleTermination()))
            );
        }
        return builtInHooks;
    }

    /**
     * Runs the hooks in parallel and waits for them within the deadline.
     */
    private static void runHooks(List<ShutdownHook> hooks, Deadline deadline) {
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
            if (deadline.isBounded()) {
                allStoppersFuture.get(deadline.remainingNanos(), TimeUnit.NANOSECONDS);
            } else {
                allStoppersFuture.get();
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

    /**
     * Drains the work this node accepted, once the hooks are done, on a node with a termination handler. The plugin drains run only if
     * the handler reported the node ready, since only then can the work they turn away be served elsewhere, and only if the wait for
     * the hooks ended normally: neither did the grace period run out, nor was the shutdown thread interrupted, as the drains could not
     * be waited for then.
     * <p>
     * The tasks of the awaited actions are waited for in any case, and after the plugin drains, since some of that work may still reach
     * this node as new tasks while they run. Only the tasks running when the wait starts are waited for, even on a ready node: its
     * peers in the cluster stop sending it work, but senders that do not see its shutdown, such as a remote cluster running a
     * cross-cluster search through it, keep doing so until it closes, and waiting for their tasks as well might never end (the grace
     * period is unbounded by default).
     */
    private void drain(TerminationHandler.Outcome handlerOutcome, Deadline deadline) {
        final boolean expired = deadline.isExpired();
        final boolean interrupted = Thread.currentThread().isInterrupted();
        if (handlerOutcome == TerminationHandler.Outcome.READY && expired == false && interrupted == false) {
            logger.info(
                "termination handler reported the node ready for shutdown, running [{}] plugin drains then waiting for the tasks of "
                    + "actions {}",
                pluginDrains.size(),
                awaitedActions
            );
            awaitPluginDrains(deadline);
        } else {
            // the handler reports its own failures, so this is only worth a warning if it leaves drains unstarted
            logger.log(
                pluginDrains.isEmpty() ? Level.DEBUG : Level.WARN,
                "skipping [{}] plugin drains, termination handler outcome [{}], grace period expired [{}], interrupted [{}]; "
                    + "waiting for the tasks of actions {}",
                pluginDrains.size(),
                handlerOutcome,
                expired,
                interrupted,
                awaitedActions
            );
        }
        final boolean tasksCompleted = awaitTasksComplete(
            deadline.awaitTimeout(),
            sleeper,
            awaitedActions,
            TaskScope.INITIAL,
            transportService.getTaskManager(),
            null,
            null
        );
        logger.debug("graceful shutdown drain finished, tasks completed [{}]", tasksCompleted);
    }

    /**
     * Starts the plugin drains and waits for them within the deadline.
     */
    private void awaitPluginDrains(Deadline deadline) {
        record Drain(String name, SubscribableListener<Void> completion) {}

        final var drains = new ArrayList<Drain>();
        final var allDrainedFuture = new PlainActionFuture<Void>();
        try (var listeners = new RefCountingListener(allDrainedFuture)) {
            for (var pluginDrain : pluginDrains) {
                // a drain that fails to start or completes exceptionally counts as completed, so that it holds up neither the others
                // nor the task wait that follows
                final ActionListener<Void> drained = ActionListener.notifyOnce(listeners.acquire());
                try {
                    final var completion = Objects.requireNonNull(pluginDrain.start().get(), "drain completion");
                    drains.add(new Drain(pluginDrain.name(), completion));
                    completion.addListener(ActionListener.wrap(drained::onResponse, e -> {
                        logger.warn(Strings.format("shutdown drain of [%s] failed", pluginDrain.name()), e);
                        drained.onResponse(null);
                    }));
                } catch (Exception e) {
                    logger.warn(Strings.format("failed to start the shutdown drain of [%s]", pluginDrain.name()), e);
                    drained.onResponse(null);
                }
            }
        }

        final Supplier<String> incompleteDrainsDescriber = () -> drains.stream()
            .filter(d -> d.completion().isDone() == false)
            .map(Drain::name)
            .collect(Collectors.joining(", ", "[", "]"));

        try {
            if (deadline.isBounded()) {
                allDrainedFuture.get(deadline.remainingNanos(), TimeUnit.NANOSECONDS);
            } else {
                allDrainedFuture.get();
            }
        } catch (ExecutionException e) {
            assert false : e; // failures are logged and released as completions
            logger.warn("failed during plugin shutdown drains", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn(Strings.format("interrupted while waiting for plugin shutdown drains: %s", incompleteDrainsDescriber.get()), e);
        } catch (TimeoutException e) {
            logger.warn("timed out while waiting for plugin shutdown drains: {}", incompleteDrainsDescriber.get());
        }
    }

    /**
     * The point in time by which graceful shutdown must be done, measured on {@code clock} from {@code startNanos}, or no such point if
     * {@code node.maximum_shutdown_grace_period} is zero.
     */
    private static final class Deadline {
        private static final long UNBOUNDED = -1L;

        private final LongSupplier clock;
        private final long startNanos;
        private final long budgetNanos;

        private Deadline(LongSupplier clock, long startNanos, long budgetNanos) {
            this.clock = clock;
            this.startNanos = startNanos;
            this.budgetNanos = budgetNanos;
        }

        static Deadline start(TimeValue maxTimeout, LongSupplier clock) {
            return new Deadline(clock, clock.getAsLong(), TimeValue.ZERO.equals(maxTimeout) ? UNBOUNDED : maxTimeout.nanos());
        }

        boolean isBounded() {
            return budgetNanos != UNBOUNDED;
        }

        /** May be zero or negative once expired. */
        long remainingNanos() {
            assert isBounded();
            return budgetNanos - (clock.getAsLong() - startNanos);
        }

        boolean isExpired() {
            return isBounded() && remainingNanos() <= 0;
        }

        /**
         * The timeout for {@link #awaitTasksComplete}, where {@link TimeValue#ZERO} means unbounded. An expired deadline yields the
         * shortest positive timeout instead, so that the wait polls once, reports the tasks still running and returns (see
         * {@link #awaitTasksCompleteInternal}).
         */
        TimeValue awaitTimeout() {
            return isBounded() ? TimeValue.timeValueNanos(Math.max(1L, remainingNanos())) : TimeValue.ZERO;
        }
    }

    /// The polling interval used by [#awaitTasksCompleteInternal]. Chosen to allow short response times, but (since checking the tasks list
    /// is relatively expensive) not so short that we waste CPU time we could be spending on finishing those tasks.
    static final TimeValue AWAIT_TASKS_POLL_INTERVAL = TimeValue.timeValueMillis(500);

    /**
     * Pauses between task polls and keeps the time that the waits are measured on. Exists, and its methods are package-private, for
     * testing.
     */
    protected static class Sleeper {

        void sleep(TimeValue interval) throws InterruptedException {
            Thread.sleep(interval.millis());
        }

        long relativeTimeNanos() {
            return System.nanoTime();
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
    /// [#AWAIT_TASKS_POLL_INTERVAL] between each poll, until either no matching tasks are returned or `timeout` has elapsed on the
    /// `sleeper`'s clock; the last sleep is shortened to end at the timeout, and a poll that finds less than a millisecond left (the
    /// granularity of the sleeps) times out without sleeping. A `timeout` of zero means no timeout. Depending on `scope`, tasks that
    /// start after the first poll are either waited for as well or ignored. Invokes `taskNotifier` exactly once for each task waited
    /// for. Returns true if it found no matching tasks, false if it timed out or was
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
        final long startNanos = sleeper.relativeTimeNanos();
        boolean firstPoll = true;
        boolean slept = false;
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
                final TimeValue nextSleep;
                if (TimeValue.ZERO.equals(timeout)) {
                    nextSleep = AWAIT_TASKS_POLL_INTERVAL;
                } else {
                    final long remainingMillis = TimeUnit.NANOSECONDS.toMillis(
                        timeout.nanos() - (sleeper.relativeTimeNanos() - startNanos)
                    );
                    nextSleep = TimeValue.timeValueMillis(Math.min(AWAIT_TASKS_POLL_INTERVAL.millis(), Math.max(0L, remainingMillis)));
                }
                if (nextSleep.millis() == 0) {
                    if (slept) {
                        logger.warn(
                            "timed out after waiting [{}] for [{}] tasks to {}: {}",
                            timeout,
                            tasksRemaining.size(),
                            waitFor,
                            countPerAction(tasksRemaining)
                        );
                    } else {
                        // e.g. the shutdown grace period ran out before this wait started; the timeout would only mislead
                        logger.warn(
                            "no time left to wait for [{}] tasks to {}: {}",
                            tasksRemaining.size(),
                            waitFor,
                            countPerAction(tasksRemaining)
                        );
                    }
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
                        nextSleep,
                        countPerAction(tasksRemaining)
                    );
                }
                try {
                    slept = true;
                    sleeper.sleep(nextSleep);
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
