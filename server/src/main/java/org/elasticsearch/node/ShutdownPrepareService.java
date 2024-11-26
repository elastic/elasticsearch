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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.tasks.TaskManager;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

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

    private final Logger logger = LogManager.getLogger(ShutdownPrepareService.class);
    private final Settings settings;
    private final HttpServerTransport httpServerTransport;
    private final TerminationHandler terminationHandler;
    private volatile boolean hasBeenShutdown = false;

    public ShutdownPrepareService(Settings settings, HttpServerTransport httpServerTransport, TerminationHandler terminationHandler) {
        this.settings = settings;
        this.httpServerTransport = httpServerTransport;
        this.terminationHandler = terminationHandler;
    }

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
     * Invokes hooks to prepare this node to be closed. This should be called when Elasticsearch receives a request to shut down
     * gracefully from the underlying operating system, before system resources are closed. This method will block
     * until the node is ready to shut down.
     * <p>
     * Note that this class is part of infrastructure to react to signals from the operating system - most graceful shutdown
     * logic should use Node Shutdown, see {@link org.elasticsearch.cluster.metadata.NodesShutdownMetadata}.
     */
    public void prepareForShutdown(TaskManager taskManager) {
        assert hasBeenShutdown == false;
        hasBeenShutdown = true;
        final var maxTimeout = MAXIMUM_SHUTDOWN_TIMEOUT_SETTING.get(settings);
        final var reindexTimeout = MAXIMUM_REINDEXING_TIMEOUT_SETTING.get(settings);

        record Stopper(String name, SubscribableListener<Void> listener) {
            boolean isIncomplete() {
                return listener().isDone() == false;
            }
        }

        final var stoppers = new ArrayList<Stopper>();
        final var allStoppersFuture = new PlainActionFuture<Void>();
        try (var listeners = new RefCountingListener(allStoppersFuture)) {
            final BiConsumer<String, Runnable> stopperRunner = (name, action) -> {
                final var stopper = new Stopper(name, new SubscribableListener<>());
                stoppers.add(stopper);
                stopper.listener().addListener(listeners.acquire());
                new Thread(() -> {
                    try {
                        action.run();
                    } catch (Exception ex) {
                        logger.warn("unexpected exception in shutdown task [" + stopper.name() + "]", ex);
                    } finally {
                        stopper.listener().onResponse(null);
                    }
                }, stopper.name()).start();
            };

            stopperRunner.accept("http-server-transport-stop", httpServerTransport::close);
            stopperRunner.accept("async-search-stop", () -> awaitSearchTasksComplete(maxTimeout, taskManager));
            stopperRunner.accept("reindex-stop", () -> awaitReindexTasksComplete(reindexTimeout, taskManager));
            if (terminationHandler != null) {
                stopperRunner.accept("termination-handler-stop", terminationHandler::handleTermination);
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

    private void awaitTasksComplete(TimeValue timeout, String taskName, TaskManager taskManager) {
        long millisWaited = 0;
        while (true) {
            long tasksRemaining = taskManager.getTasks().values().stream().filter(task -> taskName.equals(task.getAction())).count();
            if (tasksRemaining == 0) {
                logger.debug("all " + taskName + " tasks complete");
                return;
            } else {
                // Let the system work on those tasks for a while. We're on a dedicated thread to manage app shutdown, so we
                // literally just want to wait and not take up resources on this thread for now. Poll period chosen to allow short
                // response times, but checking the tasks list is relatively expensive, and we don't want to waste CPU time we could
                // be spending on finishing those tasks.
                final TimeValue pollPeriod = TimeValue.timeValueMillis(500);
                millisWaited += pollPeriod.millis();
                if (TimeValue.ZERO.equals(timeout) == false && millisWaited >= timeout.millis()) {
                    logger.warn(
                        format("timed out after waiting [%s] for [%d] " + taskName + " tasks to finish", timeout.toString(), tasksRemaining)
                    );
                    return;
                }
                logger.debug(format("waiting for [%s] " + taskName + " tasks to finish, next poll in [%s]", tasksRemaining, pollPeriod));
                try {
                    Thread.sleep(pollPeriod.millis());
                } catch (InterruptedException ex) {
                    logger.warn(
                        format(
                            "interrupted while waiting [%s] for [%d] " + taskName + " tasks to finish",
                            timeout.toString(),
                            tasksRemaining
                        )
                    );
                    return;
                }
            }
        }
    }

    private void awaitSearchTasksComplete(TimeValue asyncSearchTimeout, TaskManager taskManager) {
        awaitTasksComplete(asyncSearchTimeout, TransportSearchAction.NAME, taskManager);
    }

    private void awaitReindexTasksComplete(TimeValue asyncReindexTimeout, TaskManager taskManager) {
        awaitTasksComplete(asyncReindexTimeout, ReindexAction.NAME, taskManager);
    }

}
