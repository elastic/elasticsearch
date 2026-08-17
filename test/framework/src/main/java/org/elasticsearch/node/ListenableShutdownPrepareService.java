/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/// A [ShutdownPrepareService] that allows tests to listen for task timeouts
public class ListenableShutdownPrepareService extends ShutdownPrepareService {

    /// Notified when [#awaitTasksComplete(TimeValue, Sleeper, String, TaskManager, Consumer, Consumer)] times out
    public interface TaskTimeoutListener {

        /// @param taskName The type of task that failed
        /// @param tasks The list of tasks that timed out
        void onTimeout(String taskName, List<Task> tasks);
    }

    private final List<TaskTimeoutListener> taskTimeoutListeners = new CopyOnWriteArrayList<>();

    public ListenableShutdownPrepareService(
        Settings settings,
        HttpServerTransport httpServerTransport,
        TransportService transportService,
        TerminationHandler terminationHandler
    ) {
        super(settings, httpServerTransport, transportService, terminationHandler);
    }

    /// Marker plugin to indicate we should use the listenable service
    public static class TestPlugin extends org.elasticsearch.plugins.Plugin {}

    public void addTaskTimeoutListener(TaskTimeoutListener listener) {
        taskTimeoutListeners.add(listener);
    }

    @Override
    protected boolean awaitTasksComplete(
        TimeValue timeout,
        Sleeper sleeper,
        String taskName,
        TaskManager taskManager,
        @Nullable Consumer<Task> taskNotifier,
        @Nullable Consumer<List<Task>> onTimeout
    ) {
        return awaitTasksCompleteInternal(timeout, sleeper, taskName, taskManager, taskNotifier, tasks -> {
            notifyTaskTimeout(taskName, tasks);
            if (onTimeout != null) {
                onTimeout.accept(tasks);
            }
        });
    }

    private void notifyTaskTimeout(String taskName, List<Task> tasks) {
        for (TaskTimeoutListener listener : taskTimeoutListeners) {
            listener.onTimeout(taskName, tasks);
        }
    }
}
