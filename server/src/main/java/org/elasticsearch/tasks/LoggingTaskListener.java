/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.UpdateForV9;

import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

/**
 * An {@link ActionListener} that just logs the task and its response at the info level. Used when we need a listener but aren't returning
 * the result to the user.
 */
public final class LoggingTaskListener<Response> implements ActionListener<Response> {

    public static final String LOGGING_ENABLED_QUERY_PARAMETER = "log_task_completion";
    private static final Logger logger = LogManager.getLogger(LoggingTaskListener.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(LoggingTaskListener.class);

    private final Task task;

    private LoggingTaskListener(Task task) {
        this.task = task;
    }

    @Override
    public void onResponse(Response response) {
        logger.info("{} finished with response {}", task.getId(), response);
    }

    @Override
    public void onFailure(Exception e) {
        logger.warn(() -> format("%s failed with exception", task.getId()), e);
    }

    @UpdateForV9 // always just use noop() in v9
    public static <Response> Task runWithLoggingTaskListener(boolean enabled, Function<ActionListener<Response>, Task> taskSupplier) {
        if (enabled == false) {
            return taskSupplier.apply(ActionListener.noop());
        }

        deprecationLogger.warn(
            DeprecationCategory.OTHER,
            "logging_task_listener",
            """
                Logging the completion of a task using [{}] is deprecated and will be removed in a future version. Instead, use the task \
                management API [{}] to monitor long-running tasks for completion. To suppress this warning and opt-in to the future \
                behaviour now, set [?{}=false] when calling the affected API.""",
            LoggingTaskListener.class.getCanonicalName(),
            ReferenceDocs.TASK_MANAGEMENT_API,
            LOGGING_ENABLED_QUERY_PARAMETER
        );
        final var responseListener = new SubscribableListener<Response>();
        final var task = taskSupplier.apply(responseListener);
        responseListener.addListener(new LoggingTaskListener<>(task));
        return task;
    }
}
