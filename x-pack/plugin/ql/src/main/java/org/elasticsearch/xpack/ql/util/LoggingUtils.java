/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;

public final class LoggingUtils {

    private LoggingUtils() {}

    /**
     * Log the failure of a QL query, that failed with an exception.
     * @param logger The logger to send the failure message to.
     * @param queryName The type of QL query. Example `EQL query`. It is prepended to the failure message.
     * @param throwable The exception the query failed with.
     * @param preamble An optional message to log along the failure. Can be `null`.
     */
    public static void logQueryFailure(Logger logger, String queryName, Throwable throwable, @Nullable String preamble) {
        StringBuilder builder = new StringBuilder(queryName);
        builder.append(" ");
        builder.append(preamble == null ? "failed" : preamble);
        builder.append(": ");

        String canonical = throwable.getClass().getCanonicalName();
        String message = throwable.getMessage();
        builder.append(canonical);
        builder.append(": ");
        builder.append(message);
        builder.append("\n");
        builder.append(ExceptionsHelper.formatStackTrace(throwable.getStackTrace()));

        Throwable cause;
        if ((cause = throwable.getCause()) != null) {
            cause = ExceptionsHelper.unwrapCause(cause);
            builder.append("\n");
            builder.append("caused by: ");
            builder.append(cause.getClass().getCanonicalName());
            builder.append(": ");
            builder.append(cause.getMessage());
            builder.append("\n");
            builder.append(ExceptionsHelper.formatStackTrace(cause.getStackTrace()));
        }

        for (Throwable t : throwable.getSuppressed()) {
            builder.append("\n");
            builder.append("suppressing: ");
            builder.append(t.getClass().getCanonicalName());
            builder.append(": ");
            builder.append(t.getMessage());
            builder.append("\n");
            builder.append(ExceptionsHelper.formatStackTrace(t.getStackTrace()));
        }

        Level level = ExceptionsHelper.status(throwable).getStatus() >= 500 ? Level.WARN : Level.DEBUG;
        logger.log(level, builder.toString());
    }

    /**
     * Log the execution time and query when handling an ES|QL response.
     */
    public static <Response> ActionListener<Response> wrapWithFailureLogging(
        ActionListener<Response> actionListener,
        Logger logger,
        String queryName
    ) {
        return ActionListener.wrap(actionListener::onResponse, ex -> {
            logQueryFailure(logger, queryName, ex, null);
            actionListener.onFailure(ex);
        });
    }
}
