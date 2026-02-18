/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;

/**
 * Context for {@link ActivityLogger}.
 * This class contains the information needed to log the query and is extended by specific logging contexts for each action.
 * {@link ActivityLogProducer} uses the context data to compose fields for the log message.
 */
public abstract class ActivityLoggerContext {
    private final long tookInNanos;
    private final String type;
    private final @Nullable Exception error;
    private final Task task;

    public ActivityLoggerContext(Task task, String type, long tookInNanos, @Nullable Exception error) {
        this.type = type;
        this.error = error;
        this.tookInNanos = tookInNanos;
        this.task = task;
    }

    public ActivityLoggerContext(Task task, String type, long tookInNanos) {
        this(task, type, tookInNanos, null);
    }

    public long getTookInNanos() {
        return tookInNanos;
    }

    public boolean isSuccess() {
        return error == null;
    }

    public String getType() {
        return type;
    }

    public String getErrorMessage() {
        return error == null ? "" : error.getMessage();
    }

    public String getErrorType() {
        return error == null ? "" : error.getClass().getName();
    }

    public String getOpaqueId() {
        return task.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
    }
}
