/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.tasks.Task;

/**
 * Builder class for a logger context.
 * The builder is created at the beginning of a logging operation and will produce a context when the operation is completed,
 * which is usually the completion of the listener.
 * @param <Context> Context type to build.
 * @param <Request> Request class.
 * @param <Response> Response type for the listener.
 */
public abstract class ActivityLoggerContextBuilder<Context extends ActivityLoggerContext, Request, Response> {

    private final long start;
    protected final Request request;
    protected final Task task;

    protected ActivityLoggerContextBuilder(Task task, Request request) {
        start = System.nanoTime();
        this.task = task;
        this.request = request;
    }

    protected long elapsed() {
        return System.nanoTime() - start;
    }

    /**
     * Build context for successful completion
     */
    public abstract Context build(Response response);

    /**
     * Build context for failure completion
     */
    public abstract Context build(Exception e);
}
