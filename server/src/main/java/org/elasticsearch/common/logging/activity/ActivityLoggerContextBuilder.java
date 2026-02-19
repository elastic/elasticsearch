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

import java.util.function.LongSupplier;

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
    protected final LongSupplier nanoTimeSupplier;

    protected ActivityLoggerContextBuilder(Task task, Request request) {
        this(task, request, System::nanoTime);
    }

    protected ActivityLoggerContextBuilder(Task task, Request request, LongSupplier nanoTimeSupplier) {
        start = nanoTimeSupplier.getAsLong();
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.task = task;
        this.request = request;
    }

    protected long elapsed() {
        return nanoTimeSupplier.getAsLong() - start;
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
