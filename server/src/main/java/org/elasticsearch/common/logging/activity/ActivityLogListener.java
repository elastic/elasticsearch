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

import java.util.function.Consumer;

/**
 * Follows actions at the point where the activity log sees them, whether or not the log is enabled and whatever its
 * threshold, so that a feature can look at requests and their outcome without them being logged.
 * <p>
 * {@link #start} is called for every request of the action, so it must be cheap: a listener that only wants some requests
 * decides there, and only the requests it follows pay for being followed.
 *
 * @param <Context> the context of a completed action, holding its request and its response or failure
 * @param <Request> the request of the action
 */
public interface ActivityLogListener<Context extends ActivityLoggerContext, Request> {

    /**
     * Called when the action starts, on the thread that starts it.
     *
     * @return what to call with the context once the action completes, or {@code null} not to follow this request. It is
     *         called on the thread that completes the action, before the caller is told, so it must not block, and it copies
     *         what it needs of a response, which may be released once it returns. An exception it throws is logged and does
     *         not affect the action.
     */
    @Nullable
    Consumer<Context> start(Task task, Request request);
}
