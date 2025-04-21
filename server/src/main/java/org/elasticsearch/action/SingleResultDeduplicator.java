/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.function.Consumer;

/**
 * Wraps an async action that consumes an {@link ActionListener} such that multiple invocations of {@link #execute(ActionListener)} can
 * share the result from a single call to the wrapped action.
 *
 * @param <T> Result type
 */
public abstract class SingleResultDeduplicator<T> {

    protected final ThreadContext threadContext;
    protected final Consumer<ActionListener<T>> executeAction;

    public SingleResultDeduplicator(ThreadContext threadContext, Consumer<ActionListener<T>> executeAction) {
        this.threadContext = threadContext;
        this.executeAction = executeAction;
    }

    /**
     * Execute the action for the given {@link ActionListener}.
     * @param listener listener to resolve with execution result. The listener always has its threadContext preserved, i.e.
     *                 when the listener is invoked, it will see its original threadContext plus any response headers generated
     *                 by performing the {@code executeAction}.
     */
    public abstract void execute(ActionListener<T> listener);
}
