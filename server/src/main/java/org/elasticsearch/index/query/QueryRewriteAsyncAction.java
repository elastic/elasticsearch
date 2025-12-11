/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;

import java.util.List;
import java.util.function.Consumer;

/**
 * Represents a unique action that needs to be executed asynchronously during query rewriting.
 * A {@link QueryRewriteAsyncAction} is registered using
 * {@link QueryRewriteContext#registerUniqueRewriteAction(QueryRewriteAsyncAction, Consumer)}.
 * This is useful when we want to remove duplicate and costly async actions that take part in query rewriting, such as generating
 * embeddings for semantic search.
 * Since we need to determine whether an action has already been registered, we require implementors to provide implementations for
 * {@link #hashCode()} and {@link #equals(Object)},
 */
public abstract class QueryRewriteAsyncAction<T> {
    /**
     * The execute method will:
     * - Execute the action using {@link #execute(Client, ActionListener)}
     * - Pass the action result to the consumers and execute the consumers
     * - Call the final listener
     *
     * @param client An internal client that can be used during execution.
     * @param listener The listener that will be called after the action and consumers have been executed.
     * @param consumers A list of consumer that expect the result of the action.
     */
    @SuppressWarnings("unchecked")
    public void execute(Client client, ActionListener<?> listener, List<Consumer<?>> consumers) {
        execute(client, listener.delegateFailureAndWrap((l, result) -> {
            consumers.forEach(consumer -> ((Consumer<T>) consumer).accept(result));
            l.onResponse(null);
        }));
    }

    /**
     * This method will execute the async action and pass its result to the listener.
     *
     * @param client A rest client that can be used during execution.
     * @param listener The listener that will be called with the action result
     */
    protected abstract void execute(Client client, ActionListener<T> listener);

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);
}
