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
 */
public abstract class QueryRewriteAsyncAction {

    /**
     * The execute method will:
     * - Execute the action
     * - Pass the action result to the consumers and execute the consumers
     * - Call the listener
     *
     * @param client A rest client that can be used during execution.
     * @param listener The listener that will be called after the action and consumers have been executed.
     * @param consumers A list of consumer that expect the result of the action. It is the responsibility of the implementor to execute
     *                  the consumers.
     */
    public abstract void execute(Client client, ActionListener<?> listener, List<Consumer<Object>> consumers);

    private final QueryRewriteContext queryRewriteContext;

    public QueryRewriteAsyncAction(QueryRewriteContext queryRewriteContext) {
        this.queryRewriteContext = queryRewriteContext;
    }

    protected QueryRewriteContext getQueryRewriteContext() {
        return queryRewriteContext;
    }
}
