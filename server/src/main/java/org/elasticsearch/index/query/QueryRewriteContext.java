/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version.
 */
public class QueryRewriteContext {
    private final NamedXContentRegistry xContentRegistry;
    private final NamedWriteableRegistry writeableRegistry;
    protected final Client client;
    protected final LongSupplier nowInMillis;
    private final List<BiConsumer<Client, ActionListener<?>>> asyncActions = new ArrayList<>();

    public QueryRewriteContext(
            NamedXContentRegistry xContentRegistry, NamedWriteableRegistry writeableRegistry,Client client,
            LongSupplier nowInMillis) {

        this.xContentRegistry = xContentRegistry;
        this.writeableRegistry = writeableRegistry;
        this.client = client;
        this.nowInMillis = nowInMillis;
    }

    /**
     * The registry used to build new {@link XContentParser}s. Contains registered named parsers needed to parse the query.
     */
    public NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }

    /**
     * Returns the time in milliseconds that is shared across all resources involved. Even across shards and nodes.
     */
    public long nowInMillis() {
        return nowInMillis.getAsLong();
    }

    public NamedWriteableRegistry getWriteableRegistry() {
        return writeableRegistry;
    }

    /**
     * Returns an instance of {@link SearchExecutionContext} if available of null otherwise
     */
    public SearchExecutionContext convertToSearchExecutionContext() {
        return null;
    }

    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return null;
    }

    /**
     * Registers an async action that must be executed before the next rewrite round in order to make progress.
     * This should be used if a rewriteabel needs to fetch some external resources in order to be executed ie. a document
     * from an index.
     */
    public void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction) {
        asyncActions.add(asyncAction);
    }

    /**
     * Returns <code>true</code> if there are any registered async actions.
     */
    public boolean hasAsyncActions() {
        return asyncActions.isEmpty() == false;
    }

    /**
     * Executes all registered async actions and notifies the listener once it's done. The value that is passed to the listener is always
     * <code>null</code>. The list of registered actions is cleared once this method returns.
     */
    public void executeAsyncActions(ActionListener<Object> listener) {
        if (asyncActions.isEmpty()) {
            listener.onResponse(null);
        } else {
            CountDown countDown = new CountDown(asyncActions.size());
            ActionListener<Object> internalListener = new ActionListener<Object>() {
                @Override
                public void onResponse(Object o) {
                    if (countDown.countDown()) {
                        listener.onResponse(null);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (countDown.fastForward()) {
                        listener.onFailure(e);
                    }
                }
            };
            // make a copy to prevent concurrent modification exception
            List<BiConsumer<Client, ActionListener<?>>> biConsumers = new ArrayList<>(asyncActions);
            asyncActions.clear();
            for (BiConsumer<Client, ActionListener<?>> action : biConsumers) {
                action.accept(client, internalListener);
            }
        }
    }

}
