/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ParsingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A basic interface for rewriteable classes.
 */
public interface Rewriteable<T> {

    int MAX_REWRITE_ROUNDS = 16;

    /**
     * Rewrites this instance based on the provided context. The returned
     * objects will be the same instance as this if no changes during the
     * rewrite were applied.
     */
    T rewrite(QueryRewriteContext ctx) throws IOException;

    /**
     * Rewrites the given {@link Rewriteable} into its primitive form. Rewriteables that for instance fetch resources from remote hosts or
     * can simplify / optimize itself should do their heavy lifting during {@link #rewrite(QueryRewriteContext)}. This method
     * rewrites the rewriteable until it doesn't change anymore.
     * @param original the original rewriteable to rewrite
     * @param context the rewrite context to use
     * @throws IOException if an {@link IOException} occurs
     */
    static <T extends Rewriteable<T>> T rewrite(T original, QueryRewriteContext context) throws IOException {
        return rewrite(original, context, false);
    }

    /**
     * Rewrites the given {@link Rewriteable} into its primitive form. Rewriteables that for instance fetch resources from remote hosts or
     * can simplify / optimize itself should do their heavy lifting during
     * {@link #rewriteAndFetch(Rewriteable, QueryRewriteContext, ActionListener)} (QueryRewriteContext)}. This method rewrites the
     * rewriteable until it doesn't change anymore.
     * @param original the original rewriteable to rewrite
     * @param context the rewrite context to use
     * @param assertNoAsyncTasks if <code>true</code> the rewrite will fail if there are any pending async tasks on the context after the
     *                          rewrite. See {@link QueryRewriteContext#executeAsyncActions(ActionListener)} for details
     * @throws IOException if an {@link IOException} occurs
     */
    static <T extends Rewriteable<T>> T rewrite(T original, QueryRewriteContext context, boolean assertNoAsyncTasks) throws IOException {
        T builder = original;
        int iteration = 0;
        for (T rewrittenBuilder = builder.rewrite(context); rewrittenBuilder != builder; rewrittenBuilder = builder.rewrite(context)) {
            if (assertNoAsyncTasks && context.hasAsyncActions()) {
                throw new IllegalStateException("async actions are left after rewrite");
            }
            builder = rewrittenBuilder;
            if (iteration++ >= MAX_REWRITE_ROUNDS) {
                // this is some protection against user provided queries if they don't obey the contract of rewrite we allow 16 rounds
                // and then we fail to prevent infinite loops
                throw new IllegalStateException(
                    "too many rewrite rounds, rewriteable might return new objects even if they are not rewritten"
                );
            }
        }
        return builder;
    }

    /**
     * Rewrites the given rewriteable and fetches pending async tasks for each round before rewriting again.
     */
    static <T extends Rewriteable<T>> void rewriteAndFetch(T original, QueryRewriteContext context, ActionListener<T> rewriteResponse) {
        rewriteAndFetch(original, context, rewriteResponse, 0);
    }

    /**
     * Rewrites the given rewriteable and fetches pending async tasks for each round before rewriting again.
     */
    static <T extends Rewriteable<T>> void rewriteAndFetch(
        T original,
        QueryRewriteContext context,
        ActionListener<T> rewriteResponse,
        int iteration
    ) {
        T builder = original;
        try {
            for (T rewrittenBuilder = builder.rewrite(context); rewrittenBuilder != builder; rewrittenBuilder = builder.rewrite(context)) {
                builder = rewrittenBuilder;
                if (iteration++ >= MAX_REWRITE_ROUNDS) {
                    // this is some protection against user provided queries if they don't obey the contract of rewrite we allow 16 rounds
                    // and then we fail to prevent infinite loops
                    throw new IllegalStateException(
                        "too many rewrite rounds, rewriteable might return new objects even if they are not rewritten"
                    );
                }
                if (context.hasAsyncActions()) {
                    T finalBuilder = builder;
                    final int currentIterationNumber = iteration;
                    context.executeAsyncActions(
                        rewriteResponse.delegateFailureAndWrap((l, n) -> rewriteAndFetch(finalBuilder, context, l, currentIterationNumber))
                    );
                    return;
                }
            }
            rewriteResponse.onResponse(builder);
        } catch (IOException | IllegalArgumentException | ParsingException ex) {
            rewriteResponse.onFailure(ex);
        }
    }

    /**
     * Rewrites each element of the list until it doesn't change and returns a new list iff there is at least one element of the list that
     * changed during it's rewrite. Otherwise the given list instance is returned unchanged.
     */
    static <T extends Rewriteable<T>> List<T> rewrite(List<T> rewritables, QueryRewriteContext context) throws IOException {
        List<T> list = rewritables;
        boolean changed = false;
        if (rewritables != null && rewritables.isEmpty() == false) {
            list = new ArrayList<>(rewritables.size());
            for (T instance : rewritables) {
                T rewrite = rewrite(instance, context);
                if (instance != rewrite) {
                    changed = true;
                }
                list.add(rewrite);
            }
        }
        return changed ? list : rewritables;
    }
}
