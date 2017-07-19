/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;

import java.io.IOException;

/**
 * A basic interface for rewriteable classes.
 */
public interface Rewriteable<T> {

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
     * can simplify / optimize itself should do their heavy lifting during {@link #rewrite(QueryRewriteContext)}. This method
     * rewrites the rewriteable until it doesn't change anymore.
     * @param original the original rewriteable to rewrite
     * @param context the rewrite context to use
     * @param assertNoAsyncTasks if <code>true</code> the rewrite will fail if there are any pending async tasks on the context after the
     *                          rewrite. See {@link QueryRewriteContext#executeAsyncActions(ActionListener)} for detals
     * @throws IOException if an {@link IOException} occurs
     */
    static <T extends Rewriteable<T>> T rewrite(T original, QueryRewriteContext context, boolean assertNoAsyncTasks) throws IOException {
        T builder = original;
        for (T rewrittenBuilder = builder.rewrite(context); rewrittenBuilder != builder;
             rewrittenBuilder = builder.rewrite(context)) {
            if (assertNoAsyncTasks && context.hasAsyncActions()) {
                throw new IllegalStateException("async actions are left after rewrite");
            }
            builder = rewrittenBuilder;
        }
        return builder;
    }

    /**
     * Rewrites the given rewriteable and fetches pending async tasks for each round before rewriting again.
     */
    static <T extends Rewriteable<T>> void rewriteAndFetch(T original, QueryRewriteContext context, ActionListener<T> rewriteResponse) {
        T builder = original;
        try {
            for (T rewrittenBuilder = builder.rewrite(context); rewrittenBuilder != builder;
                 rewrittenBuilder = builder.rewrite(context)) {
                builder = rewrittenBuilder;
                if (context.hasAsyncActions()) {
                    T finalBuilder = builder;
                    context.executeAsyncActions(ActionListener.wrap(n -> rewriteAndFetch(finalBuilder, context, rewriteResponse),
                        rewriteResponse::onFailure));
                    return;
                }
            }
            rewriteResponse.onResponse(builder);
        } catch (IOException ex) {
            rewriteResponse.onFailure(ex);
        }
    }
}
