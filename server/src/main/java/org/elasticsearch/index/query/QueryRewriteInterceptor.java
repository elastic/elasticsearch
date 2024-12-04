/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import java.util.List;

/**
 * Enables modules and plugins to intercept and rewrite queries during the query rewrite phase on the coordinator node.
 */
public interface QueryRewriteInterceptor {

    /**
     * Returns a rewritten query if modifications are required; otherwise,
     * returns the same provided {@link QueryBuilder} instance unchanged.
     *
     * @param context the {@link QueryRewriteContext} providing the context for the rewrite operation
     * @param queryBuilder the original {@link QueryBuilder} to potentially rewrite
     * @return the rewritten {@link QueryBuilder}, or the original instance if no rewrite was needed
     */
    QueryBuilder rewrite(QueryRewriteContext context, QueryBuilder queryBuilder);

    static QueryRewriteInterceptor multi(List<QueryRewriteInterceptor> interceptors) {
        if (interceptors.isEmpty()) {
            return (c, q) -> q;
        }
        return new Multi(interceptors);
    }

    class Multi implements QueryRewriteInterceptor {
        final List<QueryRewriteInterceptor> interceptors;

        private Multi(List<QueryRewriteInterceptor> interceptors) {
            this.interceptors = interceptors;
        }

        @Override
        public QueryBuilder rewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
            for (var interceptor : interceptors) {
                var rewritten = interceptor.rewrite(context, queryBuilder);
                if (rewritten != queryBuilder) {
                    return rewritten;
                }
            }
            return queryBuilder;
        }
    }
}
