/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

public abstract class NewSemanticQueryRewriteInterceptor implements QueryRewriteInterceptor {
    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        // Don't do any rewriting except wrapping the original query in a InterceptedQueryBuilder
        // This way, any follow-up transformations will be handled by the InterceptedQueryBuilder rewrite cycle
        return null;
    }
}
