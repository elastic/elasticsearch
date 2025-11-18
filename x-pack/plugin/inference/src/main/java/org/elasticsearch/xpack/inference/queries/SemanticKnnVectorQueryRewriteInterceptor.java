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
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

public class SemanticKnnVectorQueryRewriteInterceptor implements QueryRewriteInterceptor {
    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        if (queryBuilder instanceof KnnVectorQueryBuilder knnVectorQueryBuilder) {
            return new InterceptedInferenceKnnVectorQueryBuilder(knnVectorQueryBuilder);
        } else {
            throw new IllegalStateException("Unexpected query builder type: " + queryBuilder.getClass());
        }
    }

    @Override
    public String getQueryName() {
        return KnnVectorQueryBuilder.NAME;
    }
}
