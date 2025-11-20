/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

import java.io.IOException;

public class SemanticKnnVectorQueryRewriteInterceptor implements QueryRewriteInterceptor {
    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) throws IOException {
        if (queryBuilder instanceof KnnVectorQueryBuilder knnVectorQueryBuilder) {
            return interceptKnnQuery(context, knnVectorQueryBuilder);
        } else {
            throw new IllegalStateException("Unexpected query builder type: " + queryBuilder.getClass());
        }
    }

    private static InterceptedInferenceKnnVectorQueryBuilder interceptKnnQuery(
        QueryRewriteContext context,
        KnnVectorQueryBuilder knnVectorQueryBuilder
    ) throws IOException {
        boolean changed = false;
        for (QueryBuilder filter : knnVectorQueryBuilder.filterQueries()) {
            QueryBuilder rewritten = filter.rewrite(context);
            if (rewritten != filter) {
                changed = true;
            }
        }
        if (changed) {
            knnVectorQueryBuilder.setFilterQueries(Rewriteable.rewrite(knnVectorQueryBuilder.filterQueries(), context));
        }
        return new InterceptedInferenceKnnVectorQueryBuilder(knnVectorQueryBuilder);
    }

    @Override
    public String getQueryName() {
        return KnnVectorQueryBuilder.NAME;
    }
}
