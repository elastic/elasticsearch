/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SemanticKnnVectorQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_KNN_VECTOR_QUERY_FILTERS_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_knn_vector_query_filters_rewrite_interception_supported"
    );

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
        List<QueryBuilder> rewrittenFilters = new ArrayList<>(knnVectorQueryBuilder.filterQueries().size());
        for (QueryBuilder filter : knnVectorQueryBuilder.filterQueries()) {
            QueryBuilder rewritten = filter.rewrite(context);
            if (rewritten != filter) {
                changed = true;
            }
            rewrittenFilters.add(rewritten);
        }
        if (changed) {
            knnVectorQueryBuilder.setFilterQueries(rewrittenFilters);
        }
        return new InterceptedInferenceKnnVectorQueryBuilder(knnVectorQueryBuilder);
    }

    @Override
    public String getQueryName() {
        return KnnVectorQueryBuilder.NAME;
    }
}
