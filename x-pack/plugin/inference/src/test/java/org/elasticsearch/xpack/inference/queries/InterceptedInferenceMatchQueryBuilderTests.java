/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class InterceptedInferenceMatchQueryBuilderTests extends AbstractInterceptedInferenceQueryBuilderTestCase<MatchQueryBuilder> {
    @Override
    protected MatchQueryBuilder createQueryBuilder(String field) {
        return new MatchQueryBuilder(field, "foo");
    }

    @Override
    protected QueryRewriteInterceptor createQueryRewriteInterceptor() {
        return new SemanticMatchQueryRewriteInterceptor();
    }

    @Override
    protected TransportVersion getMinimalSupportedVersion() {
        return new MatchQueryBuilder("foo", "bar").getMinimalSupportedVersion();
    }

    @Override
    protected void assertCoordinatorNodeRewriteOnInferenceField(
        QueryBuilder original,
        QueryBuilder rewritten,
        TransportVersion transportVersion,
        QueryRewriteContext queryRewriteContext
    ) {
        assertThat(original, instanceOf(MatchQueryBuilder.class));
        if (transportVersion.onOrAfter(TransportVersions.NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            assertThat(rewritten, instanceOf(InterceptedInferenceMatchQueryBuilder.class));

            InterceptedInferenceMatchQueryBuilder intercepted = (InterceptedInferenceMatchQueryBuilder) rewritten;
            assertThat(intercepted.originalQuery, equalTo(original));
            assertThat(intercepted.inferenceResultsMap, notNullValue());
            assertFalse(intercepted.inferenceResultsMap.isEmpty());
        } else {
            // Rewrite using the query rewrite context to populate the inference results
            QueryBuilder expectedLegacyIntercepted = new BwCSemanticMatchQueryRewriteInterceptor().interceptAndRewrite(
                queryRewriteContext,
                original
            );
            QueryBuilder expectedLegacyRewritten = rewriteAndFetch(expectedLegacyIntercepted, queryRewriteContext);
            assertThat(rewritten, equalTo(expectedLegacyRewritten));
        }
    }

    @Override
    protected void assertCoordinatorNodeRewriteOnNonInferenceField(QueryBuilder original, QueryBuilder rewritten) {
        assertThat(original, instanceOf(MatchQueryBuilder.class));
        assertThat(rewritten, equalTo(original));
    }
}
