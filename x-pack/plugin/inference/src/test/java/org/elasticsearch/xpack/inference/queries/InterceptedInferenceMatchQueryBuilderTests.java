/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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
    protected void assertCoordinatorNodeRewriteOnInferenceField(
        QueryBuilder original,
        QueryBuilder rewritten,
        TransportVersion transportVersion
    ) {
        assertThat(original, instanceOf(MatchQueryBuilder.class));
        assertThat(rewritten, instanceOf(InterceptedInferenceMatchQueryBuilder.class));

        InterceptedInferenceMatchQueryBuilder intercepted = (InterceptedInferenceMatchQueryBuilder) rewritten;
        assertThat(intercepted.originalQuery, equalTo(original));
        assertThat(intercepted.inferenceResultsMap, notNullValue());
        assertFalse(intercepted.inferenceResultsMap.isEmpty());
    }

    @Override
    protected void assertCoordinatorNodeRewriteOnNonInferenceField(QueryBuilder original, QueryBuilder rewritten) {
        assertThat(original, instanceOf(MatchQueryBuilder.class));
        assertThat(rewritten, equalTo(original));
    }
}
