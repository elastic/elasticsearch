/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.search.TokenPruningConfigTests;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class InterceptedInferenceSparseVectorQueryBuilderTests extends AbstractInterceptedInferenceQueryBuilderTestCase<
    SparseVectorQueryBuilder> {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        List<Plugin> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(new XPackPlugin(Settings.EMPTY));
        return plugins;
    }

    @Override
    protected SparseVectorQueryBuilder createQueryBuilder(String field) {
        return new SparseVectorQueryBuilder(
            field,
            null,
            SPARSE_INFERENCE_ID,
            "foo",
            randomBoolean(),
            TokenPruningConfigTests.testInstance()
        ).boost(randomFloatBetween(0.1f, 4.0f, true)).queryName(randomAlphanumericOfLength(5));
    }

    @Override
    protected QueryRewriteInterceptor createQueryRewriteInterceptor() {
        return new SemanticSparseVectorQueryRewriteInterceptor();
    }

    @Override
    protected TransportVersion getMinimalSupportedVersion() {
        return createQueryBuilder("foo").getMinimalSupportedVersion();
    }

    @Override
    protected void assertCoordinatorNodeRewriteOnInferenceField(
        QueryBuilder original,
        QueryBuilder rewritten,
        TransportVersion transportVersion,
        QueryRewriteContext queryRewriteContext
    ) {
        assertThat(original, instanceOf(SparseVectorQueryBuilder.class));
        if (transportVersion.onOrAfter(TransportVersions.NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            assertThat(rewritten, instanceOf(InterceptedInferenceSparseVectorQueryBuilder.class));

            InterceptedInferenceSparseVectorQueryBuilder intercepted = (InterceptedInferenceSparseVectorQueryBuilder) rewritten;
            assertThat(intercepted.originalQuery, equalTo(original));
            assertThat(intercepted.inferenceResultsMap, notNullValue());
            assertThat(intercepted.inferenceResultsMap.size(), equalTo(1));
        } else {
            // Rewrite using the query rewrite context to populate the inference results
            QueryBuilder expectedLegacyIntercepted = new BwCSemanticSparseVectorQueryRewriteInterceptor().interceptAndRewrite(
                queryRewriteContext,
                original
            );
            QueryBuilder expectedLegacyRewritten = rewriteAndFetch(expectedLegacyIntercepted, queryRewriteContext);
            assertThat(rewritten, equalTo(expectedLegacyRewritten));
        }
    }

    @Override
    protected void assertCoordinatorNodeRewriteOnNonInferenceField(QueryBuilder original, QueryBuilder rewritten) {
        assertThat(original, instanceOf(SparseVectorQueryBuilder.class));
        assertThat(rewritten, instanceOf(SparseVectorQueryBuilder.class));
        SparseVectorQueryBuilder originalSparseVector = (SparseVectorQueryBuilder) original;
        SparseVectorQueryBuilder intercepted = (SparseVectorQueryBuilder) rewritten;

        List<WeightedToken> interceptedQueryVector = intercepted.getQueryVectors();
        assertThat(interceptedQueryVector, notNullValue());

        // The rewrite replaced the inference ID with a query vector, so we can't directly compare the original query to the rewritten one.
        // Make a version of the original with the query vector that we can use for comparison.
        SparseVectorQueryBuilder originalSparseVectorWithQueryVector = new SparseVectorQueryBuilder(
            originalSparseVector.getFieldName(),
            interceptedQueryVector,
            null,
            null,
            originalSparseVector.shouldPruneTokens(),
            originalSparseVector.getTokenPruningConfig()
        ).boost(originalSparseVector.boost()).queryName(originalSparseVector.queryName());
        assertThat(intercepted, equalTo(originalSparseVectorWithQueryVector));
    }
}
