/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.IVF_FORMAT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class InterceptedInferenceKnnVectorQueryBuilderTests extends AbstractInterceptedInferenceQueryBuilderTestCase<
    KnnVectorQueryBuilder> {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        List<Plugin> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(new FakeMlPlugin());
        return plugins;
    }

    @Override
    protected KnnVectorQueryBuilder createQueryBuilder(String field) {
        return new KnnVectorQueryBuilder(
            field,
            new TextEmbeddingQueryVectorBuilder(DENSE_INFERENCE_ID, "foo"),
            10,
            100,
            IVF_FORMAT.isEnabled() ? 10f : null,
            null
        );
    }

    @Override
    protected QueryRewriteInterceptor createQueryRewriteInterceptor() {
        return new SemanticKnnVectorQueryRewriteInterceptor();
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
        assertThat(original, instanceOf(KnnVectorQueryBuilder.class));
        if (transportVersion.onOrAfter(TransportVersions.NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            assertThat(rewritten, instanceOf(InterceptedInferenceKnnVectorQueryBuilder.class));

            InterceptedInferenceKnnVectorQueryBuilder intercepted = (InterceptedInferenceKnnVectorQueryBuilder) rewritten;
            assertThat(intercepted.originalQuery, equalTo(original));
            assertThat(intercepted.inferenceResultsMap, notNullValue());
            assertFalse(intercepted.inferenceResultsMap.isEmpty());
        } else {
            // Rewrite using the query rewrite context to populate the inference results
            QueryBuilder expectedLegacyIntercepted = new BwCSemanticKnnVectorQueryRewriteInterceptor().interceptAndRewrite(
                queryRewriteContext,
                original
            );
            QueryBuilder expectedLegacyRewritten = rewriteAndFetch(expectedLegacyIntercepted, queryRewriteContext);
            assertThat(rewritten, equalTo(expectedLegacyRewritten));
        }

    }

    @Override
    protected void assertCoordinatorNodeRewriteOnNonInferenceField(QueryBuilder original, QueryBuilder rewritten) {
        assertThat(original, instanceOf(KnnVectorQueryBuilder.class));
        assertThat(rewritten, instanceOf(KnnVectorQueryBuilder.class));
        KnnVectorQueryBuilder originalKnn = (KnnVectorQueryBuilder) original;
        KnnVectorQueryBuilder intercepted = (KnnVectorQueryBuilder) rewritten;

        VectorData interceptedQueryVector = intercepted.queryVector();
        assertThat(interceptedQueryVector, notNullValue());
        assertThat(interceptedQueryVector.floatVector().length, equalTo(DENSE_INFERENCE_ID_SETTINGS.dimensions()));

        // The rewrite replaced the query vector builder with a query vector, so we can't directly compare the original query to the
        // rewritten one. Make a version of the original with the query vector that we can use for comparison.
        KnnVectorQueryBuilder originalKnnWithQueryVector = new KnnVectorQueryBuilder(
            originalKnn.getFieldName(),
            interceptedQueryVector,
            originalKnn.k(),
            originalKnn.numCands(),
            originalKnn.visitPercentage(),
            originalKnn.rescoreVectorBuilder(),
            originalKnn.getVectorSimilarity()
        );
        assertThat(intercepted, equalTo(originalKnnWithQueryVector));
    }

    private static class FakeMlPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QueryVectorBuilderSpec<?>> getQueryVectorBuilders() {
            return List.of(
                new QueryVectorBuilderSpec<>(
                    TextEmbeddingQueryVectorBuilder.NAME,
                    TextEmbeddingQueryVectorBuilder::new,
                    TextEmbeddingQueryVectorBuilder.PARSER
                )
            );
        }
    }
}
