/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
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
        return new KnnVectorQueryBuilder(field, new TextEmbeddingQueryVectorBuilder(DENSE_INFERENCE_ID, "foo"), 30, 200, 30f, 0.2f).boost(
            randomFloatBetween(0.1f, 4.0f, true)
        )
            .queryName(randomAlphanumericOfLength(5))
            .addFilterQuery(new TermsQueryBuilder(IndexFieldMapper.NAME, randomAlphanumericOfLength(5)));
    }

    @Override
    protected InterceptedInferenceQueryBuilder<KnnVectorQueryBuilder> createInterceptedQueryBuilder(
        KnnVectorQueryBuilder originalQuery,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    ) {
        return new InterceptedInferenceKnnVectorQueryBuilder(
            new InterceptedInferenceKnnVectorQueryBuilder(originalQuery),
            inferenceResultsMap
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
            assertThat(intercepted.inferenceResultsMap.size(), equalTo(1));
        } else {
            // Rewrite using the query rewrite context to populate the inference results
            @SuppressWarnings("deprecation")
            QueryBuilder expectedLegacyIntercepted = new LegacySemanticKnnVectorQueryRewriteInterceptor().interceptAndRewrite(
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
        ).boost(originalKnn.boost()).queryName(originalKnn.queryName()).addFilterQueries(originalKnn.filterQueries());
        assertThat(intercepted, equalTo(originalKnnWithQueryVector));
    }

    public void testInterceptAndRewrite() throws Exception {
        final String field = "test_field";
        final TestIndex testIndex1 = new TestIndex("test-index-1", Map.of(field, DENSE_INFERENCE_ID), Map.of());
        final TestIndex testIndex2 = new TestIndex(
            "test-index-2",
            Map.of(),
            Map.of(
                field,
                Map.of(
                    "type",
                    "dense_vector",
                    "element_type",
                    DENSE_INFERENCE_ID_SETTINGS.elementType().toString(),
                    "dims",
                    DENSE_INFERENCE_ID_SETTINGS.dimensions()
                )
            )
        );
        final KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            field,
            new TextEmbeddingQueryVectorBuilder(DENSE_INFERENCE_ID, "foo"),
            50,
            500,
            50f,
            null
        ).boost(3.0f).queryName("bar").addFilterQuery(new TermsQueryBuilder(IndexFieldMapper.NAME, "test-index-*"));

        // Perform coordinator node rewrite
        final QueryRewriteContext queryRewriteContext = createQueryRewriteContext(
            Map.of(testIndex1.name(), testIndex1.semanticTextFields(), testIndex2.name(), testIndex2.semanticTextFields()),
            Map.of(),
            TransportVersion.current()
        );
        QueryBuilder coordinatorRewritten = rewriteAndFetch(knnQuery, queryRewriteContext);

        // Use a serialization cycle to strip InterceptedQueryBuilderWrapper
        coordinatorRewritten = copyNamedWriteable(coordinatorRewritten, writableRegistry(), QueryBuilder.class);
        assertThat(coordinatorRewritten, instanceOf(InterceptedInferenceKnnVectorQueryBuilder.class));
        InterceptedInferenceKnnVectorQueryBuilder coordinatorIntercepted = (InterceptedInferenceKnnVectorQueryBuilder) coordinatorRewritten;
        assertThat(coordinatorIntercepted.originalQuery, equalTo(knnQuery));
        assertThat(coordinatorIntercepted.inferenceResultsMap, notNullValue());
        assertThat(coordinatorIntercepted.inferenceResultsMap.size(), equalTo(1));

        InferenceResults inferenceResults = coordinatorIntercepted.inferenceResultsMap.get(
            new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, DENSE_INFERENCE_ID)
        );
        assertThat(inferenceResults, notNullValue());
        assertThat(inferenceResults, instanceOf(MlTextEmbeddingResults.class));
        VectorData queryVector = new VectorData(((MlTextEmbeddingResults) inferenceResults).getInferenceAsFloat());

        // Perform data node rewrite on test index 1
        final QueryRewriteContext indexMetadataContextTestIndex1 = createIndexMetadataContext(
            testIndex1.name(),
            testIndex1.semanticTextFields(),
            testIndex1.nonInferenceFields()
        );
        QueryBuilder dataRewrittenTestIndex1 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextTestIndex1);
        NestedQueryBuilder expectedDataRewrittenTestIndex1 = buildExpectedNestedQuery(
            knnQuery,
            queryVector,
            indexMetadataContextTestIndex1
        );
        assertThat(dataRewrittenTestIndex1, equalTo(expectedDataRewrittenTestIndex1));

        // Perform data node rewrite on test index 2
        final QueryRewriteContext indexMetadataContextTestIndex2 = createIndexMetadataContext(
            testIndex2.name(),
            testIndex2.semanticTextFields(),
            testIndex2.nonInferenceFields()
        );
        QueryBuilder dataRewrittenTestIndex2 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextTestIndex2);
        QueryBuilder expectedDataRewrittenTestIndex2 = buildExpectedKnnQuery(knnQuery, queryVector, indexMetadataContextTestIndex2);
        assertThat(dataRewrittenTestIndex2, equalTo(expectedDataRewrittenTestIndex2));
    }

    private static NestedQueryBuilder buildExpectedNestedQuery(
        KnnVectorQueryBuilder knnQuery,
        VectorData queryVector,
        QueryRewriteContext indexMetadataContext
    ) {
        List<QueryBuilder> rewrittenFilterQueries = rewriteFilterQueries(knnQuery, indexMetadataContext);
        boolean rewriteInnerToMatchNone = rewrittenFilterQueries.stream().anyMatch(q -> q instanceof MatchNoneQueryBuilder);

        QueryBuilder expectedInnerQuery;
        if (rewriteInnerToMatchNone) {
            expectedInnerQuery = new MatchNoneQueryBuilder();
        } else {
            expectedInnerQuery = new KnnVectorQueryBuilder(
                SemanticTextField.getEmbeddingsFieldName(knnQuery.getFieldName()),
                queryVector,
                knnQuery.k(),
                knnQuery.numCands(),
                knnQuery.visitPercentage(),
                knnQuery.rescoreVectorBuilder(),
                knnQuery.getVectorSimilarity()
            ).addFilterQueries(rewrittenFilterQueries);
        }

        return QueryBuilders.nestedQuery(SemanticTextField.getChunksFieldName(knnQuery.getFieldName()), expectedInnerQuery, ScoreMode.Max)
            .boost(knnQuery.boost())
            .queryName(knnQuery.queryName());
    }

    private static QueryBuilder buildExpectedKnnQuery(
        KnnVectorQueryBuilder knnQuery,
        VectorData queryVector,
        QueryRewriteContext indexMetadataContext
    ) {
        List<QueryBuilder> rewrittenFilterQueries = rewriteFilterQueries(knnQuery, indexMetadataContext);
        boolean rewriteToMatchNone = rewrittenFilterQueries.stream().anyMatch(q -> q instanceof MatchNoneQueryBuilder);

        QueryBuilder expectedQuery;
        if (rewriteToMatchNone) {
            expectedQuery = new MatchNoneQueryBuilder();
        } else {
            expectedQuery = new KnnVectorQueryBuilder(
                knnQuery.getFieldName(),
                queryVector,
                knnQuery.k(),
                knnQuery.numCands(),
                knnQuery.visitPercentage(),
                knnQuery.rescoreVectorBuilder(),
                knnQuery.getVectorSimilarity()
            ).boost(knnQuery.boost()).queryName(knnQuery.queryName()).addFilterQueries(rewrittenFilterQueries);
        }

        return expectedQuery;
    }

    private static List<QueryBuilder> rewriteFilterQueries(KnnVectorQueryBuilder knnQuery, QueryRewriteContext indexMetadataContext) {
        List<QueryBuilder> rewrittenFilterQueries = new ArrayList<>();
        for (QueryBuilder filterQuery : knnQuery.filterQueries()) {
            QueryBuilder rewrittenFilterQuery = rewriteAndFetch(filterQuery, indexMetadataContext);
            rewrittenFilterQueries.add(rewrittenFilterQuery);
        }

        return rewrittenFilterQueries;
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
