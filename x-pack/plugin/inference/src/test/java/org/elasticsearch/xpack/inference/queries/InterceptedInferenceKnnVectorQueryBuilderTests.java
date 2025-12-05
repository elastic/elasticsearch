/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.search.TokenPruningConfigTests;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class InterceptedInferenceKnnVectorQueryBuilderTests extends AbstractInterceptedInferenceQueryBuilderTestCase<
    KnnVectorQueryBuilder> {

    private static final TransportVersion NEW_SEMANTIC_QUERY_INTERCEPTORS = TransportVersion.fromName("new_semantic_query_interceptors");

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        List<Plugin> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(new FakeMlPlugin());
        plugins.add(new XPackPlugin(Settings.EMPTY));
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
        return new InterceptedInferenceKnnVectorQueryBuilder(originalQuery, inferenceResultsMap);
    }

    @Override
    protected List<QueryRewriteInterceptor> createQueryRewriteInterceptors() {
        return List.of(
            new SemanticKnnVectorQueryRewriteInterceptor(),
            new SemanticMatchQueryRewriteInterceptor(),
            new SemanticSparseVectorQueryRewriteInterceptor()
        );
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
        if (transportVersion.supports(NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
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
            TransportVersion.current(),
            null
        );
        QueryBuilder coordinatorRewritten = rewriteAndFetch(knnQuery, queryRewriteContext);

        // Use a serialization cycle to strip InterceptedQueryBuilderWrapper
        coordinatorRewritten = copyNamedWriteable(coordinatorRewritten, writableRegistry(), QueryBuilder.class);

        MlDenseEmbeddingResults inferenceResults = assertQueryIsInterceptedKnnWithValidResults(coordinatorRewritten);
        VectorData queryVector = new VectorData(inferenceResults.getInferenceAsFloat());

        // Perform data node rewrite on test index 1
        final QueryRewriteContext indexMetadataContextTestIndex1 = createIndexMetadataContext(
            testIndex1.name(),
            testIndex1.semanticTextFields(),
            testIndex1.nonInferenceFields()
        );
        QueryBuilder dataRewrittenTestIndex1 = rewriteAndFetch(coordinatorRewritten, indexMetadataContextTestIndex1);
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
        QueryBuilder dataRewrittenTestIndex2 = rewriteAndFetch(coordinatorRewritten, indexMetadataContextTestIndex2);
        QueryBuilder expectedDataRewrittenTestIndex2 = buildExpectedKnnQuery(knnQuery, queryVector, indexMetadataContextTestIndex2);
        assertThat(dataRewrittenTestIndex2, equalTo(expectedDataRewrittenTestIndex2));
    }

    public void testCoordinatorNodeRewrite_GivenKnnQueryWithSemanticFilters_ShouldInterceptFilters() throws Exception {
        final String denseField1 = "dense_field_1";
        final String denseField2 = "dense_field_2";
        final String sparseField = "sparse_field";
        final TestIndex testIndex = new TestIndex(
            "test-index",
            Map.of(denseField1, DENSE_INFERENCE_ID, denseField2, DENSE_INFERENCE_ID, sparseField, SPARSE_INFERENCE_ID),
            Map.of()
        );
        final KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            denseField1,
            new TextEmbeddingQueryVectorBuilder(DENSE_INFERENCE_ID, "foo"),
            50,
            500,
            50f,
            null
        ).boost(3.0f)
            .queryName("bar")
            .addFilterQuery(
                QueryBuilders.boolQuery()
                    .filter(
                        new KnnVectorQueryBuilder(
                            denseField2,
                            new TextEmbeddingQueryVectorBuilder(DENSE_INFERENCE_ID, "some query"),
                            50,
                            500,
                            50f,
                            null
                        )
                    )
            )
            .addFilterQuery(
                new KnnVectorQueryBuilder(
                    denseField2,
                    new TextEmbeddingQueryVectorBuilder(DENSE_INFERENCE_ID, "some query"),
                    50,
                    500,
                    50f,
                    null
                ).addFilterQuery(
                    new SparseVectorQueryBuilder(
                        sparseField,
                        null,
                        SPARSE_INFERENCE_ID,
                        "some other query",
                        randomBoolean(),
                        TokenPruningConfigTests.testInstance()
                    )
                )
            )
            .addFilterQuery(new MatchQueryBuilder(sparseField, "some other query"));

        // Perform coordinator node rewrite
        final QueryRewriteContext queryRewriteContext = createQueryRewriteContext(
            Map.of(testIndex.name(), testIndex.semanticTextFields()),
            Map.of(),
            TransportVersion.current(),
            null
        );
        QueryBuilder coordinatorRewritten = rewriteAndFetch(knnQuery, queryRewriteContext);

        // Use a serialization cycle to strip InterceptedQueryBuilderWrapper
        coordinatorRewritten = copyNamedWriteable(coordinatorRewritten, writableRegistry(), QueryBuilder.class);
        QueryBuilder serializedKnnQuery = copyNamedWriteable(knnQuery, writableRegistry(), QueryBuilder.class);

        assertQueryIsInterceptedKnnWithValidResults(coordinatorRewritten);
        InterceptedInferenceKnnVectorQueryBuilder coordinatorIntercepted = (InterceptedInferenceKnnVectorQueryBuilder) coordinatorRewritten;
        assertThat(coordinatorIntercepted.originalQuery, equalTo(serializedKnnQuery));

        assertThat(coordinatorIntercepted.originalQuery.filterQueries(), hasSize(3));

        // Assertions on first filter
        {
            assertThat(coordinatorIntercepted.originalQuery.filterQueries().get(0), instanceOf(BoolQueryBuilder.class));
            BoolQueryBuilder filter = (BoolQueryBuilder) coordinatorIntercepted.originalQuery.filterQueries().get(0);
            assertThat(filter.filter(), hasSize(1));
            assertQueryIsInterceptedKnnWithValidResults(filter.filter().get(0));
        }

        // Assertions on second filter
        {
            assertQueryIsInterceptedKnnWithValidResults(coordinatorIntercepted.originalQuery.filterQueries().get(1));
            InterceptedInferenceKnnVectorQueryBuilder filter =
                (InterceptedInferenceKnnVectorQueryBuilder) coordinatorIntercepted.originalQuery.filterQueries().get(1);
            assertThat(filter.originalQuery.filterQueries(), hasSize(1));
            assertQueryIsInterceptedSparseVectorWithValidResults(filter.originalQuery.filterQueries().get(0));
        }

        // Assertions on third filter
        {
            assertThat(
                coordinatorIntercepted.originalQuery.filterQueries().get(2),
                instanceOf(InterceptedInferenceMatchQueryBuilder.class)
            );
            InterceptedInferenceMatchQueryBuilder filter = (InterceptedInferenceMatchQueryBuilder) coordinatorIntercepted.originalQuery
                .filterQueries()
                .get(2);
            assertInterceptedQueryHasValidResultsForSparseVector(filter);
        }
    }

    public void testCcsSerializationWithMinimizeRoundTripsFalse() throws Exception {
        ccsSerializationWithMinimizeRoundTripsFalseTestCase(TaskType.TEXT_EMBEDDING, KnnVectorQueryBuilder.NAME);
    }

    private static MlDenseEmbeddingResults assertQueryIsInterceptedKnnWithValidResults(QueryBuilder query) {
        assertThat(query, instanceOf(InterceptedInferenceKnnVectorQueryBuilder.class));
        InterceptedInferenceKnnVectorQueryBuilder interceptedKnn = (InterceptedInferenceKnnVectorQueryBuilder) query;
        assertThat(interceptedKnn.inferenceResultsMap, notNullValue());
        assertThat(interceptedKnn.inferenceResultsMap.size(), equalTo(1));
        InferenceResults inferenceResults = interceptedKnn.inferenceResultsMap.get(
            new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, DENSE_INFERENCE_ID)
        );
        assertThat(inferenceResults, notNullValue());
        assertThat(inferenceResults, instanceOf(MlDenseEmbeddingResults.class));
        return (MlDenseEmbeddingResults) inferenceResults;
    }

    private static void assertQueryIsInterceptedSparseVectorWithValidResults(QueryBuilder query) {
        assertThat(query, instanceOf(InterceptedInferenceSparseVectorQueryBuilder.class));
        assertInterceptedQueryHasValidResultsForSparseVector((InterceptedInferenceSparseVectorQueryBuilder) query);
    }

    private static void assertInterceptedQueryHasValidResultsForSparseVector(InterceptedInferenceQueryBuilder<?> intercepted) {
        assertThat(intercepted.inferenceResultsMap, notNullValue());
        assertThat(intercepted.inferenceResultsMap.size(), equalTo(1));
        InferenceResults inferenceResults = intercepted.inferenceResultsMap.get(
            new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, SPARSE_INFERENCE_ID)
        );
        assertThat(inferenceResults, notNullValue());
        assertThat(inferenceResults, instanceOf(TextExpansionResults.class));
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
