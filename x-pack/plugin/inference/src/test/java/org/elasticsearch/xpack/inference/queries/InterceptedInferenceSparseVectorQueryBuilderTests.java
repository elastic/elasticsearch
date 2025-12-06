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
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.search.TokenPruningConfigTests;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class InterceptedInferenceSparseVectorQueryBuilderTests extends AbstractInterceptedInferenceQueryBuilderTestCase<
    SparseVectorQueryBuilder> {

    private static final TransportVersion NEW_SEMANTIC_QUERY_INTERCEPTORS = TransportVersion.fromName("new_semantic_query_interceptors");

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
    protected InterceptedInferenceQueryBuilder<SparseVectorQueryBuilder> createInterceptedQueryBuilder(
        SparseVectorQueryBuilder originalQuery,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    ) {
        return new InterceptedInferenceSparseVectorQueryBuilder(originalQuery, inferenceResultsMap);
    }

    @Override
    protected List<QueryRewriteInterceptor> createQueryRewriteInterceptors() {
        return List.of(new SemanticSparseVectorQueryRewriteInterceptor());
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
        if (transportVersion.supports(NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            assertThat(rewritten, instanceOf(InterceptedInferenceSparseVectorQueryBuilder.class));

            InterceptedInferenceSparseVectorQueryBuilder intercepted = (InterceptedInferenceSparseVectorQueryBuilder) rewritten;
            assertThat(intercepted.originalQuery, equalTo(original));
            assertThat(intercepted.inferenceResultsMap, notNullValue());
            assertThat(intercepted.inferenceResultsMap.size(), equalTo(1));
        } else {
            // Rewrite using the query rewrite context to populate the inference results
            @SuppressWarnings("deprecation")
            QueryBuilder expectedLegacyIntercepted = new LegacySemanticSparseVectorQueryRewriteInterceptor().interceptAndRewrite(
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
        SparseVectorQueryBuilder originalSparseVectorWithQueryVector = buildExpectedSparseVectorQuery(
            originalSparseVector,
            interceptedQueryVector
        );
        assertThat(intercepted, equalTo(originalSparseVectorWithQueryVector));
    }

    public void testInterceptAndRewrite() throws Exception {
        final String field = "test_field";
        final TestIndex testIndex1 = new TestIndex("test-index-1", Map.of(field, SPARSE_INFERENCE_ID), Map.of());
        final TestIndex testIndex2 = new TestIndex("test-index-2", Map.of(), Map.of(field, Map.of("type", "sparse_vector")));
        final SparseVectorQueryBuilder sparseVectorQuery = createQueryBuilder(field);

        // Perform coordinator node rewrite
        final QueryRewriteContext queryRewriteContext = createQueryRewriteContext(
            Map.of(testIndex1.name(), testIndex1.semanticTextFields(), testIndex2.name(), testIndex2.semanticTextFields()),
            Map.of(),
            TransportVersion.current(),
            null
        );
        QueryBuilder coordinatorRewritten = rewriteAndFetch(sparseVectorQuery, queryRewriteContext);

        // Use a serialization cycle to strip InterceptedQueryBuilderWrapper
        coordinatorRewritten = copyNamedWriteable(coordinatorRewritten, writableRegistry(), QueryBuilder.class);
        assertThat(coordinatorRewritten, instanceOf(InterceptedInferenceSparseVectorQueryBuilder.class));
        InterceptedInferenceSparseVectorQueryBuilder coordinatorIntercepted =
            (InterceptedInferenceSparseVectorQueryBuilder) coordinatorRewritten;
        assertThat(coordinatorIntercepted.originalQuery, equalTo(sparseVectorQuery));
        assertThat(coordinatorIntercepted.inferenceResultsMap, notNullValue());
        assertThat(coordinatorIntercepted.inferenceResultsMap.size(), equalTo(1));

        InferenceResults inferenceResults = coordinatorIntercepted.inferenceResultsMap.get(
            new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, SPARSE_INFERENCE_ID)
        );
        assertThat(inferenceResults, notNullValue());
        assertThat(inferenceResults, instanceOf(TextExpansionResults.class));
        TextExpansionResults textExpansionResults = (TextExpansionResults) inferenceResults;

        // Perform data node rewrite on test index 1
        final QueryRewriteContext indexMetadataContextTestIndex1 = createIndexMetadataContext(
            testIndex1.name(),
            testIndex1.semanticTextFields(),
            testIndex1.nonInferenceFields()
        );
        QueryBuilder dataRewrittenTestIndex1 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextTestIndex1);
        NestedQueryBuilder expectedDataRewrittenTestIndex1 = buildExpectedNestedQuery(
            sparseVectorQuery,
            textExpansionResults.getWeightedTokens()
        );
        assertThat(dataRewrittenTestIndex1, equalTo(expectedDataRewrittenTestIndex1));

        // Perform data node rewrite on test index 2
        final QueryRewriteContext indexMetadataContextTestIndex2 = createIndexMetadataContext(
            testIndex2.name(),
            testIndex2.semanticTextFields(),
            testIndex2.nonInferenceFields()
        );
        QueryBuilder dataRewrittenTestIndex2 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextTestIndex2);
        SparseVectorQueryBuilder expectedDataRewrittenTestIndex2 = buildExpectedSparseVectorQuery(
            sparseVectorQuery,
            textExpansionResults.getWeightedTokens()
        );
        assertThat(dataRewrittenTestIndex2, equalTo(expectedDataRewrittenTestIndex2));
    }

    public void testCcsSerializationWithMinimizeRoundTripsFalse() throws Exception {
        ccsSerializationWithMinimizeRoundTripsFalseTestCase(TaskType.SPARSE_EMBEDDING, SparseVectorQueryBuilder.NAME);
    }

    private static NestedQueryBuilder buildExpectedNestedQuery(
        SparseVectorQueryBuilder sparseVectorQuery,
        List<WeightedToken> queryVector
    ) {
        SparseVectorQueryBuilder expectedInnerQuery = new SparseVectorQueryBuilder(
            SemanticTextField.getEmbeddingsFieldName(sparseVectorQuery.getFieldName()),
            queryVector,
            null,
            null,
            sparseVectorQuery.shouldPruneTokens(),
            sparseVectorQuery.getTokenPruningConfig()
        );

        return QueryBuilders.nestedQuery(
            SemanticTextField.getChunksFieldName(sparseVectorQuery.getFieldName()),
            expectedInnerQuery,
            ScoreMode.Max
        ).boost(sparseVectorQuery.boost()).queryName(sparseVectorQuery.queryName());
    }

    private static SparseVectorQueryBuilder buildExpectedSparseVectorQuery(
        SparseVectorQueryBuilder sparseVectorQuery,
        List<WeightedToken> queryVector
    ) {
        return new SparseVectorQueryBuilder(
            sparseVectorQuery.getFieldName(),
            queryVector,
            null,
            null,
            sparseVectorQuery.shouldPruneTokens(),
            sparseVectorQuery.getTokenPruningConfig()
        ).boost(sparseVectorQuery.boost()).queryName(sparseVectorQuery.queryName());
    }
}
