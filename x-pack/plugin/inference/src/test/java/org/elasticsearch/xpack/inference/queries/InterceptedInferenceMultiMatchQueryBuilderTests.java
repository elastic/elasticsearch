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
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class InterceptedInferenceMultiMatchQueryBuilderTests extends AbstractInterceptedInferenceQueryBuilderTestCase<
    MultiMatchQueryBuilder> {
    @Override
    protected MultiMatchQueryBuilder createQueryBuilder(String field) {
        return new MultiMatchQueryBuilder("foo", field).boost(randomFloatBetween(0.1f, 4.0f, true))
            .queryName(randomAlphanumericOfLength(5));
    }

    @Override
    protected InterceptedInferenceQueryBuilder<MultiMatchQueryBuilder> createInterceptedQueryBuilder(
        MultiMatchQueryBuilder originalQuery,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    ) {
        return new InterceptedInferenceMultiMatchQueryBuilder(
            new InterceptedInferenceMultiMatchQueryBuilder(originalQuery),
            inferenceResultsMap
        );
    }

    @Override
    protected QueryRewriteInterceptor createQueryRewriteInterceptor() {
        return new SemanticMultiMatchQueryRewriteInterceptor();
    }

    @Override
    protected TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.NEW_SEMANTIC_QUERY_INTERCEPTORS;
    }

    @Override
    protected void assertCoordinatorNodeRewriteOnInferenceField(
        QueryBuilder original,
        QueryBuilder rewritten,
        TransportVersion transportVersion,
        QueryRewriteContext queryRewriteContext
    ) {
        assertThat(original, instanceOf(MultiMatchQueryBuilder.class));
        if (transportVersion.onOrAfter(TransportVersions.MULTI_MATCH_WITH_NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            assertThat(rewritten, instanceOf(InterceptedInferenceMultiMatchQueryBuilder.class));

            InterceptedInferenceMultiMatchQueryBuilder intercepted = (InterceptedInferenceMultiMatchQueryBuilder) rewritten;
            assertThat(intercepted.originalQuery, equalTo(original));
            assertThat(intercepted.inferenceResultsMap, notNullValue());
            assertFalse(intercepted.inferenceResultsMap.isEmpty());
        } else {
            // For older versions, multi_match should rewrite to the original query
            assertThat(rewritten, equalTo(original));
        }
    }

    @Override
    protected void assertCoordinatorNodeRewriteOnNonInferenceField(QueryBuilder original, QueryBuilder rewritten) {
        assertThat(original, instanceOf(MultiMatchQueryBuilder.class));
        assertThat(rewritten, equalTo(original));
    }

    public void testInterceptAndRewrite() throws Exception {
        TestData data = TestData.random();
        TestIndices indices = TestIndices.create(data, "mixed");

        final MultiMatchQueryBuilder multiMatchQuery = new MultiMatchQueryBuilder(data.queryText).field(data.field1, data.field1Boost)
            .field(data.field2, data.field2Boost)
            .field(data.field3, data.field3Boost)
            .boost(data.queryBoost)
            .queryName(data.queryName);

        // Perform coordinator node rewrite
        final QueryRewriteContext queryRewriteContext = createQueryRewriteContext(
            Map.of(
                indices.index1.name(),
                indices.index1.semanticTextFields(),
                indices.index2.name(),
                indices.index2.semanticTextFields(),
                indices.index3.name(),
                indices.index3.semanticTextFields()
            ),
            Map.of(),
            TransportVersion.current()
        );
        QueryBuilder coordinatorRewritten = rewriteAndFetch(multiMatchQuery, queryRewriteContext);

        // Use a serialization cycle to strip InterceptedQueryBuilderWrapper
        coordinatorRewritten = copyNamedWriteable(coordinatorRewritten, writableRegistry(), QueryBuilder.class);
        assertThat(coordinatorRewritten, instanceOf(InterceptedInferenceMultiMatchQueryBuilder.class));
        InterceptedInferenceMultiMatchQueryBuilder coordinatorIntercepted =
            (InterceptedInferenceMultiMatchQueryBuilder) coordinatorRewritten;
        assertThat(coordinatorIntercepted.originalQuery, equalTo(multiMatchQuery));
        assertThat(coordinatorIntercepted.inferenceResultsMap, notNullValue());
        assertThat(coordinatorIntercepted.inferenceResultsMap.size(), equalTo(2));
        assertTrue(
            coordinatorIntercepted.inferenceResultsMap.containsKey(
                new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, DENSE_INFERENCE_ID)
            )
        );
        assertTrue(
            coordinatorIntercepted.inferenceResultsMap.containsKey(
                new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, SPARSE_INFERENCE_ID)
            )
        );

        // Perform data node rewrite on test index 1
        final QueryRewriteContext indexMetadataContextIndex1 = createIndexMetadataContext(
            indices.index1.name(),
            indices.index1.semanticTextFields(),
            indices.index1.nonInferenceFields()
        );
        QueryBuilder dataRewrittenIndex1 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex1);
        // Should return original multi_match since no semantic fields
        assertThat(dataRewrittenIndex1, equalTo(multiMatchQuery));

        // Perform data node rewrite on test index 2
        final QueryRewriteContext indexMetadataContextIndex2 = createIndexMetadataContext(
            indices.index2.name(),
            indices.index2.semanticTextFields(),
            indices.index2.nonInferenceFields()
        );
        QueryBuilder dataRewrittenIndex2 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex2);

        // Create expected DisMax query for index2
        DisMaxQueryBuilder expectedIndex2 = QueryBuilders.disMaxQuery()
            .add(
                new SemanticQueryBuilder(data.field3, data.queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(
                    data.field3Boost
                )
            )
            .add(
                new SemanticQueryBuilder(data.field2, data.queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(
                    data.field2Boost
                )
            )
            .add(
                new MultiMatchQueryBuilder(data.queryText).field(data.field1, data.field1Boost)
                    .type(multiMatchQuery.type())
                    .lenient(multiMatchQuery.lenient())
            )
            .tieBreaker(multiMatchQuery.type().tieBreaker())
            .boost(data.queryBoost)
            .queryName(data.queryName);

        assertThat(dataRewrittenIndex2, equalTo(expectedIndex2));

        // Perform data node rewrite on test index 3
        final QueryRewriteContext indexMetadataContextIndex3 = createIndexMetadataContext(
            indices.index3.name(),
            indices.index3.semanticTextFields(),
            indices.index3.nonInferenceFields()
        );
        QueryBuilder dataRewrittenIndex3 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex3);

        // Create expected DisMax query for index3
        DisMaxQueryBuilder expectedIndex3 = QueryBuilders.disMaxQuery()
            .add(
                new SemanticQueryBuilder(data.field3, data.queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(
                    data.field3Boost
                )
            )
            .add(
                new SemanticQueryBuilder(data.field2, data.queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(
                    data.field2Boost
                )
            )
            .add(
                new MultiMatchQueryBuilder(data.queryText).field(data.field1, data.field1Boost)
                    .type(multiMatchQuery.type())
                    .lenient(multiMatchQuery.lenient())
            )
            .tieBreaker(multiMatchQuery.type().tieBreaker())
            .boost(data.queryBoost)
            .queryName(data.queryName);

        assertThat(dataRewrittenIndex3, equalTo(expectedIndex3));
    }

    public void testInterceptAndRewriteWithDefaultFields() throws Exception {
        TestData data = TestData.random();
        TestIndices indices = TestIndices.create(data, "default-mixed");

        final MultiMatchQueryBuilder multiMatchQuery = new MultiMatchQueryBuilder(data.queryText).boost(data.queryBoost)
            .queryName(data.queryName);

        // Create default field settings with boosts
        Settings defaultFieldSettings = Settings.builder()
            .putList(
                "index.query.default_field",
                data.field1 + "^" + data.field1Boost,
                data.field2 + "^" + data.field2Boost,
                data.field3 + "^" + data.field3Boost
            )
            .build();

        // Perform coordinator node rewrite with default field settings
        final QueryRewriteContext queryRewriteContext = createQueryRewriteContext(
            Map.of(
                indices.index1.name(),
                indices.index1.semanticTextFields(),
                indices.index2.name(),
                indices.index2.semanticTextFields(),
                indices.index3.name(),
                indices.index3.semanticTextFields()
            ),
            Map.of(),
            TransportVersion.current(),
            Map.of(
                indices.index1.name(),
                defaultFieldSettings,
                indices.index2.name(),
                defaultFieldSettings,
                indices.index3.name(),
                defaultFieldSettings
            )
        );
        QueryBuilder coordinatorRewritten = rewriteAndFetch(multiMatchQuery, queryRewriteContext);

        // Use a serialization cycle to strip InterceptedQueryBuilderWrapper
        coordinatorRewritten = copyNamedWriteable(coordinatorRewritten, writableRegistry(), QueryBuilder.class);
        assertThat(coordinatorRewritten, instanceOf(InterceptedInferenceMultiMatchQueryBuilder.class));
        InterceptedInferenceMultiMatchQueryBuilder coordinatorIntercepted =
            (InterceptedInferenceMultiMatchQueryBuilder) coordinatorRewritten;
        assertThat(coordinatorIntercepted.originalQuery, equalTo(multiMatchQuery));
        assertThat(coordinatorIntercepted.inferenceResultsMap, notNullValue());
        assertThat(coordinatorIntercepted.inferenceResultsMap.size(), equalTo(2));
        assertTrue(
            coordinatorIntercepted.inferenceResultsMap.containsKey(
                new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, DENSE_INFERENCE_ID)
            )
        );
        assertTrue(
            coordinatorIntercepted.inferenceResultsMap.containsKey(
                new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, SPARSE_INFERENCE_ID)
            )
        );

        // Perform data node rewrite on test index 1
        final QueryRewriteContext indexMetadataContextIndex1 = createIndexMetadataContext(
            indices.index1.name(),
            indices.index1.semanticTextFields(),
            indices.index1.nonInferenceFields(),
            defaultFieldSettings
        );
        QueryBuilder dataRewrittenIndex1 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex1);
        assertThat(dataRewrittenIndex1, equalTo(multiMatchQuery));

        // Perform data node rewrite on test index 2
        final QueryRewriteContext indexMetadataContextIndex2 = createIndexMetadataContext(
            indices.index2.name(),
            indices.index2.semanticTextFields(),
            indices.index2.nonInferenceFields(),
            defaultFieldSettings
        );
        QueryBuilder dataRewrittenIndex2 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex2);

        // Create expected DisMax query for index2 using default fields
        DisMaxQueryBuilder expectedIndex2 = QueryBuilders.disMaxQuery()
            .add(
                new SemanticQueryBuilder(data.field3, data.queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(
                    data.field3Boost
                )
            )
            .add(
                new SemanticQueryBuilder(data.field2, data.queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(
                    data.field2Boost
                )
            )
            .add(
                new MultiMatchQueryBuilder(data.queryText).field(data.field1, data.field1Boost)
                    .type(multiMatchQuery.type())
                    .lenient(multiMatchQuery.lenient())
            )
            .tieBreaker(multiMatchQuery.type().tieBreaker())
            .boost(data.queryBoost)
            .queryName(data.queryName);
        assertThat(dataRewrittenIndex2, equalTo(expectedIndex2));

        // Perform data node rewrite on test index 3
        final QueryRewriteContext indexMetadataContextIndex3 = createIndexMetadataContext(
            indices.index3.name(),
            indices.index3.semanticTextFields(),
            indices.index3.nonInferenceFields(),
            defaultFieldSettings
        );
        QueryBuilder dataRewrittenIndex3 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex3);

        // Create expected DisMax query for index3 using default fields
        DisMaxQueryBuilder expectedIndex3 = QueryBuilders.disMaxQuery()
            .add(
                new SemanticQueryBuilder(data.field3, data.queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(
                    data.field3Boost
                )
            )
            .add(
                new SemanticQueryBuilder(data.field2, data.queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(
                    data.field2Boost
                )
            )
            .add(
                new MultiMatchQueryBuilder(data.queryText).field(data.field1, data.field1Boost)
                    .type(multiMatchQuery.type())
                    .lenient(multiMatchQuery.lenient())
            )
            .tieBreaker(multiMatchQuery.type().tieBreaker())
            .boost(data.queryBoost)
            .queryName(data.queryName);
        assertThat(dataRewrittenIndex3, equalTo(expectedIndex3));
    }

    private record TestData(
        String field1,
        String field2,
        String field3,
        String queryText,
        float queryBoost,
        float field1Boost,
        float field2Boost,
        float field3Boost,
        String queryName
    ) {
        static TestData random() {
            return new TestData(
                "field1",
                "field2",
                "field3",
                "test query",
                randomFloatBetween(1.1f, 3.0f, true),
                randomFloatBetween(1.1f, 2.0f, true),
                randomFloatBetween(1.1f, 2.0f, true),
                randomFloatBetween(1.1f, 2.0f, true),
                randomAlphanumericOfLength(6)
            );
        }
    }

    private record TestIndices(TestIndex index1, TestIndex index2, TestIndex index3) {
        static TestIndices create(TestData data, String prefix) {
            return new TestIndices(
                new TestIndex(
                    prefix + "-index-1",
                    Map.of(),
                    Map.of(data.field1, Map.of("type", "text"), data.field2, Map.of("type", "text"), data.field3, Map.of("type", "text"))
                ),
                new TestIndex(
                    prefix + "-index-2",
                    Map.of(data.field2, SPARSE_INFERENCE_ID, data.field3, SPARSE_INFERENCE_ID),
                    Map.of(data.field1, Map.of("type", "text"))
                ),
                new TestIndex(
                    prefix + "-index-3",
                    Map.of(data.field2, SPARSE_INFERENCE_ID, data.field3, DENSE_INFERENCE_ID),
                    Map.of(data.field1, Map.of("type", "text"))
                )
            );
        }
    }
}
