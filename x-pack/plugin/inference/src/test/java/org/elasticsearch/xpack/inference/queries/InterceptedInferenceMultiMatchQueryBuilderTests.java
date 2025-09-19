/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.Map;

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
        final String field1 = "field1";
        final String field2 = "field2";
        final String field3 = "field3";
        final String queryText = "test query";

        final float queryBoost = randomFloatBetween(1.1f, 3.0f, true);
        final float field1Boost = randomFloatBetween(1.1f, 2.0f, true);
        final float field2Boost = randomFloatBetween(1.1f, 2.0f, true);
        final float field3Boost = randomFloatBetween(1.1f, 2.0f, true);
        final String queryName = randomAlphanumericOfLength(6);

        final TestIndex testIndex1 = new TestIndex(
            "mixed-index-1",
            Map.of(),
            Map.of(
                field1, Map.of("type", "text"),
                field2, Map.of("type", "text"),
                field3, Map.of("type", "text")
            )
        );

        final TestIndex testIndex2 = new TestIndex(
            "mixed-index-2",
            Map.of(
                field2, SPARSE_INFERENCE_ID,
                field3, SPARSE_INFERENCE_ID
            ),
            Map.of(field1, Map.of("type", "text"))
        );

        final TestIndex testIndex3 = new TestIndex(
            "mixed-index-3",
            Map.of(
                field2, SPARSE_INFERENCE_ID,
                field3, DENSE_INFERENCE_ID
            ),
            Map.of(field1, Map.of("type", "text"))
        );

        final MultiMatchQueryBuilder multiMatchQuery = new MultiMatchQueryBuilder(queryText)
            .field(field1, field1Boost)
            .field(field2, field2Boost)
            .field(field3, field3Boost)
            .boost(queryBoost)
            .queryName(queryName);

        // Perform coordinator node rewrite
        final QueryRewriteContext queryRewriteContext = createQueryRewriteContext(
            Map.of(
                testIndex1.name(), testIndex1.semanticTextFields(),
                testIndex2.name(), testIndex2.semanticTextFields(),
                testIndex3.name(), testIndex3.semanticTextFields()
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
        assertTrue(coordinatorIntercepted.inferenceResultsMap.containsKey(DENSE_INFERENCE_ID));
        assertTrue(coordinatorIntercepted.inferenceResultsMap.containsKey(SPARSE_INFERENCE_ID));

        // Perform data node rewrite on test index 1
        final QueryRewriteContext indexMetadataContextIndex1 = createIndexMetadataContext(
            testIndex1.name(),
            testIndex1.semanticTextFields(),
            testIndex1.nonInferenceFields()
        );
        QueryBuilder dataRewrittenIndex1 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex1);
        // Should return original multi_match since no semantic fields
        assertThat(dataRewrittenIndex1, equalTo(multiMatchQuery));

        // Perform data node rewrite on test index 2
        final QueryRewriteContext indexMetadataContextIndex2 = createIndexMetadataContext(
            testIndex2.name(),
            testIndex2.semanticTextFields(),
            testIndex2.nonInferenceFields()
        );
        QueryBuilder dataRewrittenIndex2 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex2);

        // Create expected DisMax query for index2
        DisMaxQueryBuilder expectedIndex2 = QueryBuilders.disMaxQuery()
            .add(new SemanticQueryBuilder(field3, queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(field3Boost))
            .add(new SemanticQueryBuilder(field2, queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(field2Boost))
            .add(new MultiMatchQueryBuilder(queryText).field(field1, field1Boost).type(multiMatchQuery.type()).lenient(multiMatchQuery.lenient()))
            .tieBreaker(multiMatchQuery.type().tieBreaker())
            .boost(queryBoost)
            .queryName(queryName);

        assertThat(dataRewrittenIndex2, equalTo(expectedIndex2));

        // Perform data node rewrite on test index 3
        final QueryRewriteContext indexMetadataContextIndex3 = createIndexMetadataContext(
            testIndex3.name(),
            testIndex3.semanticTextFields(),
            testIndex3.nonInferenceFields()
        );
        QueryBuilder dataRewrittenIndex3 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextIndex3);

        // Create expected DisMax query for index3
        DisMaxQueryBuilder expectedIndex3 = QueryBuilders.disMaxQuery()
            .add(new SemanticQueryBuilder(field3, queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(field3Boost))
            .add(new SemanticQueryBuilder(field2, queryText, null, coordinatorIntercepted.inferenceResultsMap).boost(field2Boost))
            .add(new MultiMatchQueryBuilder(queryText).field(field1, field1Boost).type(multiMatchQuery.type()).lenient(multiMatchQuery.lenient()))
            .tieBreaker(multiMatchQuery.type().tieBreaker())
            .boost(queryBoost)
            .queryName(queryName);

        assertThat(dataRewrittenIndex3, equalTo(expectedIndex3));
    }
}
