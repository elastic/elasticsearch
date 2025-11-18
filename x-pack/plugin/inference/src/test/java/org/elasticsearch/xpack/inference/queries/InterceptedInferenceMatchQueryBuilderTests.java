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
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class InterceptedInferenceMatchQueryBuilderTests extends AbstractInterceptedInferenceQueryBuilderTestCase<MatchQueryBuilder> {

    private static final TransportVersion NEW_SEMANTIC_QUERY_INTERCEPTORS = TransportVersion.fromName("new_semantic_query_interceptors");

    @Override
    protected MatchQueryBuilder createQueryBuilder(String field) {
        return new MatchQueryBuilder(field, "foo").boost(randomFloatBetween(0.1f, 4.0f, true)).queryName(randomAlphanumericOfLength(5));
    }

    @Override
    protected InterceptedInferenceQueryBuilder<MatchQueryBuilder> createInterceptedQueryBuilder(
        MatchQueryBuilder originalQuery,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    ) {
        return new InterceptedInferenceMatchQueryBuilder(originalQuery, inferenceResultsMap);
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
    ) throws Exception {
        assertThat(original, instanceOf(MatchQueryBuilder.class));
        if (transportVersion.supports(NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            assertThat(rewritten, instanceOf(InterceptedInferenceMatchQueryBuilder.class));

            InterceptedInferenceMatchQueryBuilder intercepted = (InterceptedInferenceMatchQueryBuilder) rewritten;
            assertThat(intercepted.originalQuery, equalTo(original));
            assertThat(intercepted.inferenceResultsMap, notNullValue());
            assertFalse(intercepted.inferenceResultsMap.isEmpty());
        } else {
            // Rewrite using the query rewrite context to populate the inference results
            @SuppressWarnings("deprecation")
            QueryBuilder expectedLegacyIntercepted = new LegacySemanticMatchQueryRewriteInterceptor().interceptAndRewrite(
                queryRewriteContext,
                original
            );
            QueryBuilder expectedLegacyRewritten = rewriteAndFetch(expectedLegacyIntercepted, queryRewriteContext);

            // Run the expected query through a serialization cycle to align the inference results map representations
            QueryBuilder expectedLegacySerialized = copyNamedWriteable(
                expectedLegacyRewritten,
                writableRegistry(),
                QueryBuilder.class,
                transportVersion
            );

            assertThat(rewritten, equalTo(expectedLegacySerialized));
        }
    }

    @Override
    protected void assertCoordinatorNodeRewriteOnNonInferenceField(QueryBuilder original, QueryBuilder rewritten) {
        assertThat(original, instanceOf(MatchQueryBuilder.class));
        assertThat(rewritten, equalTo(original));
    }

    public void testInterceptAndRewrite() throws Exception {
        final String field = "test_field";
        final String queryText = "foo";
        final TestIndex testIndex1 = new TestIndex("test-index-1", Map.of(field, DENSE_INFERENCE_ID), Map.of());
        final TestIndex testIndex2 = new TestIndex("test-index-2", Map.of(field, SPARSE_INFERENCE_ID), Map.of());
        final TestIndex testIndex3 = new TestIndex("test-index-3", Map.of(), Map.of(field, Map.of("type", "text")));
        final MatchQueryBuilder matchQuery = new MatchQueryBuilder(field, queryText).boost(3.0f).queryName("bar");

        // Perform coordinator node rewrite
        final QueryRewriteContext queryRewriteContext = createQueryRewriteContext(
            Map.of(
                testIndex1.name(),
                testIndex1.semanticTextFields(),
                testIndex2.name(),
                testIndex2.semanticTextFields(),
                testIndex3.name(),
                testIndex3.semanticTextFields()
            ),
            Map.of(),
            TransportVersion.current(),
            null
        );
        QueryBuilder coordinatorRewritten = rewriteAndFetch(matchQuery, queryRewriteContext);

        // Use a serialization cycle to strip InterceptedQueryBuilderWrapper
        coordinatorRewritten = copyNamedWriteable(coordinatorRewritten, writableRegistry(), QueryBuilder.class);
        assertThat(coordinatorRewritten, instanceOf(InterceptedInferenceMatchQueryBuilder.class));
        InterceptedInferenceMatchQueryBuilder coordinatorIntercepted = (InterceptedInferenceMatchQueryBuilder) coordinatorRewritten;
        assertThat(coordinatorIntercepted.originalQuery, equalTo(matchQuery));
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

        final SemanticQueryBuilder expectedSemanticQuery = new SemanticQueryBuilder(
            field,
            queryText,
            null,
            coordinatorIntercepted.inferenceResultsMap
        ).boost(matchQuery.boost()).queryName(matchQuery.queryName());

        // Perform data node rewrite on test index 1
        final QueryRewriteContext indexMetadataContextTestIndex1 = createIndexMetadataContext(
            testIndex1.name(),
            testIndex1.semanticTextFields(),
            testIndex1.nonInferenceFields()
        );
        QueryBuilder dataRewrittenTestIndex1 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextTestIndex1);
        assertThat(dataRewrittenTestIndex1, equalTo(expectedSemanticQuery));

        // Perform data node rewrite on test index 2
        final QueryRewriteContext indexMetadataContextTestIndex2 = createIndexMetadataContext(
            testIndex2.name(),
            testIndex2.semanticTextFields(),
            testIndex2.nonInferenceFields()
        );
        QueryBuilder dataRewrittenTestIndex2 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextTestIndex2);
        assertThat(dataRewrittenTestIndex2, equalTo(expectedSemanticQuery));

        // Perform data node rewrite on test index 3
        final QueryRewriteContext indexMetadataContextTestIndex3 = createIndexMetadataContext(
            testIndex3.name(),
            testIndex3.semanticTextFields(),
            testIndex3.nonInferenceFields()
        );
        QueryBuilder dataRewrittenTestIndex3 = rewriteAndFetch(coordinatorIntercepted, indexMetadataContextTestIndex3);
        assertThat(dataRewrittenTestIndex3, equalTo(matchQuery));
    }
}
