/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.inference.queries.SemanticMultiMatchQueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class SemanticMultiMatchQueryRewriteInterceptorTests extends ESTestCase {

    private TestThreadPool threadPool;
    private NoOpClient client;
    private Index index;

    private static final String FIELD_NAME_1 = "field1";
    private static final String FIELD_NAME_2 = "field2";
    private static final String NON_INFERENCE_FIELD = "text_field";
    private static final String VALUE = "test query";
    private static final String QUERY_NAME = "multi_match_query";
    private static final float BOOST = 3.5f;
    private static final float FIELD_BOOST_1 = 2.0f;
    private static final float FIELD_BOOST_2 = 1.5f;

    @Before
    public void setup() {
        threadPool = createThreadPool();
        client = new NoOpClient(threadPool);
        index = new Index(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @After
    public void cleanup() {
        threadPool.close();
    }

    public void testMultiMatchQueryOnSingleInferenceFieldIsRewrittenToSemanticQuery() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME_1,
            new InferenceFieldMetadata(index.getName(), "inferenceId", new String[] { FIELD_NAME_1 }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        MultiMatchQueryBuilder original = createTestQueryBuilder().fields(Map.of(FIELD_NAME_1, 1.0f));
        QueryBuilder rewritten = original.rewrite(context);

        assertTrue(
            "Expected query to be intercepted, but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof InterceptedQueryBuilderWrapper
        );
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof SemanticQueryBuilder);
        SemanticQueryBuilder semanticQueryBuilder = (SemanticQueryBuilder) intercepted.queryBuilder;
        assertEquals(FIELD_NAME_1, semanticQueryBuilder.getFieldName());
        assertEquals(VALUE, semanticQueryBuilder.getQuery());
    }

    public void testMultiMatchQueryOnMultipleInferenceFieldsIsRewrittenToDisMaxQuery() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME_1, new InferenceFieldMetadata(index.getName(), "inferenceId1", new String[] { FIELD_NAME_1 }, null),
            FIELD_NAME_2, new InferenceFieldMetadata(index.getName(), "inferenceId2", new String[] { FIELD_NAME_2 }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        MultiMatchQueryBuilder original = createTestQueryBuilder().fields(
            Map.of(FIELD_NAME_1, FIELD_BOOST_1, FIELD_NAME_2, FIELD_BOOST_2)
        );
        QueryBuilder rewritten = original.rewrite(context);

        assertTrue(
            "Expected query to be intercepted, but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof InterceptedQueryBuilderWrapper
        );
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof DisMaxQueryBuilder);
        DisMaxQueryBuilder disMaxQuery = (DisMaxQueryBuilder) intercepted.queryBuilder;
        assertEquals(2, disMaxQuery.innerQueries().size());

        for (QueryBuilder innerQuery : disMaxQuery.innerQueries()) {
            assertTrue(innerQuery instanceof SemanticQueryBuilder);
            SemanticQueryBuilder semanticQuery = (SemanticQueryBuilder) innerQuery;
            assertEquals(VALUE, semanticQuery.getQuery());
            assertTrue(semanticQuery.getFieldName().equals(FIELD_NAME_1) || semanticQuery.getFieldName().equals(FIELD_NAME_2));
        }
    }

    public void testMultiMatchQueryOnNonInferenceFieldRemainsUnchanged() throws IOException {
        QueryRewriteContext context = createQueryRewriteContext(Map.of()); // No inference fields
        MultiMatchQueryBuilder original = createTestQueryBuilder().fields(Map.of(NON_INFERENCE_FIELD, 1.0f));
        QueryBuilder rewritten = original.rewrite(context);

        assertTrue(
            "Expected query to remain MultiMatchQueryBuilder but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof MultiMatchQueryBuilder
        );
        assertEquals(original, rewritten);
    }

    public void testUnsupportedQueryTypeCrossFields() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME_1,
            new InferenceFieldMetadata(index.getName(), "inferenceId", new String[] { FIELD_NAME_1 }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);

        {
            MultiMatchQueryBuilder original = createTestQueryBuilder()
                .fields(Map.of(FIELD_NAME_1, 1.0f))
                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);


            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> original.rewrite(context));
            assertThat(exception.getMessage(), containsString("multi_match query with type [cross_fields] is not supported"));
        }

        {
            MultiMatchQueryBuilder original = createTestQueryBuilder()
                .fields(Map.of(FIELD_NAME_1, 1.0f))
                .type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);

            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> original.rewrite(context));
            assertThat(exception.getMessage(), containsString("multi_match query with type [bool_prefix] is not supported"));
        }

        {
            MultiMatchQueryBuilder original = createTestQueryBuilder()
                .fields(Map.of(FIELD_NAME_1, 1.0f))
                .type(MultiMatchQueryBuilder.Type.PHRASE);

            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> original.rewrite(context));
            assertThat(exception.getMessage(), containsString("multi_match query with type [phrase] is not supported"));
        }

        {
            MultiMatchQueryBuilder original = createTestQueryBuilder()
                .fields(Map.of(FIELD_NAME_1, 1.0f))
                .type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX);

            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> original.rewrite(context));
            assertThat(exception.getMessage(), containsString("multi_match query with type [phrase_prefix] is not supported"));
        }
    }

    public void testSupportedQueryTypeBestFields() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME_1,
            new InferenceFieldMetadata(index.getName(), "inferenceId", new String[] { FIELD_NAME_1 }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        MultiMatchQueryBuilder original = createTestQueryBuilder()
            .fields(Map.of(FIELD_NAME_1, 1.0f))
            .type(MultiMatchQueryBuilder.Type.BEST_FIELDS);
        QueryBuilder rewritten = original.rewrite(context);

        assertTrue(rewritten instanceof InterceptedQueryBuilderWrapper);
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof SemanticQueryBuilder);
    }

    public void testSupportedQueryTypeMostFields() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME_1,
            new InferenceFieldMetadata(index.getName(), "inferenceId", new String[] { FIELD_NAME_1 }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        MultiMatchQueryBuilder original = createTestQueryBuilder()
            .fields(Map.of(FIELD_NAME_1, 1.0f))
            .type(MultiMatchQueryBuilder.Type.MOST_FIELDS);
        QueryBuilder rewritten = original.rewrite(context);

        assertTrue(rewritten instanceof InterceptedQueryBuilderWrapper);
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof SemanticQueryBuilder);
    }

    public void testBoostsAndQueryNameAppliedCorrectly() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME_1, new InferenceFieldMetadata(index.getName(), "inferenceId1", new String[] { FIELD_NAME_1 }, null),
            FIELD_NAME_2, new InferenceFieldMetadata(index.getName(), "inferenceId2", new String[] { FIELD_NAME_2 }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        MultiMatchQueryBuilder original = createTestQueryBuilder()
            .fields(Map.of(FIELD_NAME_1, FIELD_BOOST_1, FIELD_NAME_2, FIELD_BOOST_2))
            .boost(BOOST)
            .queryName(QUERY_NAME);
        QueryBuilder rewritten = original.rewrite(context);

        assertTrue(rewritten instanceof InterceptedQueryBuilderWrapper);
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof DisMaxQueryBuilder);
        DisMaxQueryBuilder disMaxQuery = (DisMaxQueryBuilder) intercepted.queryBuilder;

        assertEquals(BOOST, disMaxQuery.boost(), 0.0f);
        assertEquals(QUERY_NAME, disMaxQuery.queryName());

        for (QueryBuilder innerQuery : disMaxQuery.innerQueries()) {
            assertTrue(innerQuery instanceof SemanticQueryBuilder);
            SemanticQueryBuilder semanticQuery = (SemanticQueryBuilder) innerQuery;
            String fieldName = semanticQuery.getFieldName();

            if (FIELD_NAME_1.equals(fieldName)) {
                assertEquals(FIELD_BOOST_1, semanticQuery.boost(), 0.0f);
            } else if (FIELD_NAME_2.equals(fieldName)) {
                assertEquals(FIELD_BOOST_2, semanticQuery.boost(), 0.0f);
            } else {
                fail("Unexpected field name: " + fieldName);
            }
        }
    }

    private MultiMatchQueryBuilder createTestQueryBuilder() {
        return new MultiMatchQueryBuilder(VALUE);
    }

    private QueryRewriteContext createQueryRewriteContext(Map<String, InferenceFieldMetadata> inferenceFields) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putInferenceFields(inferenceFields)
            .build();

        ResolvedIndices resolvedIndices = new MockResolvedIndices(
            Map.of(),
            new OriginalIndices(new String[] { index.getName() }, IndicesOptions.DEFAULT),
            Map.of(index, indexMetadata)
        );

        return new QueryRewriteContext(null, client, null, resolvedIndices, null, createRewriteInterceptor());
    }

    private QueryRewriteInterceptor createRewriteInterceptor() {
        return new SemanticMultiMatchQueryRewriteInterceptor();
    }
}
