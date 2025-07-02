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
import org.elasticsearch.xpack.inference.queries.SemanticMatchQueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

public class SemanticMatchQueryRewriteInterceptorTests extends ESTestCase {

    private TestThreadPool threadPool;
    private NoOpClient client;
    private Index index;

    private static final String FIELD_NAME = "fieldName";
    private static final String VALUE = "value";
    private static final String QUERY_NAME = "match_query";
    private static final float BOOST = 5.0f;

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

    public void testMatchQueryOnInferenceFieldIsInterceptedAndRewrittenToSemanticQuery() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME,
            new InferenceFieldMetadata(index.getName(), "inferenceId", new String[] { FIELD_NAME }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        QueryBuilder original = createTestQueryBuilder();
        QueryBuilder rewritten = original.rewrite(context);
        assertTrue(
            "Expected query to be intercepted, but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof InterceptedQueryBuilderWrapper
        );
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof SemanticQueryBuilder);
        SemanticQueryBuilder semanticQueryBuilder = (SemanticQueryBuilder) intercepted.queryBuilder;
        assertEquals(FIELD_NAME, semanticQueryBuilder.getFieldName());
        assertEquals(VALUE, semanticQueryBuilder.getQuery());
    }

    public void testMatchQueryOnNonInferenceFieldRemainsMatchQuery() throws IOException {
        QueryRewriteContext context = createQueryRewriteContext(Map.of()); // No inference fields
        QueryBuilder original = createTestQueryBuilder();
        QueryBuilder rewritten = original.rewrite(context);
        assertTrue(
            "Expected query to remain match but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof MatchQueryBuilder
        );
        assertEquals(original, rewritten);
    }

    public void testBoostInMatchQueryRewrite() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME,
            new InferenceFieldMetadata(index.getName(), "inferenceId", new String[] { FIELD_NAME }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        QueryBuilder original = createTestQueryBuilderWithBoost();
        QueryBuilder rewritten = original.rewrite(context);
        assertTrue(
            "Expected query to be intercepted, but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof InterceptedQueryBuilderWrapper
        );
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof SemanticQueryBuilder);
        SemanticQueryBuilder semanticQueryBuilder = (SemanticQueryBuilder) intercepted.queryBuilder;
        assertEquals(FIELD_NAME, semanticQueryBuilder.getFieldName());
        assertEquals(VALUE, semanticQueryBuilder.getQuery());
        assertEquals(BOOST, semanticQueryBuilder.boost(), 0.0f);
        assertEquals(QUERY_NAME, semanticQueryBuilder.queryName());
    }

    private MatchQueryBuilder createTestQueryBuilder() {
        return new MatchQueryBuilder(FIELD_NAME, VALUE);
    }

    private MatchQueryBuilder createTestQueryBuilderWithBoost() {
        MatchQueryBuilder queryBuilder = createTestQueryBuilder();
        queryBuilder.boost(BOOST);
        queryBuilder.queryName(QUERY_NAME);
        return queryBuilder;
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
        return new SemanticMatchQueryRewriteInterceptor();
    }
}
