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
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.queries.SemanticSparseVectorQueryRewriteInterceptor;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

public class SemanticSparseVectorQueryRewriteInterceptorTests extends ESTestCase {

    private TestThreadPool threadPool;
    private NoOpClient client;
    private Index index;

    private static final String FIELD_NAME = "fieldName";
    private static final String INFERENCE_ID = "inferenceId";
    private static final String QUERY = "query";

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

    public void testSparseVectorQueryOnInferenceFieldIsInterceptedAndRewritten() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME,
            new InferenceFieldMetadata(index.getName(), "inferenceId", new String[] { FIELD_NAME }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        QueryBuilder original = new SparseVectorQueryBuilder(FIELD_NAME, INFERENCE_ID, QUERY);
        QueryBuilder rewritten = original.rewrite(context);
        assertTrue(
            "Expected query to be intercepted, but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof InterceptedQueryBuilderWrapper
        );
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof NestedQueryBuilder);
        NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) intercepted.queryBuilder;
        assertEquals(SemanticTextField.getChunksFieldName(FIELD_NAME), nestedQueryBuilder.path());
        QueryBuilder innerQuery = nestedQueryBuilder.query();
        assertTrue(innerQuery instanceof SparseVectorQueryBuilder);
        SparseVectorQueryBuilder sparseVectorQueryBuilder = (SparseVectorQueryBuilder) innerQuery;
        assertEquals(SemanticTextField.getEmbeddingsFieldName(FIELD_NAME), sparseVectorQueryBuilder.getFieldName());
        assertEquals(INFERENCE_ID, sparseVectorQueryBuilder.getInferenceId());
        assertEquals(QUERY, sparseVectorQueryBuilder.getQuery());
    }

    public void testSparseVectorQueryOnInferenceFieldWithoutInferenceIdIsInterceptedAndRewritten() throws IOException {
        Map<String, InferenceFieldMetadata> inferenceFields = Map.of(
            FIELD_NAME,
            new InferenceFieldMetadata(index.getName(), "inferenceId", new String[] { FIELD_NAME }, null)
        );
        QueryRewriteContext context = createQueryRewriteContext(inferenceFields);
        QueryBuilder original = new SparseVectorQueryBuilder(FIELD_NAME, null, QUERY);
        QueryBuilder rewritten = original.rewrite(context);
        assertTrue(
            "Expected query to be intercepted, but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof InterceptedQueryBuilderWrapper
        );
        InterceptedQueryBuilderWrapper intercepted = (InterceptedQueryBuilderWrapper) rewritten;
        assertTrue(intercepted.queryBuilder instanceof NestedQueryBuilder);
        NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) intercepted.queryBuilder;
        assertEquals(SemanticTextField.getChunksFieldName(FIELD_NAME), nestedQueryBuilder.path());
        QueryBuilder innerQuery = nestedQueryBuilder.query();
        assertTrue(innerQuery instanceof SparseVectorQueryBuilder);
        SparseVectorQueryBuilder sparseVectorQueryBuilder = (SparseVectorQueryBuilder) innerQuery;
        assertEquals(SemanticTextField.getEmbeddingsFieldName(FIELD_NAME), sparseVectorQueryBuilder.getFieldName());
        assertEquals(INFERENCE_ID, sparseVectorQueryBuilder.getInferenceId());
        assertEquals(QUERY, sparseVectorQueryBuilder.getQuery());
    }

    public void testSparseVectorQueryOnNonInferenceFieldRemainsUnchanged() throws IOException {
        QueryRewriteContext context = createQueryRewriteContext(Map.of()); // No inference fields
        QueryBuilder original = new SparseVectorQueryBuilder(FIELD_NAME, INFERENCE_ID, QUERY);
        QueryBuilder rewritten = original.rewrite(context);
        assertTrue(
            "Expected query to remain sparse_vector but was [" + rewritten.getClass().getName() + "]",
            rewritten instanceof SparseVectorQueryBuilder
        );
        assertEquals(original, rewritten);
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
        return new SemanticSparseVectorQueryRewriteInterceptor();
    }
}
