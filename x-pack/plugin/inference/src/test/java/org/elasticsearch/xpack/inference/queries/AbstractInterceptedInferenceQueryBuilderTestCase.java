/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractInterceptedInferenceQueryBuilderTestCase<T extends InterceptedInferenceQueryBuilder<?>> extends ESTestCase {
    private TestThreadPool threadPool;

    @Before
    public void setUp() {
        threadPool = createThreadPool();
    }

    @After
    @Override
    public void tearDown() {
        threadPool.close();
    }

    public void testInterceptAndRewrite() {
        // TODO: Implement
    }

    public void testSerialization() {
        // TODO: Implement
    }

    public void testBwCSerialization() {
        // TODO: Implement
    }

    protected QueryRewriteContext createQueryRewriteContext(
        Map<String, Map<String, String>> localIndexInferenceFields,
        Map<String, String> remoteIndexNames,
        Map<String, MinimalServiceSettings> inferenceEndpoints,
        TransportVersion minTransportVersion
    ) {
        Map<Index, IndexMetadata> indexMetadata = new HashMap<>();
        for (var indexEntry : localIndexInferenceFields.entrySet()) {
            String indexName = indexEntry.getKey();
            Map<String, String> inferenceFields = localIndexInferenceFields.get(indexName);

            Index index = new Index(indexName, randomAlphaOfLength(10));
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                )
                .numberOfShards(1)
                .numberOfReplicas(0);

            for (var inferenceFieldEntry : inferenceFields.entrySet()) {
                String inferenceFieldName = inferenceFieldEntry.getKey();
                String inferenceId = inferenceFieldEntry.getValue();
                indexMetadataBuilder.putInferenceField(
                    new InferenceFieldMetadata(inferenceFieldName, inferenceId, new String[] { inferenceFieldName }, null)
                );
            }

            indexMetadata.put(index, indexMetadataBuilder.build());
        }

        Map<String, OriginalIndices> remoteIndices = new HashMap<>();
        if (remoteIndexNames != null) {
            for (var entry : remoteIndexNames.entrySet()) {
                remoteIndices.put(entry.getKey(), new OriginalIndices(new String[] { entry.getValue() }, IndicesOptions.DEFAULT));
            }
        }

        ResolvedIndices resolvedIndices = new MockResolvedIndices(
            remoteIndices,
            new OriginalIndices(localIndexInferenceFields.keySet().toArray(new String[0]), IndicesOptions.DEFAULT),
            indexMetadata
        );

        Client client = new MockInferenceClient(threadPool, inferenceEndpoints);

        return new QueryRewriteContext(
            null,
            client,
            null,
            minTransportVersion,
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            null,
            createQueryRewriteInterceptor(),
            null
        );
    }

    protected abstract QueryRewriteInterceptor createQueryRewriteInterceptor();
}
