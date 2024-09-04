/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportBulkActionIndicesThatCannotBeCreatedTests extends ESTestCase {
    private static final Consumer<String> noop = index -> {};

    public void testNonExceptional() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(randomAlphaOfLength(5)));
        bulkRequest.add(new IndexRequest(randomAlphaOfLength(5)));
        bulkRequest.add(new DeleteRequest(randomAlphaOfLength(5)));
        bulkRequest.add(new UpdateRequest(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        // Test emulating that index can be auto-created
        indicesThatCannotBeCreatedTestCase(emptySet(), bulkRequest, index -> true, noop);
        // Test emulating that index cannot be auto-created
        indicesThatCannotBeCreatedTestCase(emptySet(), bulkRequest, index -> false, noop);
        // Test emulating auto_create_index=true with some indices already created.
        indicesThatCannotBeCreatedTestCase(emptySet(), bulkRequest, index -> randomBoolean(), noop);
    }

    public void testAllFail() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("no"));
        bulkRequest.add(new IndexRequest("can't"));
        bulkRequest.add(new DeleteRequest("do").version(0).versionType(VersionType.EXTERNAL));
        bulkRequest.add(new UpdateRequest("nothin", randomAlphaOfLength(5)));
        indicesThatCannotBeCreatedTestCase(Set.of("no", "can't", "do", "nothin"), bulkRequest, index -> true, index -> {
            throw new IndexNotFoundException("Can't make it because I say so");
        });
    }

    public void testSomeFail() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("ok"));
        bulkRequest.add(new IndexRequest("bad"));
        // Emulate auto_create_index=-bad,+*
        indicesThatCannotBeCreatedTestCase(Set.of("bad"), bulkRequest, index -> true, index -> {
            if (index.equals("bad")) {
                throw new IndexNotFoundException("Can't make it because I say so");
            }
        });
    }

    private void indicesThatCannotBeCreatedTestCase(
        Set<String> expected,
        BulkRequest bulkRequest,
        Function<String, Boolean> shouldAutoCreate,
        Consumer<String> simulateAutoCreate
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(state.metadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(state.blocks()).thenReturn(mock(ClusterBlocks.class));
        when(clusterService.state()).thenReturn(state);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(clusterService.localNode()).thenReturn(localNode);
        when(localNode.isIngestNode()).thenReturn(randomBoolean());

        final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE
        ) {
            @Override
            public boolean hasIndexAbstraction(String indexAbstraction, ClusterState state) {
                return shouldAutoCreate.apply(indexAbstraction) == false;
            }
        };

        final ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        FeatureService mockFeatureService = mock(FeatureService.class);
        when(mockFeatureService.clusterHasFeature(any(), any())).thenReturn(true);
        TransportBulkAction action = new TransportBulkAction(
            threadPool,
            transportService,
            clusterService,
            null,
            mockFeatureService,
            new NodeClient(Settings.EMPTY, threadPool),
            mock(ActionFilters.class),
            indexNameExpressionResolver,
            new IndexingPressure(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            FailureStoreMetrics.NOOP
        ) {
            @Override
            void executeBulk(
                Task task,
                BulkRequest bulkRequest,
                long startTimeNanos,
                ActionListener<BulkResponse> listener,
                Executor executor,
                AtomicArray<BulkItemResponse> responses,
                Map<String, IndexNotFoundException> indicesThatCannotBeCreated
            ) {
                assertEquals(expected, indicesThatCannotBeCreated.keySet());
            }

            @Override
            void createIndex(CreateIndexRequest createIndexRequest, ActionListener<CreateIndexResponse> listener) {
                String index = createIndexRequest.index();
                try {
                    simulateAutoCreate.accept(index);
                    // If we try to create an index just immediately assume it worked
                    listener.onResponse(new CreateIndexResponse(true, true, index));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        };
        action.doExecute(null, bulkRequest, ActionTestUtils.assertNoFailureListener(bulkItemResponse -> {}));
    }
}
