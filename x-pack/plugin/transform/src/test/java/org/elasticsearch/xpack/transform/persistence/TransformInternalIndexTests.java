/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransformInternalIndexTests extends ESTestCase {

    private ClusterState stateWithLatestVersionedIndex;

    public static ClusterState randomTransformClusterState() {
        return randomTransformClusterState(true);
    }

    public static ClusterState randomTransformClusterState(boolean shardsReady) {
        ImmutableOpenMap.Builder<String, IndexMetadata> indexMapBuilder = ImmutableOpenMap.builder();
        try {
            IndexMetadata.Builder builder = new IndexMetadata.Builder(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME).settings(
                Settings.builder()
                    .put(TransformInternalIndex.settings())
                    .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                    .build()
            ).numberOfReplicas(0).numberOfShards(1).putMapping(Strings.toString(TransformInternalIndex.mappings()));
            indexMapBuilder.put(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME, builder.build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.indices(indexMapBuilder.build());
        ClusterState.Builder csBuilder = ClusterState.builder(ClusterName.DEFAULT);
        csBuilder.metadata(metaBuilder.build());

        final var index = new Index(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME, UUIDs.randomBase64UUID());
        csBuilder.routingTable(
            RoutingTable.builder()
                .add(
                    IndexRoutingTable.builder(index)
                        .addShard(
                            TestShardRouting.newShardRouting(
                                new ShardId(index, 0),
                                "node_a",
                                null,
                                true,
                                shardsReady ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
                            )
                        )
                        .build()
                )
                .build()
        );

        return csBuilder.build();
    }

    @Before
    public void setupClusterStates() {
        stateWithLatestVersionedIndex = randomTransformClusterState();
    }

    public void testHaveLatestVersionedIndexTemplate() {
        assertTrue(TransformInternalIndex.hasLatestVersionedIndex(stateWithLatestVersionedIndex));
        assertTrue(TransformInternalIndex.allPrimaryShardsActiveForLatestVersionedIndex(stateWithLatestVersionedIndex));
        assertFalse(TransformInternalIndex.hasLatestVersionedIndex(ClusterState.EMPTY_STATE));
        assertFalse(TransformInternalIndex.allPrimaryShardsActiveForLatestVersionedIndex(ClusterState.EMPTY_STATE));
        assertFalse(TransformInternalIndex.allPrimaryShardsActiveForLatestVersionedIndex(randomTransformClusterState(false)));
    }

    public void testCreateLatestVersionedIndexIfRequired_GivenNotRequired() {

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(stateWithLatestVersionedIndex);

        Client client = mock(Client.class);

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        ActionListener<Void> testListener = ActionListener.wrap(aVoid -> gotResponse.set(true), e -> fail(e.getMessage()));

        TransformInternalIndex.createLatestVersionedIndexIfRequired(clusterService, client, testListener);

        assertTrue(gotResponse.get());
        verifyNoMoreInteractions(client);
    }

    public void testCreateLatestVersionedIndexIfRequired_GivenRequired() {

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new CreateIndexResponse(true, true, TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME));
            return null;
        }).when(indicesClient).create(any(), any());

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        ActionListener<Void> testListener = ActionListener.wrap(aVoid -> gotResponse.set(true), e -> fail(e.getMessage()));

        TransformInternalIndex.createLatestVersionedIndexIfRequired(clusterService, client, testListener);

        assertTrue(gotResponse.get());
        verify(client, times(1)).threadPool();
        verify(client, times(1)).admin();
        verifyNoMoreInteractions(client);
        verify(adminClient, times(1)).indices();
        verifyNoMoreInteractions(adminClient);
        verify(indicesClient, times(1)).create(any(), any());
        verifyNoMoreInteractions(indicesClient);
    }

    public void testCreateLatestVersionedIndexIfRequired_GivenShardInitializationPending() {

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(randomTransformClusterState(false));

        ClusterAdminClient clusterClient = mock(ClusterAdminClient.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ClusterHealthResponse> listener = (ActionListener<ClusterHealthResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new ClusterHealthResponse());
            return null;
        }).when(clusterClient).health(any(), any());

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        ActionListener<Void> testListener = ActionListener.wrap(aVoid -> gotResponse.set(true), e -> fail(e.getMessage()));

        TransformInternalIndex.createLatestVersionedIndexIfRequired(clusterService, client, testListener);

        assertTrue(gotResponse.get());
        verify(client, times(1)).threadPool();
        verify(client, times(1)).admin();
        verifyNoMoreInteractions(client);
        verify(adminClient, times(1)).cluster();
        verifyNoMoreInteractions(adminClient);
        verify(clusterClient, times(1)).health(any(), any());
        verifyNoMoreInteractions(clusterClient);
    }

    public void testCreateLatestVersionedIndexIfRequired_GivenConcurrentCreation() {

        // simulate the case that 1st the index does not exist, but got created and allocated meanwhile
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE).thenReturn(stateWithLatestVersionedIndex);

        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(new ResourceAlreadyExistsException(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME));
            return null;
        }).when(indicesClient).create(any(), any());

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        ActionListener<Void> testListener = ActionListener.wrap(aVoid -> gotResponse.set(true), e -> fail(e.getMessage()));

        TransformInternalIndex.createLatestVersionedIndexIfRequired(clusterService, client, testListener);

        assertTrue(gotResponse.get());
        verify(client, times(1)).threadPool();
        verify(client, times(1)).admin();
        verifyNoMoreInteractions(client);
        verify(adminClient, times(1)).indices();
        verifyNoMoreInteractions(adminClient);
        verify(indicesClient, times(1)).create(any(), any());
        verifyNoMoreInteractions(indicesClient);
    }

    public void testCreateLatestVersionedIndexIfRequired_GivenConcurrentCreationShardInitializationPending() {

        // simulate the case that 1st the index does not exist, but got created, however allocation is pending
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE).thenReturn(randomTransformClusterState(false));

        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(new ResourceAlreadyExistsException(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME));
            return null;
        }).when(indicesClient).create(any(), any());

        ClusterAdminClient clusterClient = mock(ClusterAdminClient.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ClusterHealthResponse> listener = (ActionListener<ClusterHealthResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new ClusterHealthResponse());
            return null;
        }).when(clusterClient).health(any(), any());

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesClient);
        when(adminClient.cluster()).thenReturn(clusterClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        ActionListener<Void> testListener = ActionListener.wrap(aVoid -> gotResponse.set(true), e -> fail(e.getMessage()));

        TransformInternalIndex.createLatestVersionedIndexIfRequired(clusterService, client, testListener);

        assertTrue(gotResponse.get());
        verify(client, times(2)).threadPool();
        verify(client, times(2)).admin();
        verifyNoMoreInteractions(client);
        verify(adminClient, times(1)).indices();
        verify(adminClient, times(1)).cluster();
        verifyNoMoreInteractions(adminClient);
        verify(indicesClient, times(1)).create(any(), any());
        verifyNoMoreInteractions(indicesClient);
        verify(clusterClient, times(1)).health(any(), any());
        verifyNoMoreInteractions(clusterClient);
    }

    public void testEnsureLatestIndexAndTemplateInstalled_GivenRequired() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new CreateIndexResponse(true, true, TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME));
            return null;
        }).when(indicesClient).create(any(), any());

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        ActionListener<Void> testListener = ActionListener.wrap(aVoid -> gotResponse.set(true), e -> fail(e.getMessage()));

        TransformInternalIndex.createLatestVersionedIndexIfRequired(clusterService, client, testListener);

        assertTrue(gotResponse.get());
        verify(client, times(1)).threadPool();
        verify(client, times(1)).admin();
        verifyNoMoreInteractions(client);
        verify(adminClient, times(1)).indices();
        verifyNoMoreInteractions(adminClient);
        verify(indicesClient, times(1)).create(any(), any());
        verifyNoMoreInteractions(indicesClient);
    }
}
