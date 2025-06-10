/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MlIndexRolloverTests extends ESTestCase {

    private final IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();

    public void testIsAbleToRun_IndicesDoNotExist() {
        RoutingTable.Builder routingTable = RoutingTable.builder();
        var rollover = new MlIndexRollover(
            List.of(
                new MlIndexRollover.IndexPatternAndAlias("my-index1-*", "my-index1-alias"),
                new MlIndexRollover.IndexPatternAndAlias("my-index2-*", "my-index2-alias")
            ),
            indexNameExpressionResolver,
            mock(Client.class)
        );

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        assertTrue(rollover.isAbleToRun(csBuilder.build()));
    }

    public void testIsAbleToRun_IndicesHaveNoRouting() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("my-index-000001");
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(RoutingTable.builder().build()); // no routing to index
        csBuilder.metadata(metadata);

        var rollover = new MlIndexRollover(
            List.of(new MlIndexRollover.IndexPatternAndAlias("my-index-*", "my-index-alias")),
            indexNameExpressionResolver,
            mock(Client.class)
        );

        assertFalse(rollover.isAbleToRun(csBuilder.build()));
    }

    public void testIsAbleToRun_IndicesHaveNoActiveShards() {
        String indexName = "my-index-000001";
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );
        Index index = new Index(indexName, "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
            ShardRouting.Role.DEFAULT
        );
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        var routingTable = RoutingTable.builder()
            .add(IndexRoutingTable.builder(index).addIndexShard(IndexShardRoutingTable.builder(shardId).addShard(shardRouting)))
            .build();

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable);
        csBuilder.metadata(metadata);

        var rollover = new MlIndexRollover(
            List.of(new MlIndexRollover.IndexPatternAndAlias("my-index-*", "my-index-alias")),
            indexNameExpressionResolver,
            mock(Client.class)
        );

        assertFalse(rollover.isAbleToRun(csBuilder.build()));
    }

    public void testRunUpdate_NoMatchingIndices() {
        RoutingTable.Builder routingTable = RoutingTable.builder();

        var client = mock(Client.class);
        var rollover = new MlIndexRollover(
            List.of(
                new MlIndexRollover.IndexPatternAndAlias("my-index1-*", "my-index1-alias"),
                new MlIndexRollover.IndexPatternAndAlias("my-index2-*", "my-index2-alias")
            ),
            indexNameExpressionResolver,
            client
        );

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        rollover.runUpdate(csBuilder.build());
        verify(client).settings();
        verify(client).threadPool();
        verify(client).projectResolver();
        verifyNoMoreInteractions(client);
    }

    public void testRunUpdate_UpToDateIndicesWithAlias() {
        String indexName = "my-index-000001";
        String indexAlias = "my-index-write";
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );
        indexMetadata.putAlias(AliasMetadata.builder(indexAlias).build());

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        var client = mock(Client.class);
        var rollover = new MlIndexRollover(
            List.of(new MlIndexRollover.IndexPatternAndAlias("my-index-*", indexAlias)),
            indexNameExpressionResolver,
            client
        );

        rollover.runUpdate(csBuilder.build());
        // everything up to date so no action for the client
        verify(client).settings();
        verify(client).threadPool();
        verify(client).projectResolver();
        verifyNoMoreInteractions(client);
    }

    public void testRunUpdate_LegacyIndexWithAlias() {
        String indexName = "my-index-000001";
        String indexAlias = "my-index-write";
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersions.V_7_17_0) // cannot read and write to a 7.x index
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );
        indexMetadata.putAlias(AliasMetadata.builder(indexAlias).build());

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        var client = mockClientWithRolloverAndAlias();
        var rollover = new MlIndexRollover(
            List.of(new MlIndexRollover.IndexPatternAndAlias("my-index-*", indexAlias)),
            indexNameExpressionResolver,
            client
        );

        rollover.runUpdate(csBuilder.build());
        verify(client).settings();
        verify(client, times(3)).threadPool();
        verify(client).projectResolver();
        verify(client).execute(same(RolloverAction.INSTANCE), any(), any());  // index rolled over
        verifyNoMoreInteractions(client);
    }

    public void testRunUpdate_LegacyIndexWithoutAlias() {
        String indexName = "my-index-000001";
        String indexAlias = "my-index-write";
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersions.V_7_17_0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );
        // index is missing alias

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        var client = mockClientWithRolloverAndAlias();
        var rollover = new MlIndexRollover(
            List.of(new MlIndexRollover.IndexPatternAndAlias("my-index-*", indexAlias)),
            indexNameExpressionResolver,
            client
        );

        rollover.runUpdate(csBuilder.build());
        verify(client).settings();
        verify(client, times(5)).threadPool();
        verify(client).projectResolver();
        verify(client).execute(same(TransportIndicesAliasesAction.TYPE), any(), any());  // alias created
        verify(client).execute(same(RolloverAction.INSTANCE), any(), any());    // index rolled over
        verifyNoMoreInteractions(client);
    }

    public void testIsCompatibleIndexVersion() {
        assertTrue(MlIndexRollover.isCompatibleIndexVersion(IndexVersion.current()));
        assertTrue(MlIndexRollover.isCompatibleIndexVersion(IndexVersions.MINIMUM_COMPATIBLE));
        assertFalse(MlIndexRollover.isCompatibleIndexVersion(IndexVersions.MINIMUM_READONLY_COMPATIBLE));
    }

    @SuppressWarnings("unchecked")
    static Client mockClientWithRolloverAndAlias() {
        var client = mock(Client.class);
        when(client.projectClient(any())).thenReturn(client);

        doAnswer(invocationOnMock -> {
            ActionListener<RolloverResponse> actionListener = (ActionListener<RolloverResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(new RolloverResponse("old", "new", Map.of(), false, true, true, true, true));
            return null;
        }).when(client).execute(same(RolloverAction.INSTANCE), any(RolloverRequest.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            ActionListener<IndicesAliasesResponse> actionListener = (ActionListener<IndicesAliasesResponse>) invocationOnMock
                .getArguments()[2];
            actionListener.onResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);
            return null;
        }).when(client).execute(same(TransportIndicesAliasesAction.TYPE), any(IndicesAliasesRequest.class), any(ActionListener.class));

        var threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        return client;
    }
}
