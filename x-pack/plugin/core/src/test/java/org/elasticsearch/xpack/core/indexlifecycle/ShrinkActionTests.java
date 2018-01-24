/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsTestHelper;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;

public class ShrinkActionTests extends AbstractSerializingTestCase<ShrinkAction> {

    @Override
    protected ShrinkAction doParseInstance(XContentParser parser) throws IOException {
        return ShrinkAction.parse(parser);
    }

    @Override
    protected ShrinkAction createTestInstance() {
        return new ShrinkAction(randomIntBetween(1, 100));
    }

    @Override
    protected ShrinkAction mutateInstance(ShrinkAction action) {
        return new ShrinkAction(action.getNumberOfShards() + randomIntBetween(1, 2));
    }

    @Override
    protected Reader<ShrinkAction> instanceReader() {
        return ShrinkAction::new;
    }

    public void testNonPositiveShardNumber() {
        Exception e = expectThrows(Exception.class, () -> new ShrinkAction(randomIntBetween(-100, 0)));
        assertThat(e.getMessage(), equalTo("[number_of_shards] must be greater than 0"));
    }

    public void testExecuteSuccessfullyCompleted() {
        String originalIndexName = randomAlphaOfLengthBetween(1, 20);
        Index index = new Index("shrunk-" + originalIndexName, randomAlphaOfLengthBetween(1, 20));
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData originalIndexMetaData = IndexMetaData.builder(originalIndexName)
            .settings(settings(Version.CURRENT)).numberOfReplicas(0).numberOfShards(1).build();
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME_KEY, originalIndexName))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata).fPut(originalIndexName, originalIndexMetaData);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer(invocationOnMock -> {
            IndicesAliasesRequest request = (IndicesAliasesRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<IndicesAliasesResponse> listener = (ActionListener<IndicesAliasesResponse>) invocationOnMock.getArguments()[1];
            IndicesAliasesResponse response = IndicesAliasesAction.INSTANCE.newResponse();
            response.readFrom(StreamInput.wrap(new byte[] { 1 }));

            assertThat(request.getAliasActions().size(), equalTo(2));
            assertThat(request.getAliasActions().get(0).actionType(), equalTo(IndicesAliasesRequest.AliasActions.Type.REMOVE_INDEX));
            assertThat(request.getAliasActions().get(0).indices(), equalTo(new String[] { originalIndexName }));
            assertThat(request.getAliasActions().get(1).actionType(), equalTo(IndicesAliasesRequest.AliasActions.Type.ADD));
            assertThat(request.getAliasActions().get(1).indices(), equalTo(new String[] { index.getName() }));
            assertThat(request.getAliasActions().get(1).aliases(), equalTo(new String[] { originalIndexName }));

            listener.onResponse(response);
            return null;
        }).when(indicesClient).aliases(any(), any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(randomIntBetween(1, 10));

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertTrue(actionCompleted.get());
        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).aliases(any(), any());
    }

    public void testExecuteAlreadyCompletedAndRunAgain() {
        String originalIndexName = randomAlphaOfLengthBetween(1, 20);
        Index index = new Index("shrunk-" + originalIndexName, randomAlphaOfLengthBetween(1, 20));
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .putAlias(AliasMetaData.builder(originalIndexName).build())
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME_KEY, originalIndexName))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);
        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(randomIntBetween(1, 10));

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertTrue(actionCompleted.get());
        Mockito.verify(client, Mockito.never()).admin();
    }

    public void testExecuteOriginalIndexAliasFailure() {
        String originalIndexName = randomAlphaOfLengthBetween(1, 20);
        Index index = new Index("shrunk-" + originalIndexName, randomAlphaOfLengthBetween(1, 20));
        Index targetIndex = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME_KEY, originalIndexName))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(targetIndex).addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true,
                        ShardRoutingState.STARTED)))
                .build())
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesAliasesResponse> listener = (ActionListener<IndicesAliasesResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(new RuntimeException("failed"));
            return null;
        }).when(indicesClient).aliases(any(), any());

        SetOnce<Exception> onFailureException = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(randomIntBetween(1, 10));

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                onFailureException.set(e);
            }
        });

        assertThat(onFailureException.get().getMessage(), equalTo("failed"));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).aliases(any(), any());
    }

    public void testExecuteWithIssuedResizeRequest() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Index targetIndex = new Index("shrunk-" + index.getName(), randomAlphaOfLengthBetween(1, 20));
        int numberOfShards = randomIntBetween(1, 5);
        int numberOfReplicas = randomIntBetween(1, 5);
        long creationDate = randomNonNegativeLong();
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT))
            .putAlias(AliasMetaData.builder("my_alias"))
            .creationDate(creationDate)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(numberOfReplicas).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(targetIndex).addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true,
                        ShardRoutingState.STARTED)))
                .build())
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Settings expectedSettings = Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build();

        Mockito.doAnswer(invocationOnMock -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
            UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, index.getName());
            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
            return null;
        }).when(indicesClient).updateSettings(any(), any());

        Mockito.doAnswer(invocationOnMock -> {
            ResizeRequest request = (ResizeRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[1];
            assertThat(request.getSourceIndex(), equalTo(index.getName()));
            assertThat(request.getTargetIndexRequest().aliases(), equalTo(Collections.singleton(new Alias("my_alias"))));
            assertThat(request.getTargetIndexRequest().settings(), equalTo(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, creationDate).build()));
            assertThat(request.getTargetIndexRequest().index(), equalTo(targetIndex.getName()));
            ResizeResponse resizeResponse = ResizeAction.INSTANCE.newResponse();
            resizeResponse.readFrom(StreamInput.wrap(new byte[] { 1, 1, 1, 1, 1 }));
            listener.onResponse(resizeResponse);
            return null;
        }).when(indicesClient).resizeIndex(any(), any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(numberOfShards);

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertFalse(actionCompleted.get());

        Mockito.verify(client, Mockito.atLeast(1)).admin();
        Mockito.verify(adminClient, Mockito.atLeast(1)).indices();
        Mockito.verify(indicesClient, Mockito.atLeast(1)).updateSettings(any(), any());
        Mockito.verify(indicesClient, Mockito.atLeast(1)).resizeIndex(any(), any());
    }

    public void testExecuteWithIssuedResizeRequestFailure() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Index targetIndex = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(targetIndex).addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true,
                        ShardRoutingState.STARTED)))
                .build())
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
            return null;
        }).when(indicesClient).updateSettings(any(), any());

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[1];
            ResizeResponse resizeResponse = ResizeAction.INSTANCE.newResponse();
            resizeResponse.readFrom(StreamInput.wrap(new byte[] { 0, 1, 1, 1, 1 }));
            listener.onResponse(resizeResponse);
            return null;
        }).when(indicesClient).resizeIndex(any(), any());

        SetOnce<Exception> exceptionReturned = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(randomIntBetween(1, 10));

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                throw new AssertionError("Unexpected method call to onSuccess");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionReturned.set(e);
            }
        });

        assertThat(exceptionReturned.get().getMessage(), equalTo("Shrink request failed to be acknowledged"));

        Mockito.verify(client, Mockito.atLeast(1)).admin();
        Mockito.verify(adminClient, Mockito.atLeast(1)).indices();
        Mockito.verify(indicesClient, Mockito.atLeast(1)).updateSettings(any(), any());
        Mockito.verify(indicesClient, Mockito.atLeast(1)).resizeIndex(any(), any());
    }

    public void testExecuteWithAllShardsAllocatedAndShrunkenIndexSetting() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Index targetIndex = new Index("shrunk-" + index.getName(), randomAlphaOfLengthBetween(1, 20));
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_PHASE, "phase1")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action1").put(LifecycleSettings.LIFECYCLE_NAME, "test"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        IndexMetaData targetIndexMetaData = IndexMetaData.builder(targetIndex.getName())
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME_KEY, index.getName()))
            .numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(indexMetadata.getNumberOfReplicas()).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
            .fPut(index.getName(), indexMetadata).fPut(targetIndex.getName(), targetIndexMetaData);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(targetIndex).addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true,
                        ShardRoutingState.STARTED)))
                .build())
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Settings expectedSettings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "test")
            .put(LifecycleSettings.LIFECYCLE_PHASE, "phase1").put(LifecycleSettings.LIFECYCLE_ACTION, "action1").build();

        Mockito.doAnswer(invocationOnMock -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest)  invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
            UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, targetIndex.getName());
            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
            return null;
        }).when(indicesClient).updateSettings(any(), any());

        Mockito.doAnswer(invocationOnMock -> {
            DeleteIndexRequest request = (DeleteIndexRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<DeleteIndexResponse> listener = (ActionListener<DeleteIndexResponse>) invocationOnMock.getArguments()[1];
            assertNotNull(request);
            assertEquals(1, request.indices().length);
            assertEquals(index.getName(), request.indices()[0]);
            listener.onResponse(null);
            return null;
        }).when(indicesClient).delete(any(), any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(randomIntBetween(1, 10));

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertFalse(actionCompleted.get());
        Mockito.verify(client, Mockito.atLeast(1)).admin();
        Mockito.verify(adminClient, Mockito.atLeast(1)).indices();
        Mockito.verify(indicesClient, Mockito.atLeast(1)).updateSettings(any(), any());
    }

    public void testExecuteWithAllShardsAllocatedAndShrunkenIndexConfigured() {
        String lifecycleName = randomAlphaOfLengthBetween(5, 10);
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Index targetIndex = new Index("shrunk-" + index.getName(), randomAlphaOfLengthBetween(1, 20));
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_PHASE, "phase1")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action1").put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        IndexMetaData targetIndexMetaData = IndexMetaData.builder(targetIndex.getName())
            .settings(settings(Version.CURRENT)
                .put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME_KEY, index.getName())
                .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), lifecycleName))
            .numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(indexMetadata.getNumberOfReplicas()).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
            .fPut(index.getName(), indexMetadata).fPut(targetIndex.getName(), targetIndexMetaData);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(targetIndex).addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true,
                        ShardRoutingState.STARTED)))
                .build())
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(randomIntBetween(1, 10));

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertFalse(actionCompleted.get());
        Mockito.verifyZeroInteractions(client);
    }

    public void testExecuteWaitingOnAllShardsActive() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Index targetIndex = new Index("shrunk-" + index.getName(), randomAlphaOfLengthBetween(1, 20));
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        IndexMetaData targetIndexMetadata = IndexMetaData.builder(targetIndex.getName())
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME_KEY, index.getName()))
            .numberOfShards(1).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata).fPut(targetIndex.getName(), targetIndexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(targetIndex).addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true,
                        ShardRoutingState.INITIALIZING)))
                .build())
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);
        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(randomIntBetween(1, 10));

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertFalse(actionCompleted.get());
    }

    public void testExecuteIndexAlreadyExists() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Index targetIndex = new Index("shrunk-" + index.getName(), randomAlphaOfLengthBetween(1, 20));
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        IndexMetaData targetIndexMetadata = IndexMetaData.builder(targetIndex.getName())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata).fPut(targetIndex.getName(), targetIndexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metaData(MetaData.builder().indices(indices.build())).build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);

        SetOnce<Exception> actionFailed = new SetOnce<>();
        ShrinkAction action = new ShrinkAction(randomIntBetween(1, 10));

        action.execute(index, client, clusterService, new LifecycleAction.Listener() {
            @Override
            public void onSuccess(boolean completed) {
                throw new AssertionError("Unexpected method call to onSuccess");
            }

            @Override
            public void onFailure(Exception e) {
                actionFailed.set(e);
            }
        });

        assertThat(actionFailed.get().getMessage(), equalTo("Cannot shrink index [" + index.getName() + "]" +
            " because target index [" + targetIndex.getName() + "] already exists."));
    }
}
