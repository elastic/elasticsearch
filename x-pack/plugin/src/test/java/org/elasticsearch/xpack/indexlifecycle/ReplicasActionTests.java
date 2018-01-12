/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsTestHelper;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.indexlifecycle.LifecycleAction.Listener;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

public class ReplicasActionTests extends AbstractSerializingTestCase<ReplicasAction> {

    @Override
    protected ReplicasAction doParseInstance(XContentParser parser) throws IOException {
        return ReplicasAction.parse(parser);
    }

    @Override
    protected ReplicasAction createTestInstance() {
        return new ReplicasAction(randomIntBetween(0, 10));
    }

    @Override
    protected Reader<ReplicasAction> instanceReader() {
        return ReplicasAction::new;
    }

    @Override
    protected ReplicasAction mutateInstance(ReplicasAction instance) throws IOException {
        return new ReplicasAction(instance.getNumberOfReplicas() + randomIntBetween(1, 5));
    }

    public void testInvalidNumReplicas() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new ReplicasAction(randomIntBetween(-1000, -1)));
        assertEquals("[" + ReplicasAction.NUMBER_OF_REPLICAS_FIELD.getPreferredName() + "] must be >= 0", exception.getMessage());
    }

    public void testExecuteDifferentReplicaCount() {
        int existingNumReplicas = randomIntBetween(0, 10);
        int newNumReplicas = randomValueOtherThan(existingNumReplicas, () -> randomIntBetween(0, 10));

        ReplicasAction action = new ReplicasAction(newNumReplicas);

        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLengthBetween(1, 20))
                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id)).numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(existingNumReplicas).build();
        Index index = indexMetadata.getIndex();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);
        ClusterState clusterstate = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
                Settings expectedSettings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, newNumReplicas).build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, index.getName());
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());
        Mockito.when(clusterService.state()).thenReturn(clusterstate);

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        action.execute(index, client, clusterService, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(false, actionCompleted.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
        Mockito.verify(clusterService, Mockito.only()).state();
    }

    public void testExecuteUpdateReplicaCountFailure() {
        int existingNumReplicas = randomIntBetween(0, 10);
        int newNumReplicas = randomValueOtherThan(existingNumReplicas, () -> randomIntBetween(0, 10));

        ReplicasAction action = new ReplicasAction(newNumReplicas);
        Exception exception = new RuntimeException();

        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLengthBetween(1, 20))
                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id)).numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(existingNumReplicas).build();
        Index index = indexMetadata.getIndex();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);
        ClusterState clusterstate = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
                Settings expectedSettings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, newNumReplicas).build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, index.getName());
                listener.onFailure(exception);
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());
        Mockito.when(clusterService.state()).thenReturn(clusterstate);

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        action.execute(index, client, clusterService, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionThrown.set(true);
            }
        });

        assertEquals(true, exceptionThrown.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
        Mockito.verify(clusterService, Mockito.only()).state();
    }

    public void testExecuteAllocationNotComplete() {

        ReplicasAction action = createTestInstance();

        int numberOfShards = randomIntBetween(1, 5);
        int numberOfReplicas = action.getNumberOfReplicas();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLengthBetween(1, 20))
                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id)).numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas).build();
        Index index = indexMetadata.getIndex();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < numberOfShards; shard++) {
            for (int replica = 0; replica < numberOfReplicas + 1; replica++) {
                ShardRoutingState state;
                if (replica == 0) {
                    state = ShardRoutingState.STARTED;
                } else if ((replica == numberOfReplicas) || randomBoolean()) {
                    state = randomFrom(ShardRoutingState.UNASSIGNED, ShardRoutingState.INITIALIZING);
                } else {
                    state = ShardRoutingState.STARTED;
                }
                String nodeId = "node" + replica;
                if (ShardRoutingState.UNASSIGNED.equals(state)) {
                    nodeId = null;
                }
                indexRoutingTable.addShard(TestShardRouting.newShardRouting(new ShardId(index, shard), nodeId, replica == 0, state));
            }
        }
        ClusterState clusterstate = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
                .build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.when(clusterService.state()).thenReturn(clusterstate);

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        action.execute(index, client, clusterService, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(false, actionCompleted.get());

        Mockito.verify(clusterService, Mockito.times(2)).state();
        Mockito.verify(client, Mockito.never()).admin();
        Mockito.verify(adminClient, Mockito.never()).indices();
        Mockito.verify(indicesClient, Mockito.never()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecuteAllocationComplete() {

        ReplicasAction action = createTestInstance();

        int numberOfShards = randomIntBetween(1, 5);
        int numberOfReplicas = action.getNumberOfReplicas();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLengthBetween(1, 20))
                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id)).numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas).build();
        Index index = indexMetadata.getIndex();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < numberOfShards; shard++) {
            for (int replica = 0; replica < numberOfReplicas + 1; replica++) {
                ShardRoutingState state = ShardRoutingState.STARTED;
                String nodeId = "node" + replica;
                indexRoutingTable.addShard(TestShardRouting.newShardRouting(new ShardId(index, shard), nodeId, replica == 0, state));
            }
        }
        ClusterState clusterstate = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.when(clusterService.state()).thenReturn(clusterstate);

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        action.execute(index, client, clusterService, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(true, actionCompleted.get());

        Mockito.verify(clusterService, Mockito.times(2)).state();
        Mockito.verify(client, Mockito.never()).admin();
        Mockito.verify(adminClient, Mockito.never()).indices();
        Mockito.verify(indicesClient, Mockito.never()).updateSettings(Mockito.any(), Mockito.any());
    }
}
