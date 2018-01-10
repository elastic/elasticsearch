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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleContext.Listener;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;

public class InternalIndexLifecycleContextTests extends ESTestCase {
    private static final Index TEST_INDEX = new Index("test", "test");

    private ClusterState getClusterState(IndexMetaData indexMetaData) {
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
            .fPut(indexMetaData.getIndex().getName(), indexMetaData);
        MetaData metaData = MetaData.builder().indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build()).build();
        return ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
//        ClusterService clusterService = Mockito.mock(ClusterService.class);
//        Mockito.when(clusterService.state()).thenReturn(clusterState);
    }

    public void testSetPhase() {
        long creationDate = randomNonNegativeLong();
        String oldPhase = randomAlphaOfLengthBetween(1, 5);
        String newPhase = randomAlphaOfLengthBetween(6, 10);
        Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_PHASE_SETTING.getKey(), newPhase)
            .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), "").build();
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_PHASE_SETTING.getKey(), oldPhase)
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterState updatedClusterState = getClusterState(IndexMetaData.builder(idxMeta)
            .settings(Settings.builder().put(idxMeta.getSettings()).put(expectedSettings)).build());
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState, updatedClusterState);

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);


        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, TEST_INDEX.getName());
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, client, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
        assertEquals(oldPhase, context.getPhase());
        context.setPhase(newPhase, new Listener() {

            @Override
            public void onSuccess() {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected Error", e);
            }
        });

        assertEquals(true, listenerCalled.get());
        assertEquals(newPhase, context.getPhase());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testSetPhaseNotAcknowledged() {
        long creationDate = randomNonNegativeLong();
        String newPhase = randomAlphaOfLengthBetween(1, 20);
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_PHASE_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20))
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_PHASE_SETTING.getKey(), newPhase)
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), "").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, TEST_INDEX.getName());
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(false));
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, client, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
        context.setPhase(newPhase, new Listener() {

            @Override
            public void onSuccess() {
                throw new AssertionError("Unexpected Error");
            }

            @Override
            public void onFailure(Exception e) {
                assertNull(e);
                listenerCalled.set(true);
            }
        });

        assertEquals(true, listenerCalled.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testSetPhaseFailure() {
        long creationDate = randomNonNegativeLong();
        String newPhase = randomAlphaOfLengthBetween(1, 20);
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_PHASE_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20))
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        Exception exception = new RuntimeException();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_PHASE_SETTING.getKey(), newPhase)
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), "").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, TEST_INDEX.getName());
                listener.onFailure(exception);
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, client, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
        context.setPhase(newPhase, new Listener() {

            @Override
            public void onSuccess() {
                throw new AssertionError("Unexpected Error");
            }

            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
                listenerCalled.set(true);
            }
        });

        assertEquals(true, listenerCalled.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testGetPhase() {
        long creationDate = randomNonNegativeLong();
        String phase = randomAlphaOfLengthBetween(1, 20);
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_PHASE_SETTING.getKey(), phase).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, null, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        assertEquals(phase, context.getPhase());
    }

    public void testGetReplicas() {
        int replicas = randomIntBetween(0, 5);
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
            .settings(Settings.builder().put("index.version.created", 7000001L).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(replicas).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, null, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        assertEquals(replicas, context.getNumberOfReplicas());
    }

    public void testSetAction() {
        long creationDate = randomNonNegativeLong();
        String oldAction = randomAlphaOfLengthBetween(1, 5);
        String newAction = randomAlphaOfLengthBetween(6, 10);
        Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), newAction).build();
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), oldAction).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterState updatedClusterState = getClusterState(IndexMetaData.builder(idxMeta)
            .settings(Settings.builder().put(idxMeta.getSettings()).put(expectedSettings)).build());
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState, updatedClusterState);

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, TEST_INDEX.getName());
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, client, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
        assertEquals(oldAction, context.getAction());
        context.setAction(newAction, new Listener() {

            @Override
            public void onSuccess() {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected Error", e);
            }
        });

        assertEquals(true, listenerCalled.get());
        assertEquals(newAction, context.getAction());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testSetActionNotAcknoledged() {
        long creationDate = randomNonNegativeLong();
        String newAction = randomAlphaOfLengthBetween(1, 20);
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), newAction)
                        .build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, TEST_INDEX.getName());
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(false));
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, client, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
        context.setAction(newAction, new Listener() {

            @Override
            public void onSuccess() {
                throw new AssertionError("Unexpected Error");
            }

            @Override
            public void onFailure(Exception e) {
                assertNull(e);
                listenerCalled.set(true);
            }
        });

        assertEquals(true, listenerCalled.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testSetActionFailure() {
        long creationDate = randomNonNegativeLong();
        String newAction = randomAlphaOfLengthBetween(1, 20);
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        Exception exception = new RuntimeException();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), newAction)
                        .build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, TEST_INDEX.getName());
                listener.onFailure(exception);
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, client, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
        context.setAction(newAction, new Listener() {

            @Override
            public void onSuccess() {
                throw new AssertionError("Unexpected Error");
            }

            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
                listenerCalled.set(true);
            }
        });

        assertEquals(true, listenerCalled.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testGetAction() {
        long creationDate = randomNonNegativeLong();
        String action = randomAlphaOfLengthBetween(1, 20);
        IndexMetaData idxMeta = IndexMetaData.builder("test")
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_ACTION_SETTING.getKey(), action).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, null, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        assertEquals(action, context.getAction());
    }

    public void testGetLifecycleTarget() {
        long creationDate = randomNonNegativeLong();
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        IndexMetaData idxMeta = IndexMetaData.builder(index.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(index, null, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        assertEquals(index.getName(), context.getLifecycleTarget());
    }

    public void testCanExecuteBeforeTrigger() {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = random().longs(creationDate, creationDate + after.millis()).iterator().nextLong();

        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, null, clusterService, () -> now);

        Phase phase = new Phase("test_phase", after, Collections.emptyMap());

        assertFalse(context.canExecute(phase));
    }

    public void testCanExecuteOnTrigger() {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = creationDate + after.millis();

        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, null, clusterService, () -> now);

        Phase phase = new Phase("test_phase", after, Collections.emptyMap());

        assertTrue(context.canExecute(phase));
    }

    public void testCanExecuteAfterTrigger() {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = random().longs(creationDate + after.millis(), Long.MAX_VALUE).iterator().nextLong();

        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, null, clusterService, () -> now);

        Phase phase = new Phase("test_phase", after, Collections.emptyMap());

        assertTrue(context.canExecute(phase));
    }

    public void testExecuteAction() {
        IndexMetaData idxMeta = IndexMetaData.builder(TEST_INDEX.getName())
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", 0L).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = getClusterState(idxMeta);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(TEST_INDEX, null, clusterService, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        MockAction action = new MockAction();
        action.setCompleteOnExecute(true);

        assertFalse(action.wasCompleted());
        assertEquals(0L, action.getExecutedCount());

        SetOnce<Boolean> listenerCalled = new SetOnce<>();

        context.executeAction(action, new LifecycleAction.Listener() {

            @Override
            public void onSuccess(boolean completed) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertTrue(action.wasCompleted());
        assertEquals(1L, action.getExecutedCount());
        assertEquals(true, listenerCalled.get());
    }
}
