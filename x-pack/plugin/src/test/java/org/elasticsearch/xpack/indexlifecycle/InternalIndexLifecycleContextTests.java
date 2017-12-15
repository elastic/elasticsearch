/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsTestHelper;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleContext.Listener;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;

public class InternalIndexLifecycleContextTests extends ESTestCase {

    public void testSetPhase() {
        long creationDate = randomNonNegativeLong();
        String newPhase = randomAlphaOfLengthBetween(1, 20);
        String indexName = "test";
        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20))
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

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
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), newPhase)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, client, null, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
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

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testSetPhaseNotAcknowledged() {
        long creationDate = randomNonNegativeLong();
        String newPhase = randomAlphaOfLengthBetween(1, 20);
        String indexName = "test";
        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20))
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

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
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), newPhase)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(false));
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, client, null, () -> {
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
        String indexName = "test";
        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20))
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

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
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), newPhase)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onFailure(exception);
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, client, null, () -> {
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
        IndexMetaData idxMeta = IndexMetaData.builder("test")
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), phase).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, null, null, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        assertEquals(phase, context.getPhase());
    }

    public void testSetAction() {
        long creationDate = randomNonNegativeLong();
        String newAction = randomAlphaOfLengthBetween(1, 20);
        String indexName = "test";
        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

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
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), newAction)
                        .build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, client, null, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
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

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testSetActionNotAcknoledged() {
        long creationDate = randomNonNegativeLong();
        String newAction = randomAlphaOfLengthBetween(1, 20);
        String indexName = "test";
        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

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
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), newAction)
                        .build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(false));
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, client, null, () -> {
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
        String indexName = "test";
        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), randomAlphaOfLengthBetween(1, 20)).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

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
                Settings expectedSettings = Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), newAction)
                        .build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onFailure(exception);
                return null;
            }
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, client, null, () -> {
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
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), action).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, null, null, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        assertEquals(action, context.getAction());
    }

    public void testGetLifecycleTarget() {
        long creationDate = randomNonNegativeLong();
        String index = randomAlphaOfLengthBetween(1, 20);
        IndexMetaData idxMeta = IndexMetaData.builder(index)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, null, null, () -> {
            throw new AssertionError("nowSupplier should not be called");
        });

        assertEquals(index, context.getLifecycleTarget());
    }

    public void testCanExecuteBeforeTrigger() throws Exception {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = random().longs(creationDate, creationDate + after.millis()).iterator().nextLong();

        IndexMetaData idxMeta = IndexMetaData.builder("test")
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, null, null, () -> now);

        Phase phase = new Phase("test_phase", after, Collections.emptyMap());

        assertFalse(context.canExecute(phase));
    }

    public void testCanExecuteOnTrigger() throws Exception {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = creationDate + after.millis();

        IndexMetaData idxMeta = IndexMetaData.builder("test")
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, null, null, () -> now);

        Phase phase = new Phase("test_phase", after, Collections.emptyMap());

        assertTrue(context.canExecute(phase));
    }

    public void testCanExecuteAfterTrigger() throws Exception {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = random().longs(creationDate + after.millis(), Long.MAX_VALUE).iterator().nextLong();

        IndexMetaData idxMeta = IndexMetaData.builder("test")
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, null, null, () -> now);

        Phase phase = new Phase("test_phase", after, Collections.emptyMap());

        assertTrue(context.canExecute(phase));
    }

    public void testExecuteAction() {
        IndexMetaData idxMeta = IndexMetaData.builder("test")
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", 0L).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        InternalIndexLifecycleContext context = new InternalIndexLifecycleContext(idxMeta, null, null, () -> {
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
