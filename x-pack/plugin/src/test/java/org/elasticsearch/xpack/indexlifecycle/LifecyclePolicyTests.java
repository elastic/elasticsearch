/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsTestHelper;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LifecyclePolicyTests extends AbstractSerializingTestCase<LifecyclePolicy> {
    
    private NamedXContentRegistry registry;
    private String lifecycleName;

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
                .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        registry = new NamedXContentRegistry(entries);
        lifecycleName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the lifecycle name rather 
                                                 // than use the same name for all instances
    }

    @Override
    protected LifecyclePolicy createTestInstance() {
        int numberPhases = randomInt(5);
        List<Phase> phases = new ArrayList<>(numberPhases);
        for (int i = 0; i < numberPhases; i++) {
            TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
            List<LifecycleAction> actions = new ArrayList<>();
            if (randomBoolean()) {
                actions.add(new DeleteAction());
            }
            phases.add(new Phase(randomAlphaOfLength(10), after, actions));
        }
        return new LifecyclePolicy(lifecycleName, phases);
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { "phases" }; // NOCOMMIT this needs to be temporary since we should not rely on the order of the JSON map
    }

    @Override
    protected LifecyclePolicy doParseInstance(XContentParser parser) throws IOException {
        return LifecyclePolicy.parse(parser, new Tuple<>(lifecycleName, registry));
    }

    @Override
    protected Reader<LifecyclePolicy> instanceReader() {
        return LifecyclePolicy::new;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new)));
    }

    @Override
    protected LifecyclePolicy mutateInstance(LifecyclePolicy instance) throws IOException {
        String name = instance.getName();
        List<Phase> phases = instance.getPhases();
        switch (between(0, 1)) {
        case 0:
            name = name + randomAlphaOfLengthBetween(1, 5);
            break;
        case 1:
            phases = new ArrayList<>(phases);
            phases.add(new Phase(randomAlphaOfLengthBetween(1, 10), TimeValue.timeValueSeconds(randomIntBetween(1, 1000)),
                    Collections.emptyList()));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new LifecyclePolicy(name, phases);
    }

    public void testExecuteNewIndex() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName).settings(Settings.builder().put("index.version.created", 7000001L).build())
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
                Settings expectedSettings = Settings.builder()
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), "first_phase")
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        policy.execute(idxMeta, client, () -> 0L);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecuteFirstPhase() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), firstPhase.getName())
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build())
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
                Settings expectedSettings = Settings.builder()
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), MockAction.NAME).build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        policy.execute(idxMeta, client, () -> 0L);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecuteSecondPhase() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), secondPhase.getName())
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build())
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
                Settings expectedSettings = Settings.builder()
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), MockAction.NAME).build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        policy.execute(idxMeta, client, () -> 0L);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecuteThirdPhase() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), thirdPhase.getName())
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build())
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
                Settings expectedSettings = Settings.builder()
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), MockAction.NAME).build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        policy.execute(idxMeta, client, () -> 0L);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecuteMissingPhase() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), "does_not_exist")
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> policy.execute(idxMeta, client, () -> 0L));
        assertEquals(
                "Current phase [" + "does_not_exist" + "] not found in lifecycle [" + lifecycleName + "] for index [" + indexName + "]",
                exception.getMessage());

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

    public void testExecuteFirstPhaseCompletedBeforeTrigger() throws Exception {
        long creationDate = 0L;
        long now = randomIntBetween(0, 9999);
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), firstPhase.getName())
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), Phase.PHASE_COMPLETED).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        policy.execute(idxMeta, client, () -> now);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

    public void testExecuteFirstPhaseCompletedAfterTrigger() throws Exception {
        long creationDate = 0L;
        long now = randomIntBetween(10000, 1000000);
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), firstPhase.getName())
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), Phase.PHASE_COMPLETED).build())
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
                Settings expectedSettings = Settings.builder()
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), "second_phase")
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        policy.execute(idxMeta, client, () -> now);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecuteSecondPhaseCompletedBeforeTrigger() throws Exception {
        long creationDate = 0L;
        long now = randomIntBetween(0, 19999);
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), secondPhase.getName())
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), Phase.PHASE_COMPLETED).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        policy.execute(idxMeta, client, () -> now);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

    public void testExecuteSecondPhaseCompletedAfterTrigger() throws Exception {
        long creationDate = 0L;
        long now = randomIntBetween(20000, 1000000);
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), secondPhase.getName())
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), Phase.PHASE_COMPLETED).build())
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
                Settings expectedSettings = Settings.builder()
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), "third_phase")
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        policy.execute(idxMeta, client, () -> now);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecuteThirdPhaseCompleted() throws Exception {
        long creationDate = 0L;
        long now = randomIntBetween(20000, 1000000);
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, phases);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), thirdPhase.getName())
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), Phase.PHASE_COMPLETED).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        policy.execute(idxMeta, client, () -> now);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

}
