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

public class PhaseTests extends AbstractSerializingTestCase<Phase> {
    
    private NamedXContentRegistry registry;
    private String phaseName;

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
                .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        registry = new NamedXContentRegistry(entries);
        phaseName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the phase name rather 
                                             // than use the same name for all instances
    }

    @Override
    protected Phase createTestInstance() {
        TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        List<LifecycleAction> actions = new ArrayList<>();
        if (randomBoolean()) {
            actions.add(new DeleteAction());
        }
        return new Phase(phaseName, after, actions);
    }

    @Override
    protected Phase doParseInstance(XContentParser parser) throws IOException {
        
        return Phase.parse(parser, new Tuple<>(phaseName, registry));
    }

    @Override
    protected Reader<Phase> instanceReader() {
        return Phase::new;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays
                .asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new)));
    }

    @Override
    protected Phase mutateInstance(Phase instance) throws IOException {
        String name = instance.getName();
        TimeValue after = instance.getAfter();
        List<LifecycleAction> actions = instance.getActions();
        switch (between(0, 2)) {
        case 0:
            name = name + randomAlphaOfLengthBetween(1, 5);
            break;
        case 1:
            after = TimeValue.timeValueSeconds(after.getSeconds() + randomIntBetween(1, 1000));
            break;
        case 2:
            actions = new ArrayList<>(actions);
            actions.add(new DeleteAction());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new Phase(name, after, actions);
    }

    public void testCanExecuteBeforeTrigger() throws Exception {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = random().longs(creationDate, creationDate + after.millis()).iterator().nextLong();

        IndexMetaData idxMeta = IndexMetaData.builder("test")
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Phase phase = new Phase("test_phase", after, Collections.emptyList());

        assertFalse(phase.canExecute(idxMeta, () -> now));
    }

    public void testCanExecuteOnTrigger() throws Exception {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = creationDate + after.millis();

        IndexMetaData idxMeta = IndexMetaData.builder("test")
                .settings(Settings.builder().put("index.version.created", 7000001L).put("index.creation_date", creationDate).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Phase phase = new Phase("test_phase", after, Collections.emptyList());

        assertTrue(phase.canExecute(idxMeta, () -> now));
    }

    public void testCanExecuteAfterTrigger() throws Exception {
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(0, 100000));
        long creationDate = randomNonNegativeLong();
        long now = random().longs(creationDate + after.millis(), Long.MAX_VALUE).iterator().nextLong();

        IndexMetaData idxMeta = IndexMetaData.builder("test").settings(Settings.builder()
                .put("index.version.created", 7000001L)
                .put("index.creation_date", creationDate)
                .build())
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5)).build();
        
        Phase phase = new Phase("test_phase", after, Collections.emptyList());

        assertTrue(phase.canExecute(idxMeta, () -> now));
    }

    public void testExecuteNewIndex() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

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
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "first_action").build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        phase.execute(idxMeta, client);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecuteNewIndexNoActions() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Phase phase = new Phase(phaseName, after, Collections.emptyList());

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
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), Phase.PHASE_COMPLETED).build();
                UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexName);
                listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        phase.execute(idxMeta, client);

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testExecutePhaseAlreadyComplete() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        Phase phase = new Phase(phaseName, after, Collections.emptyList());

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), Phase.PHASE_COMPLETED).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        phase.execute(idxMeta, client);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

    public void testExecuteFirstAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), firstAction.getWriteableName()).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        phase.execute(idxMeta, client);

        assertTrue(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

    public void testExecuteSecondAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), secondAction.getWriteableName()).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        phase.execute(idxMeta, client);

        assertFalse(firstAction.wasExecuted());
        assertTrue(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

    public void testExecuteThirdAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), thirdAction.getWriteableName()).build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        phase.execute(idxMeta, client);

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertTrue(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

    public void testExecuteMissingAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        IndexMetaData idxMeta = IndexMetaData.builder(indexName)
                .settings(Settings.builder().put("index.version.created", 7000001L)
                        .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), "does_not_exist").build())
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> phase.execute(idxMeta, client));
        assertEquals("Current action [" + "does_not_exist" + "] not found in phase [" + phaseName + "] for index [" + indexName + "]",
                exception.getMessage());

        assertFalse(firstAction.wasExecuted());
        assertFalse(secondAction.wasExecuted());
        assertFalse(thirdAction.wasExecuted());

        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
    }

}
