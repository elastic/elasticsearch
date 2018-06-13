/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.node.Node.NODE_MASTER_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexLifecycleServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private IndexLifecycleService indexLifecycleService;
    private String nodeId;
    private DiscoveryNode masterNode;
    private IndicesAdminClient indicesClient;
    private long now;

    @Before
    public void prepareServices() {
        nodeId = randomAlphaOfLength(10);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        clusterService = mock(ClusterService.class);
        masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        now = randomNonNegativeLong();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds())));

        doAnswer(invocationOnMock -> null).when(clusterService).addListener(any());
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
            runnable.run();
            return null;
        }).when(executorService).execute(any());

        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        indexLifecycleService = new IndexLifecycleService(Settings.EMPTY, client, clusterService, clock,
            threadPool, () -> now);
        Mockito.verify(clusterService).addListener(indexLifecycleService);
        Mockito.verify(clusterService).addStateApplier(indexLifecycleService);
    }

    public void testOnlyChangesStateOnMaster() throws Exception {
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(3)).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId + "not").masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", state, state);

        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        verify(clusterService, times(1)).addListener(any());
        verify(clusterService, times(1)).addStateApplier(any());
        Mockito.verifyNoMoreInteractions(clusterService);
        assertNull(indexLifecycleService.getScheduler());
    }

    public void testElectUnElectMaster() throws Exception {
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(3)).build())
                .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(Collections.emptySortedMap()))
            .build();

        // First check that when the node has never been master the scheduler
        // and job are not set up
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId + "not").masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", state, state);

        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        verify(clusterService, times(1)).addListener(any());
        verify(clusterService, times(1)).addStateApplier(any());
        Mockito.verifyNoMoreInteractions(clusterService);
        assertNull(indexLifecycleService.getScheduler());
        assertNull(indexLifecycleService.getScheduledJob());
        
        Mockito.reset(clusterService);
        state = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
                .build();
        event = new ClusterChangedEvent("_source", state, state);

        // Check that when the node is first elected as master it sets up
        // the scheduler and job
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        Mockito.verifyZeroInteractions(clusterService);
        assertNotNull(indexLifecycleService.getScheduler());
        assertEquals(1, indexLifecycleService.getScheduler().jobCount());
        assertNotNull(indexLifecycleService.getScheduledJob());
        
        Mockito.reset(clusterService);
        state = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId + "not").masterNodeId(nodeId).add(masterNode).build())
                .build();
        event = new ClusterChangedEvent("_source", state, state);

        // Check that when the node is un-elected as master it cancels the job
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        Mockito.verifyZeroInteractions(clusterService);
        assertNotNull(indexLifecycleService.getScheduler());
        assertEquals(0, indexLifecycleService.getScheduler().jobCount());
        assertNull(indexLifecycleService.getScheduledJob());
        
        Mockito.reset(clusterService);
        state = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
                .build();
        event = new ClusterChangedEvent("_source", state, state);

        // Check that when the node is re-elected as master it cancels the job
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        Mockito.verifyZeroInteractions(clusterService);
        assertNotNull(indexLifecycleService.getScheduler());
        assertEquals(1, indexLifecycleService.getScheduler().jobCount());
        assertNotNull(indexLifecycleService.getScheduledJob());
    }

    public void testServiceSetupOnFirstClusterChange() {
        TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(1, 59));
        MetaData metaData = MetaData.builder().persistentSettings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollInterval).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", state, state);
        final SetOnce<ClusterChangedEvent> installedEvent = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            ClusterStateUpdateTask updateTask = (ClusterStateUpdateTask) invocationOnMock.getArguments()[1];
            ClusterState newState = updateTask.execute(state);
            IndexLifecycleMetadata indexLifecycleMetadata = newState.metaData().custom(IndexLifecycleMetadata.TYPE);
            assertThat(indexLifecycleMetadata.getPolicyMetadatas(), equalTo(Collections.emptySortedMap()));
            installedEvent.set(new ClusterChangedEvent(event.source(), newState, state));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));

        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);

        verify(clusterService, times(1)).addListener(any());
        verify(clusterService, times(1)).addStateApplier(any());
        verify(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        Mockito.verifyNoMoreInteractions(clusterService);
        assertNull(indexLifecycleService.getScheduler());
    }

    @After
    public void cleanup() throws IOException {
        indexLifecycleService.close();
    }

    public void testSchedulerInitializationAndUpdate() {
        TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(1, 59));
        MetaData metaData = MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(Collections.emptySortedMap()))
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        MetaData updatedPollMetaData = MetaData.builder(metaData).persistentSettings(settings(Version.CURRENT)
            .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollInterval).build())
            .build();
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterState currentState = ClusterState.builder(previousState)
            .metaData(updatedPollMetaData)
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, previousState);

        ClusterChangedEvent noChangeEvent = new ClusterChangedEvent("_source", previousState, previousState);
        indexLifecycleService.applyClusterState(noChangeEvent);
        indexLifecycleService.clusterChanged(noChangeEvent);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertThat(((TimeValueSchedule) indexLifecycleService.getScheduledJob().getSchedule()).getInterval(),
                equalTo(TimeValue.timeValueSeconds(3)));
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertThat(((TimeValueSchedule) indexLifecycleService.getScheduledJob().getSchedule()).getInterval(), equalTo(pollInterval));
        noChangeEvent = new ClusterChangedEvent("_source", currentState, currentState);
        indexLifecycleService.applyClusterState(noChangeEvent);
        indexLifecycleService.clusterChanged(noChangeEvent);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertThat(((TimeValueSchedule) indexLifecycleService.getScheduledJob().getSchedule()).getInterval(), equalTo(pollInterval));

        verify(clusterService, times(1)).addListener(any());
        verify(clusterService, times(1)).addStateApplier(any());
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testInstallMetadataFail() {
        TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(1, 59));
        MetaData metaData = MetaData.builder() .persistentSettings(settings(Version.CURRENT)
            .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollInterval).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", state, state);

        doThrow(new RuntimeException("error")).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));

        Exception exception = expectThrows(RuntimeException.class, () -> indexLifecycleService.clusterChanged(event));
        assertThat(exception.getMessage(), equalTo("error"));

        verify(clusterService, times(1)).addListener(any());
        verify(clusterService, times(1)).addStateApplier(any());
        verify(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        Mockito.verifyNoMoreInteractions(clusterService);
        assertNull(indexLifecycleService.getScheduler());
    }

//    /**
//     * Checks that a new index does the following successfully:
//     *
//     * 1. setting index.lifecycle.date
//     * 2. sets phase
//     * 3. sets action
//     * 4. executes action
//     */
//    @SuppressWarnings("unchecked")
//    public void testTriggeredWithMatchingPolicy() {
//        String policyName = randomAlphaOfLengthBetween(1, 20);
//        MockAction mockAction = new MockAction(Collections.emptyList());
//        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
//        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName,
//                Collections.singletonMap(phase.getName(), phase));
//        SortedMap<String, LifecyclePolicy> policyMap = new TreeMap<>();
//        policyMap.put(policyName, policy);
//        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
//            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName))
//            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
//        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
//            .fPut(index.getName(), indexMetadata);
//        MetaData metaData = MetaData.builder()
//            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap))
//            .indices(indices.build())
//            .persistentSettings(settings(Version.CURRENT).build())
//            .build();
//        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
//            .metaData(metaData)
//            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
//            .build();
//
//        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event(IndexLifecycle.NAME, randomLong(), randomLong());
//
//        when(clusterService.state()).thenReturn(currentState);
//
//        SetOnce<Boolean> dateUpdated = new SetOnce<>();
//        SetOnce<Boolean> phaseUpdated = new SetOnce<>();
//        SetOnce<Boolean> actionUpdated = new SetOnce<>();
//        doAnswer(invocationOnMock -> {
//            UpdateSettingsRequest request = (UpdateSettingsRequest)  invocationOnMock.getArguments()[0];
//            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
//            UpdateSettingsTestHelper.assertSettingsRequest(request, Settings.builder()
//                .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.getKey(),
//                    indexMetadata.getCreationDate()).build(), index.getName());
//            dateUpdated.set(true);
//            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
//            return null;
//        }).doAnswer(invocationOnMock -> {
//            UpdateSettingsRequest request = (UpdateSettingsRequest)  invocationOnMock.getArguments()[0];
//            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
//            UpdateSettingsTestHelper.assertSettingsRequest(request, Settings.builder()
//                .put(LifecycleSettings.LIFECYCLE_ACTION, "")
//                .put(LifecycleSettings.LIFECYCLE_ACTION_TIME, -1L)
//                .put(LifecycleSettings.LIFECYCLE_PHASE_TIME, now)
//                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase").build(), index.getName());
//            phaseUpdated.set(true);
//            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
//            return null;
//        }).doAnswer(invocationOnMock -> {
//            UpdateSettingsRequest request = (UpdateSettingsRequest)  invocationOnMock.getArguments()[0];
//            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
//            UpdateSettingsTestHelper.assertSettingsRequest(request, Settings.builder()
//                .put(LifecycleSettings.LIFECYCLE_ACTION, MockAction.NAME)
//                .put(LifecycleSettings.LIFECYCLE_ACTION_TIME, now).build(), index.getName());
//            actionUpdated.set(true);
//            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
//            return null;
//        }).when(indicesClient).updateSettings(any(), any());
//
//        indexLifecycleService.triggered(schedulerEvent);
//
//        assertThat(dateUpdated.get(), equalTo(true));
//        assertThat(phaseUpdated.get(), equalTo(true));
//        assertThat(actionUpdated.get(), equalTo(true));
//    }

//    /**
//     * Check that a policy is executed without first setting the `index.lifecycle.date` setting
//     */
//    @SuppressWarnings("unchecked")
//    public void testTriggeredWithDateSettingAlreadyPresent() {
//        String policyName = randomAlphaOfLengthBetween(1, 20);
//        MockAction mockAction = new MockAction(Collections.emptyList());
//        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
//        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName,
//            Collections.singletonMap(phase.getName(), phase));
//        SortedMap<String, LifecyclePolicy> policyMap = new TreeMap<>();
//        policyMap.put(policyName, policy);
//        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//        long creationDate = randomLongBetween(0, now - 1);
//        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
//            .settings(settings(Version.CURRENT)
//                .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName)
//                .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.getKey(), creationDate))
//            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).creationDate(creationDate).build();
//        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
//            .fPut(index.getName(), indexMetadata);
//        MetaData metaData = MetaData.builder()
//            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap))
//            .indices(indices.build())
//            .persistentSettings(settings(Version.CURRENT).build())
//            .build();
//        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
//            .metaData(metaData)
//            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
//            .build();
//
//        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event(IndexLifecycle.NAME, randomLong(), randomLong());
//
//        when(clusterService.state()).thenReturn(currentState);
//
//        SetOnce<Boolean> dateUpdated = new SetOnce<>();
//        doAnswer(invocationOnMock -> {
//            UpdateSettingsRequest request = (UpdateSettingsRequest)  invocationOnMock.getArguments()[0];
//            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
//            try {
//                UpdateSettingsTestHelper.assertSettingsRequest(request, Settings.builder()
//                    .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.getKey(),
//                        indexMetadata.getCreationDate()).build(), index.getName());
//                dateUpdated.set(true);
//            } catch (AssertionError e) {
//                // noop: here because we are either updating the phase or action prior to executing MockAction
//            }
//            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
//            return null;
//        }).when(indicesClient).updateSettings(any(), any());
//
//        indexLifecycleService.triggered(schedulerEvent);
//
//        assertNull(dateUpdated.get());
//    }

//    /**
//     * Check that if an index has an unknown lifecycle policy set it does not
//     * execute any policy but does process other indexes.
//     */
//    public void testTriggeredUnknownPolicyNameSet() {
//        String policyName = randomAlphaOfLengthBetween(1, 20);
//        MockAction mockAction = new MockAction(Collections.emptyList());
//        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
//        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName,
//                Collections.singletonMap(phase.getName(), phase));
//        SortedMap<String, LifecyclePolicy> policyMap = new TreeMap<>();
//        policyMap.put(policyName, policy);
//        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
//                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), "foo"))
//                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
//        Index index2 = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//        IndexMetaData indexMetadata2 = IndexMetaData.builder(index2.getName())
//                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName))
//                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
//        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
//                indexMetadata).fPut(index2.getName(), indexMetadata2);
//        MetaData metaData = MetaData.builder().putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap))
//                .indices(indices.build()).persistentSettings(settings(Version.CURRENT).build()).build();
//        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData)
//                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build()).build();
//
//        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event(IndexLifecycle.NAME, randomLong(), randomLong());
//
//        when(clusterService.state()).thenReturn(currentState);
//
//        doAnswer(invocationOnMock -> {
//            @SuppressWarnings("unchecked")
//            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
//            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
//            return null;
//
//        }).when(indicesClient).updateSettings(any(), any());
//
//        indexLifecycleService.triggered(schedulerEvent);
//    }

//    /**
//     * Check that if an index has no lifecycle policy set it does not execute
//     * any policy but does process other indexes.
//     */
//    public void testTriggeredNoPolicyNameSet() {
//        String policyName = randomAlphaOfLengthBetween(1, 20);
//        MockAction mockAction = new MockAction(Collections.emptyList());
//        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
//        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName,
//                Collections.singletonMap(phase.getName(), phase));
//        SortedMap<String, LifecyclePolicy> policyMap = new TreeMap<>();
//        policyMap.put(policyName, policy);
//        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName()).settings(settings(Version.CURRENT))
//                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
//        Index index2 = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//        IndexMetaData indexMetadata2 = IndexMetaData.builder(index2.getName())
//                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName))
//                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
//        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
//                indexMetadata).fPut(index2.getName(), indexMetadata2);
//        MetaData metaData = MetaData.builder().putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap))
//                .indices(indices.build()).persistentSettings(settings(Version.CURRENT).build()).build();
//        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData)
//                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build()).build();
//
//        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event(IndexLifecycle.NAME, randomLong(), randomLong());
//
//        when(clusterService.state()).thenReturn(currentState);
//
//        doAnswer(invocationOnMock -> {
//            @SuppressWarnings("unchecked")
//            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
//            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
//            return null;
//
//        }).when(indicesClient).updateSettings(any(), any());
//
//        indexLifecycleService.triggered(schedulerEvent);
//    }

    public void testTriggeredDifferentJob() {
        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event("foo", randomLong(), randomLong());
        indexLifecycleService.triggered(schedulerEvent);
        Mockito.verifyZeroInteractions(indicesClient, clusterService);
    }
}
