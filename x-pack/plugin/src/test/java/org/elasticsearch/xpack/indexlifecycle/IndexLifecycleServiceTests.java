/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsTestHelper;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.node.Node.NODE_MASTER_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexLifecycleServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private IndexLifecycleService indexLifecycleService;
    private String nodeId;
    private DiscoveryNode masterNode;
    private IndicesAdminClient indicesClient;

    @Before
    public void prepareServices() {
        nodeId = randomAlphaOfLength(10);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        clusterService = mock(ClusterService.class);
        masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        long randomMilli = randomNonNegativeLong();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(randomMilli), ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds())));

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
            threadPool, () -> randomMilli);
    }

    public void testOnlyChangesStateOnMaster() throws Exception {
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT)
                .put(IndexLifecycle.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(3)).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId + "not").masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", state, state);

        indexLifecycleService.clusterChanged(event);
        verify(clusterService, only()).addListener(any());
        assertNull(indexLifecycleService.getScheduler());
    }

    public void testServiceSetupOnFirstClusterChange() {
        TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(1, 59));
        MetaData metaData = MetaData.builder() .persistentSettings(settings(Version.CURRENT)
                .put(IndexLifecycle.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollInterval).build())
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
            assertThat(indexLifecycleMetadata.getPolicies(), equalTo(Collections.emptySortedMap()));
            installedEvent.set(new ClusterChangedEvent(event.source(), newState, state));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));

        indexLifecycleService.clusterChanged(event);

        verify(clusterService).addListener(any());
        verify(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
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
            .put(IndexLifecycle.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollInterval).build())
            .build();
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterState currentState = ClusterState.builder(previousState)
            .metaData(updatedPollMetaData)
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, previousState);

        indexLifecycleService.clusterChanged(new ClusterChangedEvent("_source", previousState, previousState));
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertThat(((IntervalSchedule)indexLifecycleService.getScheduledJob().getSchedule()).interval(),
            equalTo(new IntervalSchedule.Interval(3, IntervalSchedule.Interval.Unit.SECONDS)));
        indexLifecycleService.clusterChanged(event);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertThat(((IntervalSchedule)indexLifecycleService.getScheduledJob().getSchedule()).interval(),
            equalTo(new IntervalSchedule.Interval(pollInterval.seconds(), IntervalSchedule.Interval.Unit.SECONDS)));

        verify(clusterService, only()).addListener(any());
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testInstallMetadataFail() {
        TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(1, 59));
        MetaData metaData = MetaData.builder() .persistentSettings(settings(Version.CURRENT)
            .put(IndexLifecycle.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollInterval).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", state, state);

        doThrow(new RuntimeException("error")).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));

        Exception exception = expectThrows(RuntimeException.class, () -> indexLifecycleService.clusterChanged(event));
        assertThat(exception.getMessage(), equalTo("error"));

        verify(clusterService).addListener(any());
        verify(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        assertNull(indexLifecycleService.getScheduler());
    }

    @SuppressWarnings("unchecked")
    public void testTriggeredWithMatchingPolicy() {
        String policyName = randomAlphaOfLengthBetween(1, 20);
        MockAction mockAction = new MockAction();
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName,
                Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicy> policyMap = new TreeMap<>();
        policyMap.put(policyName, policy);
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(IndexLifecycle.LIFECYCLE_NAME_SETTING.getKey(), policyName))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
            .fPut(index.getName(), indexMetadata);
        MetaData metaData = MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event(IndexLifecycle.NAME, randomLong(), randomLong());

        when(clusterService.state()).thenReturn(currentState);

        doAnswer(invocationOnMock -> {
            ActionListener<UpdateSettingsResponse> listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
            return null;

        }).when(indicesClient).updateSettings(any(), any());

        indexLifecycleService.triggered(schedulerEvent);

        assertThat(mockAction.getExecutedCount(), equalTo(1L));
    }
}
