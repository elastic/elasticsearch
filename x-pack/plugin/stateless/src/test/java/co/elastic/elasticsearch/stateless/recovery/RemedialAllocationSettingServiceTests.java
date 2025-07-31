/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.recovery.RemedialAllocationSettingService.RemedialAllocationSettingTask;
import co.elastic.elasticsearch.stateless.recovery.RemedialAllocationSettingService.RemediateAllocationSettingClusterStateExecutor;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.getProjectWithDataStreams;
import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RemedialAllocationSettingServiceTests extends ESTestCase {

    public void testRemediationSettingsPresent() throws Exception {
        @SuppressWarnings("unchecked")
        MasterServiceTaskQueue<ClusterStateTaskListener> mockQueue = mock(MasterServiceTaskQueue.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any())).thenReturn(
            mockQueue
        );

        // Settings for both having and not having the required allocation setting
        Settings hasAllocation = Settings.builder()
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), Stateless.NAME)
            .build();

        // Single data stream project, failure store enabled, but has the allocation settings
        ProjectMetadata project = getProjectWithDataStreams(
            List.of(tuple("p1-ds1", 1)),
            List.of("p1-i1"),
            System.currentTimeMillis(),
            hasAllocation,
            0,
            false,
            true
        );

        var state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();

        ClusterChangedEvent mockEvent = mock(ClusterChangedEvent.class);
        when(mockEvent.state()).thenReturn(state);
        when(mockEvent.localNodeMaster()).thenReturn(true);

        // Create service and verify
        RemedialAllocationSettingService testService = new RemedialAllocationSettingService(mockClusterService);
        verify(mockClusterService).createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any());

        // First stage - cluster changed
        testService.clusterChanged(mockEvent);

        // Verify Interactions
        verifyNoInteractions(mockQueue);
        verify(mockClusterService).removeListener(same(testService));
        assertThat(testService.isRemediationComplete(), is(true));
    }

    public void testRemediationRequired() throws Exception {
        @SuppressWarnings("unchecked")
        MasterServiceTaskQueue<ClusterStateTaskListener> mockQueue = mock(MasterServiceTaskQueue.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any())).thenReturn(
            mockQueue
        );

        // Settings for both having and not having the required allocation setting
        Settings missingAllocation = Settings.EMPTY;

        // Another project with a few more streams, failure store enabled, missing allocation settings
        ProjectMetadata project = getProjectWithDataStreams(
            List.of(tuple("p2-ds1", 1), tuple("p2-ds2", 3)),
            List.of("p2-i1"),
            System.currentTimeMillis(),
            missingAllocation,
            0,
            false,
            true
        );

        var state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();

        ClusterChangedEvent mockEvent = mock(ClusterChangedEvent.class);
        when(mockEvent.state()).thenReturn(state);
        when(mockEvent.localNodeMaster()).thenReturn(true);

        // Create service and extract the executor it will use
        RemedialAllocationSettingService testService = new RemedialAllocationSettingService(mockClusterService);
        ArgumentCaptor<RemediateAllocationSettingClusterStateExecutor> executorArgumentCaptor = ArgumentCaptor.forClass(
            RemediateAllocationSettingClusterStateExecutor.class
        );
        verify(mockClusterService).createTaskQueue(
            eq("remediate-stateful-allocation-settings"),
            eq(Priority.URGENT),
            executorArgumentCaptor.capture()
        );
        RemediateAllocationSettingClusterStateExecutor clusterExecutor = executorArgumentCaptor.getValue();

        // First stage - cluster changed
        testService.clusterChanged(mockEvent);

        // Verify and capture the task object that should have been submitted
        ArgumentCaptor<RemedialAllocationSettingTask> taskArgumentCaptor = ArgumentCaptor.forClass(RemedialAllocationSettingTask.class);
        verify(mockQueue).submitTask(
            eq("remediate-serverless-index-settings"),
            taskArgumentCaptor.capture(),
            eq(MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT)
        );
        RemedialAllocationSettingTask clusterTask = taskArgumentCaptor.getValue();

        // Second stage - cluster update
        Tuple<ClusterState, Void> updateResult = clusterExecutor.executeTask(clusterTask, state);

        // Validations
        assertThat(updateResult.v2(), nullValue());
        Metadata updatedMetadata = updateResult.v1().metadata();

        // All of project's failure indices have been updated with the missing setting
        ProjectMetadata projectUpdated = updatedMetadata.getProject(project.id());
        projectUpdated.dataStreams()
            .values()
            .stream()
            .map(DataStream::getFailureIndices)
            .flatMap(Collection::stream)
            .map(projectUpdated::index)
            .forEach(idxMetadata -> {
                assertThat(
                    idxMetadata.getIndex().getName(),
                    ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(idxMetadata.getSettings()),
                    equalTo(Stateless.NAME)
                );
            });
        // None of the other resources in project are touched - they aren't the target of this fix
        projectUpdated.dataStreams()
            .values()
            .stream()
            .map(DataStream::getIndices)
            .flatMap(Collection::stream)
            .map(projectUpdated::index)
            .forEach(idxMetadata -> {
                assertThat(
                    idxMetadata.getIndex().getName(),
                    ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(idxMetadata.getSettings()),
                    equalTo(GatewayAllocator.ALLOCATOR_NAME)
                );
            });
        IndexMetadata projectIndex1 = projectUpdated.index("p2-i1");
        assertThat(
            projectIndex1.getIndex().getName(),
            ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(projectIndex1.getSettings()),
            equalTo(GatewayAllocator.ALLOCATOR_NAME)
        );

        // Complete listener and ensure remediation is complete
        clusterExecutor.taskSucceeded(clusterTask, updateResult.v2());
        verify(mockClusterService).removeListener(same(testService));
        assertThat(testService.isRemediationComplete(), is(true));
    }

    public void testRemediationNoFailureIndices() throws Exception {
        @SuppressWarnings("unchecked")
        MasterServiceTaskQueue<ClusterStateTaskListener> mockQueue = mock(MasterServiceTaskQueue.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any())).thenReturn(
            mockQueue
        );

        // Settings for both having and not having the required allocation setting
        Settings missingAllocation = Settings.EMPTY;

        // A project with a stream, but without failure store enabled
        ProjectMetadata project3 = getProjectWithDataStreams(
            List.of(tuple("p3-ds1", 3)),
            List.of("p3-i1"),
            System.currentTimeMillis(),
            missingAllocation,
            0,
            false,
            false
        );

        var state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project3).build();

        ClusterChangedEvent mockEvent = mock(ClusterChangedEvent.class);
        when(mockEvent.state()).thenReturn(state);
        when(mockEvent.localNodeMaster()).thenReturn(true);

        // Create service and verify
        RemedialAllocationSettingService testService = new RemedialAllocationSettingService(mockClusterService);
        verify(mockClusterService).createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any());

        // First stage - cluster changed
        testService.clusterChanged(mockEvent);

        // Verify interactions
        verifyNoInteractions(mockQueue);
        verify(mockClusterService).removeListener(same(testService));
        assertThat(testService.isRemediationComplete(), is(true));
    }

    public void testNoExecutionDuringStateRecovery() throws Exception {
        @SuppressWarnings("unchecked")
        MasterServiceTaskQueue<ClusterStateTaskListener> mockQueue = mock(MasterServiceTaskQueue.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any())).thenReturn(
            mockQueue
        );

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();

        ClusterChangedEvent mockEvent = mock(ClusterChangedEvent.class);
        when(mockEvent.state()).thenReturn(state);
        when(mockEvent.localNodeMaster()).thenReturn(true);

        // Create service and extract the executor it would use
        RemedialAllocationSettingService testService = new RemedialAllocationSettingService(mockClusterService);

        verify(mockClusterService).createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any());

        testService.clusterChanged(mockEvent);

        verifyNoMoreInteractions(mockClusterService);
        verifyNoInteractions(mockQueue);
        assertThat(testService.isRemediationComplete(), is(false));
    }

    public void testEarlyOutAfterRemediationSuccess() throws Exception {
        @SuppressWarnings("unchecked")
        MasterServiceTaskQueue<ClusterStateTaskListener> mockQueue = mock(MasterServiceTaskQueue.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any())).thenReturn(
            mockQueue
        );

        // Required allocation setting
        Settings hasAllocation = Settings.builder()
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), Stateless.NAME)
            .build();

        // Single data stream project, no remediation needed
        ProjectMetadata project1 = getProjectWithDataStreams(
            List.of(tuple("p1-ds1", 1)),
            List.of("p1-i1"),
            System.currentTimeMillis(),
            hasAllocation,
            0,
            false,
            true
        );

        var state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project1).build();

        ClusterChangedEvent mockEvent = mock(ClusterChangedEvent.class);
        when(mockEvent.state()).thenReturn(state);
        when(mockEvent.localNodeMaster()).thenReturn(true);

        // Create service and extract the executor it will use
        RemedialAllocationSettingService testService = new RemedialAllocationSettingService(mockClusterService);

        // Verify queue creation
        verify(mockClusterService).createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any());

        // First stage - cluster changed
        testService.clusterChanged(mockEvent);
        assertThat(testService.isRemediationComplete(), is(true));

        // Try to run service again, it should try and remove the listener twice total
        testService.clusterChanged(mockEvent);

        verifyNoInteractions(mockQueue);
        verify(mockClusterService, times(2)).removeListener(same(testService));
        verifyNoMoreInteractions(mockClusterService);
        assertThat(testService.isRemediationComplete(), is(true));
    }

    public void testSkipOperationIfNotMaster() throws Exception {
        @SuppressWarnings("unchecked")
        MasterServiceTaskQueue<ClusterStateTaskListener> mockQueue = mock(MasterServiceTaskQueue.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any())).thenReturn(
            mockQueue
        );

        var state = ClusterState.builder(ClusterName.DEFAULT).build();

        ClusterChangedEvent mockEvent = mock(ClusterChangedEvent.class);
        when(mockEvent.state()).thenReturn(state);
        when(mockEvent.localNodeMaster()).thenReturn(false);

        // Create service and extract the executor it will use
        RemedialAllocationSettingService testService = new RemedialAllocationSettingService(mockClusterService);

        // Verify queue creation
        verify(mockClusterService).createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any());

        // First stage - cluster changed
        testService.clusterChanged(mockEvent);
        assertThat(testService.isRemediationComplete(), is(false));

        // Try to run service again, it should try and remove the listener twice total
        testService.clusterChanged(mockEvent);

        assertThat(testService.isRemediationComplete(), is(false));
        verifyNoMoreInteractions(mockClusterService);
        verifyNoInteractions(mockQueue);
    }

    public void testRemediationOnUpdatedState() throws Exception {
        @SuppressWarnings("unchecked")
        MasterServiceTaskQueue<ClusterStateTaskListener> mockQueue = mock(MasterServiceTaskQueue.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.createTaskQueue(eq("remediate-stateful-allocation-settings"), eq(Priority.URGENT), any())).thenReturn(
            mockQueue
        );

        // Settings for both having and not having the required allocation setting
        Settings missingAllocation = Settings.EMPTY;
        Settings hasAllocation = Settings.builder()
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), Stateless.NAME)
            .build();

        // Single data stream project, failure store enabled, but has the allocation settings
        ProjectMetadata project1 = getProjectWithDataStreams(
            List.of(tuple("p1-ds1", 1)),
            List.of("p1-i1"),
            System.currentTimeMillis(),
            hasAllocation,
            0,
            false,
            true
        );

        // A project with a few more streams, failure store enabled, missing allocation settings
        ProjectMetadata project = getProjectWithDataStreams(
            List.of(tuple("p2-ds1", 1), tuple("p2-ds2", 3)),
            List.of("p2-i1"),
            System.currentTimeMillis(),
            missingAllocation,
            0,
            false,
            true
        );

        // First state has project 2 (broken)
        var stateBroken = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(randomFrom(project)).build();

        // Next state returned has project 1 (no remediation needed)
        var stateFixed = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project1).build();

        ClusterChangedEvent mockEvent = mock(ClusterChangedEvent.class);
        when(mockEvent.state()).thenReturn(stateBroken);
        when(mockEvent.localNodeMaster()).thenReturn(true);

        // Create service and extract the executor it will use
        RemedialAllocationSettingService testService = new RemedialAllocationSettingService(mockClusterService);
        ArgumentCaptor<RemediateAllocationSettingClusterStateExecutor> executorArgumentCaptor = ArgumentCaptor.forClass(
            RemediateAllocationSettingClusterStateExecutor.class
        );
        verify(mockClusterService).createTaskQueue(
            eq("remediate-stateful-allocation-settings"),
            eq(Priority.URGENT),
            executorArgumentCaptor.capture()
        );
        RemediateAllocationSettingClusterStateExecutor clusterExecutor = executorArgumentCaptor.getValue();

        // First stage - cluster changed
        testService.clusterChanged(mockEvent);

        // Verify and capture the task object that should have been submitted
        ArgumentCaptor<RemedialAllocationSettingTask> taskArgumentCaptor = ArgumentCaptor.forClass(RemedialAllocationSettingTask.class);
        verify(mockQueue).submitTask(
            eq("remediate-serverless-index-settings"),
            taskArgumentCaptor.capture(),
            eq(MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT)
        );
        RemedialAllocationSettingTask clusterTask = taskArgumentCaptor.getValue();

        // Second stage - cluster update, send the "fixed" state
        Tuple<ClusterState, Void> updateResult = clusterExecutor.executeTask(clusterTask, stateFixed);

        // Validations - state should be identical
        assertThat(updateResult.v1(), equalTo(updateResult.v1()));
        assertThat(updateResult.v2(), nullValue());

        // Complete listener and ensure remediation is complete
        clusterExecutor.taskSucceeded(clusterTask, updateResult.v2());
        verify(mockClusterService).removeListener(same(testService));
        assertThat(testService.isRemediationComplete(), is(true));
    }
}
