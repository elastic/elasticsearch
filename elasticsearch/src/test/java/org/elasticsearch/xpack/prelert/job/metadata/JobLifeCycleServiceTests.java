/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.action.UpdateJobStatusAction;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobService;
import org.junit.Before;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JobLifeCycleServiceTests extends ESTestCase {

    private DataProcessor dataProcessor;
    private Client client;
    private JobLifeCycleService jobLifeCycleService;

    @Before
    public void instantiateJobAllocator() {
        ClusterService clusterService = mock(ClusterService.class);
        ScheduledJobService scheduledJobService = mock(ScheduledJobService.class);
        dataProcessor = mock(DataProcessor.class);
        client = mock(Client.class);
        jobLifeCycleService = new JobLifeCycleService(Settings.EMPTY, client, clusterService, scheduledJobService, dataProcessor,
                Runnable::run);
    }

    public void testStartStop() {
        Allocation.Builder allocation = new Allocation.Builder();
        allocation.setJobId("_job_id");
        jobLifeCycleService.startJob(allocation.build());
        assertTrue(jobLifeCycleService.localAssignedJobs.contains("_job_id"));
        verify(dataProcessor).openJob("_job_id", false);

        jobLifeCycleService.stopJob("_job_id");
        assertTrue(jobLifeCycleService.localAssignedJobs.isEmpty());
        verify(dataProcessor).closeJob("_job_id");
    }

    public void testClusterChanged_startJob() {
        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        pmBuilder.createAllocation("_job_id", false);
        ClusterState cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));
        assertFalse("not allocated to a node",
                jobLifeCycleService.localAssignedJobs.contains("_job_id"));

        pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        pmBuilder.createAllocation("_job_id", false);
        pmBuilder.updateStatus("_job_id", JobStatus.OPENED, null);
        cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));
        assertFalse("Status not started",
                jobLifeCycleService.localAssignedJobs.contains("_job_id"));

        pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        pmBuilder.createAllocation("_job_id", false);
        pmBuilder.assignToNode("_job_id", "_node_id");
        cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));
        assertTrue("Expect allocation, because job allocation says _job_id should be allocated locally",
                jobLifeCycleService.localAssignedJobs.contains("_job_id"));
        verify(dataProcessor, times(1)).openJob("_job_id", false);

        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));
        verify(dataProcessor, times(1)).openJob("_job_id", false);
    }

    public void testClusterChanged_stopJob() {
        jobLifeCycleService.localAssignedJobs.add("_job_id");

        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        pmBuilder.createAllocation("_job_id", false);
        ClusterState cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));
        assertEquals("Status is not closing, so nothing happened", jobLifeCycleService.localAssignedJobs.size(), 1);

        pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        pmBuilder.createAllocation("_job_id", false);
        pmBuilder.updateStatus("_job_id", JobStatus.OPENED, null);
        pmBuilder.updateStatus("_job_id", JobStatus.CLOSING, null);
        pmBuilder.assignToNode("_job_id", "_node_id");
        cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));
        assertEquals(jobLifeCycleService.localAssignedJobs.size(), 0);
        verify(dataProcessor, times(1)).closeJob("_job_id");
    }

    public void testClusterChanged_allocationRemovedStopJob() {
        jobLifeCycleService.localAssignedJobs.add("_job_id");

        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        ClusterState cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));
        assertEquals(jobLifeCycleService.localAssignedJobs.size(), 0);
        verify(dataProcessor, times(1)).closeJob("_job_id");
    }

    public void testStart_openJobFails() {
        doThrow(new RuntimeException("error")).when(dataProcessor).openJob("_job_id", false);
        Allocation.Builder allocation = new Allocation.Builder();
        allocation.setJobId("_job_id");
        jobLifeCycleService.startJob(allocation.build());
        assertTrue(jobLifeCycleService.localAssignedJobs.contains("_job_id"));
        verify(dataProcessor).openJob("_job_id", false);
        UpdateJobStatusAction.Request expectedRequest = new UpdateJobStatusAction.Request("_job_id", JobStatus.FAILED);
        expectedRequest.setReason("failed to open, error");
        verify(client).execute(eq(UpdateJobStatusAction.INSTANCE), eq(expectedRequest), any());
    }

    public void testStart_closeJobFails() {
        jobLifeCycleService.localAssignedJobs.add("_job_id");
        doThrow(new RuntimeException("error")).when(dataProcessor).closeJob("_job_id");
        jobLifeCycleService.stopJob("_job_id");
        assertEquals(jobLifeCycleService.localAssignedJobs.size(), 0);
        verify(dataProcessor).closeJob("_job_id");
        UpdateJobStatusAction.Request expectedRequest = new UpdateJobStatusAction.Request("_job_id", JobStatus.FAILED);
        expectedRequest.setReason("failed to close, error");
        verify(client).execute(eq(UpdateJobStatusAction.INSTANCE), eq(expectedRequest), any());
    }
}
