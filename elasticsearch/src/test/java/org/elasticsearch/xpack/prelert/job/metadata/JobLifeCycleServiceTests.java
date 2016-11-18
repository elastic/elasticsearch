/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobService;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

public class JobLifeCycleServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private ScheduledJobService scheduledJobService;
    private DataProcessor dataProcessor;
    private Client client;
    private JobLifeCycleService jobLifeCycleService;

    @Before
    public void instantiateJobAllocator() {
        clusterService = Mockito.mock(ClusterService.class);
        scheduledJobService = Mockito.mock(ScheduledJobService.class);
        dataProcessor = Mockito.mock(DataProcessor.class);
        client = Mockito.mock(Client.class);
        jobLifeCycleService = new JobLifeCycleService(Settings.EMPTY, client, clusterService, scheduledJobService, dataProcessor,
                Runnable::run);
    }

    public void testStartStop() {
        jobLifeCycleService.startJob(buildJobBuilder("_job_id").build());
        assertTrue(jobLifeCycleService.localAllocatedJobs.contains("_job_id"));
        jobLifeCycleService.stopJob("_job_id");
        assertTrue(jobLifeCycleService.localAllocatedJobs.isEmpty());
    }

    public void testClusterChanged() {
        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        pmBuilder.putAllocation("_node_id", "_job_id");
        ClusterState cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));
        assertTrue("Expect allocation, because job allocation says _job_id should be allocated locally",
                jobLifeCycleService.localAllocatedJobs.contains("_job_id"));

        pmBuilder.removeJob("_job_id");
        ClusterState cs2 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs2, cs1));
        assertFalse("Expect no allocation, because the job has been removed", jobLifeCycleService.localAllocatedJobs.contains("_job_id"));
    }

    public void testClusterChanged_GivenJobIsPausing() {
        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder();
        Job.Builder job = buildJobBuilder("foo");
        pmBuilder.putJob(job.build(), false);
        pmBuilder.putAllocation("_node_id", "foo");
        Allocation.Builder allocation = new Allocation.Builder();
        allocation.setJobId("foo");
        allocation.setNodeId("_node_id");
        allocation.setStatus(JobStatus.PAUSING);
        pmBuilder.updateAllocation("foo", allocation.build());
        ClusterState cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();

        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));

        verify(dataProcessor).closeJob("foo");
        UpdateJobStatusAction.Request expectedRequest = new UpdateJobStatusAction.Request("foo", JobStatus.PAUSED);
        verify(client).execute(eq(UpdateJobStatusAction.INSTANCE), eq(expectedRequest), any());
    }

    public void testClusterChanged_GivenJobIsPausingAndCloseJobThrows() {
        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder();
        Job.Builder job = buildJobBuilder("foo");
        pmBuilder.putJob(job.build(), false);
        pmBuilder.putAllocation("_node_id", "foo");
        Allocation.Builder allocation = new Allocation.Builder();
        allocation.setJobId("foo");
        allocation.setNodeId("_node_id");
        allocation.setStatus(JobStatus.PAUSING);
        pmBuilder.updateAllocation("foo", allocation.build());
        ClusterState cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id"))
                .build();
        doThrow(new ElasticsearchException("")).when(dataProcessor).closeJob("foo");

        jobLifeCycleService.clusterChanged(new ClusterChangedEvent("_source", cs1, cs1));

        verify(dataProcessor).closeJob("foo");
        UpdateJobStatusAction.Request expectedRequest = new UpdateJobStatusAction.Request("foo", JobStatus.FAILED);
        verify(client).execute(eq(UpdateJobStatusAction.INSTANCE), eq(expectedRequest), any());
    }
}
