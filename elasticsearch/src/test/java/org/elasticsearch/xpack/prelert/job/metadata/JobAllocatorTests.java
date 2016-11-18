/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.Version;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.mockito.Matchers.any;

public class JobAllocatorTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private JobAllocator jobAllocator;

    @Before
    public void instantiateJobAllocator() {
        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);
        jobAllocator = new JobAllocator(Settings.EMPTY, clusterService, threadPool);
    }

    public void testShouldAllocate() {
        ClusterState cs = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, new PrelertMetadata.Builder().build()))
                .build();
        assertFalse("No jobs, so nothing to allocate", jobAllocator.shouldAllocate(cs));

        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder(cs.metaData().custom(PrelertMetadata.TYPE));
        pmBuilder.putJob((buildJobBuilder("_job_id").build()), false);
        cs = ClusterState.builder(cs).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .build();
        assertTrue("A unassigned job, so we should allocate", jobAllocator.shouldAllocate(cs));

        pmBuilder = new PrelertMetadata.Builder(cs.metaData().custom(PrelertMetadata.TYPE));
        pmBuilder.putAllocation("_node_id", "_job_id");
        cs = ClusterState.builder(cs).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .build();
        assertFalse("Job is allocate, so nothing to allocate", jobAllocator.shouldAllocate(cs));
    }

    public void testAllocateJobs() {
        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        ClusterState cs1 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .masterNodeId("_node_id"))
                .build();
        ClusterState result1 = jobAllocator.allocateJobs(cs1);
        PrelertMetadata pm = result1.metaData().custom(PrelertMetadata.TYPE);
        assertEquals("_job_id must be allocated to _node_id", pm.getAllocations().get("_job_id").getNodeId(), "_node_id");

        ClusterState result2 = jobAllocator.allocateJobs(result1);
        assertSame("job has been allocated, same instance must be returned", result1, result2);

        ClusterState cs2 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(
                        DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id1", new LocalTransportAddress("_id1"), Version.CURRENT))
                        .add(new DiscoveryNode("_node_id2", new LocalTransportAddress("_id2"), Version.CURRENT))
                        .masterNodeId("_node_id1")
                        )
                .build();
        // should fail, prelert only support single node for now
        expectThrows(IllegalStateException.class, () -> jobAllocator.allocateJobs(cs2));

        ClusterState cs3 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .build();
        // we need to have at least one node
        expectThrows(IllegalStateException.class, () -> jobAllocator.allocateJobs(cs3));

        pmBuilder = new PrelertMetadata.Builder(result1.getMetaData().custom(PrelertMetadata.TYPE));
        pmBuilder.removeJob("_job_id");
        ClusterState cs4 = ClusterState.builder(new ClusterName("_cluster_name")).metaData(MetaData.builder()
                .putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .masterNodeId("_node_id"))
                .build();
        ClusterState result3 = jobAllocator.allocateJobs(cs4);
        pm = result3.metaData().custom(PrelertMetadata.TYPE);
        assertNull("_job_id must be unallocated, because job has been removed", pm.getAllocations().get("_job_id"));
    }

    public void testClusterChanged_onlyAllocateIfMasterAndHaveUnAllocatedJobs() {
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);


        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, new PrelertMetadata.Builder().build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_id")
                        )
                .build();
        jobAllocator.clusterChanged(new ClusterChangedEvent("_source", cs, cs));
        verify(threadPool, never()).executor(ThreadPool.Names.GENERIC);
        verify(clusterService, never()).submitStateUpdateTask(any(), any());

        // make node master
        cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, new PrelertMetadata.Builder().build()))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .masterNodeId("_id")
                        .localNodeId("_id")
                        )
                .build();
        jobAllocator.clusterChanged(new ClusterChangedEvent("_source", cs, cs));
        verify(threadPool, never()).executor(ThreadPool.Names.GENERIC);
        verify(clusterService, never()).submitStateUpdateTask(any(), any());

        // add an allocated job
        PrelertMetadata.Builder pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_id").build(), false);
        pmBuilder.putAllocation("_id", "_id");
        cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .masterNodeId("_id")
                        .localNodeId("_id")
                        )
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .build();
        jobAllocator.clusterChanged(new ClusterChangedEvent("_source", cs, cs));
        verify(threadPool, never()).executor(ThreadPool.Names.GENERIC);
        verify(clusterService, never()).submitStateUpdateTask(any(), any());

        // make job not allocated
        pmBuilder = new PrelertMetadata.Builder();
        pmBuilder.putJob(buildJobBuilder("_job_id").build(), false);
        cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .masterNodeId("_id")
                        .localNodeId("_id")
                        )
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, pmBuilder.build()))
                .build();
        jobAllocator.clusterChanged(new ClusterChangedEvent("_source", cs, cs));
        verify(threadPool, times(1)).executor(ThreadPool.Names.GENERIC);
        verify(clusterService, times(1)).submitStateUpdateTask(any(), any());
    }

}
