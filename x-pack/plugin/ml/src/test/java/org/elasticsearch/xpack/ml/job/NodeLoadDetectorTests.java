/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO: in 8.0.0 remove all instances of MAX_OPEN_JOBS_NODE_ATTR from this file
public class NodeLoadDetectorTests extends ESTestCase {

    // To simplify the logic in this class all jobs have the same memory requirement
    private static final ByteSizeValue JOB_MEMORY_REQUIREMENT = ByteSizeValue.ofMb(10);

    private NodeLoadDetector nodeLoadDetector;

    @Before
    public void setup() {
        MlMemoryTracker memoryTracker = mock(MlMemoryTracker.class);
        when(memoryTracker.isRecentlyRefreshed()).thenReturn(true);
        when(memoryTracker.getAnomalyDetectorJobMemoryRequirement(anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
        when(memoryTracker.getDataFrameAnalyticsJobMemoryRequirement(anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
        when(memoryTracker.getJobMemoryRequirement(anyString(), anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
        nodeLoadDetector = new NodeLoadDetector(memoryTracker);
    }

    public void testNodeLoadDetection() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "10");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "-1");
        // MachineLearning.MACHINE_MEMORY_NODE_ATTR negative, so this will fall back to allocating by count
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name3", "_node_id3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name4", "_node_id4", new TransportAddress(InetAddress.getLoopbackAddress(), 9303),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id4", "_node_id4", JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        final ClusterState cs = ClusterState.builder(new ClusterName("_name")).nodes(nodes)
                .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasks)).build();

        NodeLoad load = nodeLoadDetector.detectNodeLoad(cs, true, nodes.get("_node_id1"), 10, 30, false);
        assertThat(load.getAssignedJobMemory(), equalTo(52428800L));
        assertThat(load.getNumAllocatingJobs(), equalTo(2L));
        assertThat(load.getNumAssignedJobs(), equalTo(2L));
        assertThat(load.getMaxJobs(), equalTo(10));
        assertThat(load.getMaxMlMemory(), equalTo(0L));

        load = nodeLoadDetector.detectNodeLoad(cs, true, nodes.get("_node_id2"), 5, 30, false);
        assertThat(load.getAssignedJobMemory(), equalTo(41943040L));
        assertThat(load.getNumAllocatingJobs(), equalTo(1L));
        assertThat(load.getNumAssignedJobs(), equalTo(1L));
        assertThat(load.getMaxJobs(), equalTo(5));
        assertThat(load.getMaxMlMemory(), equalTo(0L));

        load = nodeLoadDetector.detectNodeLoad(cs, true, nodes.get("_node_id3"), 5, 30, false);
        assertThat(load.getAssignedJobMemory(), equalTo(0L));
        assertThat(load.getNumAllocatingJobs(), equalTo(0L));
        assertThat(load.getNumAssignedJobs(), equalTo(0L));
        assertThat(load.getMaxJobs(), equalTo(5));
        assertThat(load.getMaxMlMemory(), equalTo(0L));

        load = nodeLoadDetector.detectNodeLoad(cs, true, nodes.get("_node_id4"), 5, 30, false);
        assertThat(load.getAssignedJobMemory(), equalTo(41943040L));
        assertThat(load.getNumAllocatingJobs(), equalTo(0L));
        assertThat(load.getNumAssignedJobs(), equalTo(1L));
        assertThat(load.getMaxJobs(), equalTo(5));
        assertThat(load.getMaxMlMemory(), equalTo(0L));
    }

    public void testNodeLoadDetection_withBadMaxOpenJobsAttribute() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "foo");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "-1");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        cs.nodes(nodes);
        Metadata.Builder metadata = Metadata.builder();
        cs.metadata(metadata);

        NodeLoad load = nodeLoadDetector.detectNodeLoad(cs.build(), false, nodes.get("_node_id1"), 10, 30, false);
        assertThat(load.getError(), containsString("ml.max_open_jobs attribute [foo] is not an integer"));
    }

    public void testNodeLoadDetection_withBadMachineMemoryAttribute() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "10");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "bar");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        cs.nodes(nodes);
        Metadata.Builder metadata = Metadata.builder();
        cs.metadata(metadata);

        NodeLoad load = nodeLoadDetector.detectNodeLoad(cs.build(), false, nodes.get("_node_id1"), 10, -1, false);
        assertThat(load.getError(), containsString("ml.machine_memory attribute [bar] is not a long"));
    }

}
