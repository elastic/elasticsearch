/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.TransportOpenJobAction;
import org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO: in 8.0.0 remove all instances of MAX_OPEN_JOBS_NODE_ATTR from this file
public class JobNodeSelectorTests extends ESTestCase {

    private MlMemoryTracker memoryTracker;
    private boolean isMemoryTrackerRecentlyRefreshed;

    @Before
    public void setup() {
        memoryTracker = mock(MlMemoryTracker.class);
        isMemoryTrackerRecentlyRefreshed = true;
        when(memoryTracker.isRecentlyRefreshed()).thenReturn(isMemoryTrackerRecentlyRefreshed);
    }

    public void testNodeNameAndVersion() {
        TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("unrelated", "attribute");
        DiscoveryNode node = new DiscoveryNode("_node_name1", "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_name1}{version=" + node.getVersion() + "}", JobNodeSelector.nodeNameAndVersion(node));
    }

    public void testNodeNameAndMlAttributes() {
        TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
        SortedMap<String, String> attributes = new TreeMap<>();
        attributes.put("unrelated", "attribute");
        DiscoveryNode node = new DiscoveryNode("_node_name1", "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_name1}", JobNodeSelector.nodeNameAndMlAttributes(node));

        attributes.put("ml.machine_memory", "5");
        node = new DiscoveryNode("_node_name1", "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_name1}{ml.machine_memory=5}", JobNodeSelector.nodeNameAndMlAttributes(node));

        node = new DiscoveryNode(null, "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_id1}{ml.machine_memory=5}", JobNodeSelector.nodeNameAndMlAttributes(node));

        attributes.put("node.ml", "true");
        node = new DiscoveryNode("_node_name1", "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_name1}{ml.machine_memory=5}{node.ml=true}", JobNodeSelector.nodeNameAndMlAttributes(node));
    }


    public void testSelectLeastLoadedMlNode_byCount() {
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
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        TransportOpenJobActionTests.addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        cs.nodes(nodes);
        MetaData.Builder metaData = MetaData.builder();
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job.Builder jobBuilder = buildJobBuilder("job_id4");
        jobBuilder.setJobVersion(Version.CURRENT);

        Job job = jobBuilder.build();
        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs.build(), job.getId(), MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertEquals("", result.getExplanation());
        assertEquals("_node_id3", result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_maxCapacity() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = randomIntBetween(1, 100);

        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, Integer.toString(maxRunningJobsPerNode));
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "1000000000");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        String[] jobIds = new String[numNodes * maxRunningJobsPerNode];
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "_node_id" + i;
            TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i);
            nodes.add(new DiscoveryNode("_node_name" + i, nodeId, address, nodeAttr, Collections.emptySet(), Version.CURRENT));
            for (int j = 0; j < maxRunningJobsPerNode; j++) {
                int id = j + (maxRunningJobsPerNode * i);
                jobIds[id] = "job_id" + id;
                TransportOpenJobActionTests.addJobTask(jobIds[id], nodeId, JobState.OPENED, tasksBuilder);
            }
        }
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id0", new ByteSizeValue(150, ByteSizeUnit.MB)).build(new Date());

        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs.build(), "job_id0", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result =
            jobNodeSelector.selectNode(maxRunningJobsPerNode, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertNull(result.getExecutorNode());
        assertTrue(result.getExplanation(), result.getExplanation().contains("because this node is full. Number of opened jobs ["
            + maxRunningJobsPerNode + "], xpack.ml.max_open_jobs [" + maxRunningJobsPerNode + "]"));
    }

    public void testSelectLeastLoadedMlNode_noMlNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        TransportOpenJobActionTests.addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id2", new ByteSizeValue(2, ByteSizeUnit.MB)).build(new Date());

        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs.build(), "job_id2", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result = jobNodeSelector.selectNode(20, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertTrue(result.getExplanation().contains("because this node isn't a ml node"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_maxConcurrentOpeningJobs() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "10");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "1000000000");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name3", "_node_id3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        TransportOpenJobActionTests.addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id4", "_node_id2", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id5", "_node_id3", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.nodes(nodes);
        MetaData.Builder metaData = MetaData.builder();
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        csBuilder.metaData(metaData);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id6", new ByteSizeValue(2, ByteSizeUnit.MB)).build(new Date());

        ClusterState cs = csBuilder.build();
        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs, "job_id6", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertEquals("_node_id3", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        TransportOpenJobActionTests.addJobTask("job_id6", "_node_id3", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        jobNodeSelector = new JobNodeSelector(cs, "job_id7", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertNull("no node selected, because OPENING state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        tasksBuilder.reassignTask(MlTasks.jobTaskId("job_id6"),
            new PersistentTasksCustomMetaData.Assignment("_node_id3", "test assignment"));
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        jobNodeSelector = new JobNodeSelector(cs, "job_id7", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertNull("no node selected, because stale task", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        tasksBuilder.updateTaskState(MlTasks.jobTaskId("job_id6"), null);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        jobNodeSelector = new JobNodeSelector(cs, "job_id7", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertNull("no node selected, because null state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));
    }

    public void testSelectLeastLoadedMlNode_concurrentOpeningJobsAndStaleFailedJob() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "10");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "1000000000");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name3", "_node_id3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        TransportOpenJobActionTests.addJobTask("job_id1", "_node_id1", JobState.fromString("failed"), tasksBuilder);
        // This will make the allocation stale for job_id1
        tasksBuilder.reassignTask(MlTasks.jobTaskId("job_id1"),
            new PersistentTasksCustomMetaData.Assignment("_node_id1", "test assignment"));
        TransportOpenJobActionTests.addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id4", "_node_id2", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id5", "_node_id3", null, tasksBuilder);
        TransportOpenJobActionTests.addJobTask("job_id6", "_node_id3", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.nodes(nodes);
        MetaData.Builder metaData = MetaData.builder();
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id7", new ByteSizeValue(2, ByteSizeUnit.MB)).build(new Date());

        // Allocation won't be possible if the stale failed job is treated as opening
        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs, "job_id7", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertEquals("_node_id1", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        TransportOpenJobActionTests.addJobTask("job_id7", "_node_id1", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        jobNodeSelector = new JobNodeSelector(cs, "job_id8", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertNull("no node selected, because OPENING state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));
    }

    public void testSelectLeastLoadedMlNode_noCompatibleJobTypeNodes() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "10");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "1000000000");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        TransportOpenJobActionTests.addJobTask("incompatible_type_job", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();

        Job job = mock(Job.class);
        when(job.getId()).thenReturn("incompatible_type_job");
        when(job.getJobVersion()).thenReturn(Version.CURRENT);
        when(job.getJobType()).thenReturn("incompatible_type");
        when(job.getInitialResultsIndexName()).thenReturn("shared");

        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs.build(), "incompatible_type_job", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertThat(result.getExplanation(), containsString("because this node does not support jobs of type [incompatible_type]"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_noNodesMatchingModelSnapshotMinVersion() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "10");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "1000000000");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.V_6_2_0))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                nodeAttr, Collections.emptySet(), Version.V_6_1_0))
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        TransportOpenJobActionTests.addJobTask("job_with_incompatible_model_snapshot", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_with_incompatible_model_snapshot")
            .setModelSnapshotId("incompatible_snapshot")
            .setModelSnapshotMinVersion(Version.V_6_3_0)
            .build(new Date());
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs.build(), "job_with_incompatible_model_snapshot",
            MlTasks.JOB_TASK_NAME, memoryTracker, node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertThat(result.getExplanation(), containsString(
            "because the job's model snapshot requires a node of version [6.3.0] or higher"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_jobWithRulesButNoNodeMeetsRequiredVersion() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "10");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "1000000000");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.V_6_2_0))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                nodeAttr, Collections.emptySet(), Version.V_6_3_0))
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        TransportOpenJobActionTests.addJobTask("job_with_rules", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = TransportOpenJobActionTests.jobWithRules("job_with_rules");
        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs.build(), "job_with_rules", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertThat(result.getExplanation(), containsString(
            "because jobs using custom_rules require a node of version [6.4.0] or higher"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_jobWithRulesAndNodeMeetsRequiredVersion() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "10");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "1000000000");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.V_6_2_0))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                nodeAttr, Collections.emptySet(), Version.V_6_4_0))
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        TransportOpenJobActionTests.addJobTask("job_with_rules", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = TransportOpenJobActionTests.jobWithRules("job_with_rules");
        JobNodeSelector jobNodeSelector = new JobNodeSelector(cs.build(), "job_with_rules", MlTasks.JOB_TASK_NAME, memoryTracker,
            node -> TransportOpenJobAction.nodeFilter(node, job));
        PersistentTasksCustomMetaData.Assignment result = jobNodeSelector.selectNode(10, 2, 30, isMemoryTrackerRecentlyRefreshed);
        assertNotNull(result.getExecutorNode());
    }
}
