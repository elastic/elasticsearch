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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction.TaskParams;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.autoscaling.NativeMemoryCapacity;
import org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutor.nodeFilter;
import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests.jobWithRules;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobNodeSelectorTests extends ESTestCase {

    // To simplify the logic in this class all jobs have the same memory requirement
    private static final long MAX_JOB_BYTES = ByteSizeValue.ofGb(1).getBytes();
    private static final ByteSizeValue JOB_MEMORY_REQUIREMENT = ByteSizeValue.ofMb(10);
    private static final Set<DiscoveryNodeRole> ROLES_WITH_ML = Set.of(
        DiscoveryNodeRole.MASTER_ROLE,
        DiscoveryNodeRole.ML_ROLE,
        DiscoveryNodeRole.DATA_ROLE
    );
    private static final Set<DiscoveryNodeRole> ROLES_WITHOUT_ML = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE);

    private MlMemoryTracker memoryTracker;

    @Before
    public void setup() {
        memoryTracker = mock(MlMemoryTracker.class);
        when(memoryTracker.isRecentlyRefreshed()).thenReturn(true, false);
        when(memoryTracker.getAnomalyDetectorJobMemoryRequirement(anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
        when(memoryTracker.getDataFrameAnalyticsJobMemoryRequirement(anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
        when(memoryTracker.getJobMemoryRequirement(anyString(), anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
    }

    public void testNodeNameAndVersion() {
        TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("unrelated", "attribute");
        DiscoveryNode node = DiscoveryNodeUtils.create("_node_name1", "_node_id1", ta, attributes, ROLES_WITHOUT_ML);
        assertEquals("{_node_name1}{version=" + node.getVersion() + "}", JobNodeSelector.nodeNameAndVersion(node));
    }

    public void testNodeNameAndMlAttributes() {
        TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
        SortedMap<String, String> attributes = new TreeMap<>();
        attributes.put("unrelated", "attribute");
        DiscoveryNode node = DiscoveryNodeUtils.create("_node_name1", "_node_id1", ta, attributes, ROLES_WITHOUT_ML);
        assertEquals("{_node_name1}", JobNodeSelector.nodeNameAndMlAttributes(node));

        attributes.put("ml.machine_memory", "5");
        node = DiscoveryNodeUtils.create("_node_name1", "_node_id1", ta, attributes, ROLES_WITH_ML);
        assertEquals("{_node_name1}{ml.machine_memory=5}", JobNodeSelector.nodeNameAndMlAttributes(node));

        node = DiscoveryNodeUtils.create(null, "_node_id1", ta, attributes, ROLES_WITH_ML);
        assertEquals("{_node_id1}{ml.machine_memory=5}", JobNodeSelector.nodeNameAndMlAttributes(node));
    }

    public void testSelectLeastLoadedMlNodeForAnomalyDetectorJob_maxCapacityCountLimiting() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = randomIntBetween(1, 100);
        int maxMachineMemoryPercent = 30;
        long machineMemory = (maxRunningJobsPerNode + 1) * JOB_MEMORY_REQUIREMENT.getBytes() * 100 / maxMachineMemoryPercent;

        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(machineMemory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(machineMemory / 2)
        );

        ClusterState.Builder cs = fillNodesWithRunningJobs(nodeAttr, numNodes, maxRunningJobsPerNode);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id1000", JOB_MEMORY_REQUIREMENT).build(new Date());

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(
            maxRunningJobsPerNode,
            2,
            maxMachineMemoryPercent,
            MAX_JOB_BYTES,
            false
        );
        assertNull(result.getExecutorNode());
        assertThat(
            result.getExplanation(),
            containsString(
                "node is full. Number of opened jobs and allocated native inference processes ["
                    + maxRunningJobsPerNode
                    + "], xpack.ml.max_open_jobs ["
                    + maxRunningJobsPerNode
                    + "]"
            )
        );
    }

    public void testSelectLeastLoadedMlNodeForDataFrameAnalyticsJob_maxCapacityCountLimiting() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = randomIntBetween(1, 100);
        int maxMachineMemoryPercent = 30;
        long machineMemory = (maxRunningJobsPerNode + 1) * JOB_MEMORY_REQUIREMENT.getBytes() * 100 / maxMachineMemoryPercent;

        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(machineMemory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(machineMemory / 2)
        );

        ClusterState.Builder cs = fillNodesWithRunningJobs(nodeAttr, numNodes, maxRunningJobsPerNode);

        String dataFrameAnalyticsId = "data_frame_analytics_id1000";

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            dataFrameAnalyticsId,
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            memoryTracker,
            0,
            node -> TransportStartDataFrameAnalyticsAction.TaskExecutor.nodeFilter(node, createTaskParams(dataFrameAnalyticsId))
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(
            maxRunningJobsPerNode,
            2,
            maxMachineMemoryPercent,
            MAX_JOB_BYTES,
            false
        );
        assertNull(result.getExecutorNode());
        assertThat(
            result.getExplanation(),
            containsString(
                "node is full. Number of opened jobs and allocated native inference processes ["
                    + maxRunningJobsPerNode
                    + "], xpack.ml.max_open_jobs ["
                    + maxRunningJobsPerNode
                    + "]"
            )
        );
    }

    public void testSelectLeastLoadedMlNodeForAnomalyDetectorJob_maxCapacityMemoryLimiting() {
        int numNodes = randomIntBetween(1, 10);
        int currentlyRunningJobsPerNode = randomIntBetween(1, 100);
        int maxRunningJobsPerNode = currentlyRunningJobsPerNode + 1;
        // Be careful if changing this - in order for the error message to be exactly as expected
        // the value here must divide exactly into both (JOB_MEMORY_REQUIREMENT.getBytes() * 100) and
        // MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
        int maxMachineMemoryPercent = 20;
        long currentlyRunningJobMemory = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes() + currentlyRunningJobsPerNode
            * JOB_MEMORY_REQUIREMENT.getBytes();
        long machineMemory = currentlyRunningJobMemory * 100 / maxMachineMemoryPercent;

        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(machineMemory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(machineMemory / 2)
        );

        ClusterState.Builder cs = fillNodesWithRunningJobs(nodeAttr, numNodes, currentlyRunningJobsPerNode);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id1000", JOB_MEMORY_REQUIREMENT).build(new Date());

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(
            maxRunningJobsPerNode,
            2,
            maxMachineMemoryPercent,
            MAX_JOB_BYTES,
            false
        );
        assertNull(result.getExecutorNode());
        assertThat(
            result.getExplanation(),
            containsString(
                "node has insufficient available memory. "
                    + "Available memory for ML ["
                    + currentlyRunningJobMemory
                    + " ("
                    + ByteSizeValue.ofBytes(currentlyRunningJobMemory)
                    + ")], memory required by existing jobs ["
                    + currentlyRunningJobMemory
                    + " ("
                    + ByteSizeValue.ofBytes(currentlyRunningJobMemory)
                    + ")], estimated memory required for this job ["
                    + JOB_MEMORY_REQUIREMENT.getBytes()
                    + " ("
                    + ByteSizeValue.ofBytes(JOB_MEMORY_REQUIREMENT.getBytes())
                    + ")]"
            )
        );
    }

    public void testSelectLeastLoadedMlNodeForDataFrameAnalyticsJob_givenTaskHasNullState() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = 10;
        int maxMachineMemoryPercent = 30;

        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            String.valueOf(ByteSizeValue.ofGb(1).getBytes()),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            String.valueOf(ByteSizeValue.ofMb(400).getBytes())
        );

        ClusterState.Builder cs = fillNodesWithRunningJobs(nodeAttr, numNodes, 1, JobState.OPENED, null);

        String dataFrameAnalyticsId = "data_frame_analytics_id_new";

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            dataFrameAnalyticsId,
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            memoryTracker,
            0,
            node -> TransportStartDataFrameAnalyticsAction.TaskExecutor.nodeFilter(node, createTaskParams(dataFrameAnalyticsId))
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(
            maxRunningJobsPerNode,
            2,
            maxMachineMemoryPercent,
            MAX_JOB_BYTES,
            false
        );
        assertNotNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNodeForAnomalyDetectorJob_firstJobTooBigMemoryLimiting() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = randomIntBetween(1, 100);
        int maxMachineMemoryPercent = 20;
        long firstJobTotalMemory = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes() + JOB_MEMORY_REQUIREMENT.getBytes();
        long machineMemory = (firstJobTotalMemory - 1) * 100 / maxMachineMemoryPercent;

        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(machineMemory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(machineMemory / 2)
        );

        ClusterState.Builder cs = fillNodesWithRunningJobs(nodeAttr, numNodes, 0);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id1000", JOB_MEMORY_REQUIREMENT).build(new Date());

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(
            maxRunningJobsPerNode,
            2,
            maxMachineMemoryPercent,
            MAX_JOB_BYTES,
            false
        );
        assertNull(result.getExecutorNode());
        assertThat(
            result.getExplanation(),
            containsString(
                "node has insufficient available memory. "
                    + "Available memory for ML ["
                    + (firstJobTotalMemory - 1)
                    + " ("
                    + ByteSizeValue.ofBytes((firstJobTotalMemory - 1))
                    + ")], memory required by existing jobs [0 (0b)], estimated memory required for this job ["
                    + firstJobTotalMemory
                    + " ("
                    + ByteSizeValue.ofBytes(firstJobTotalMemory)
                    + ")]"
            )
        );
    }

    public void testSelectLeastLoadedMlNodeForDataFrameAnalyticsJob_maxCapacityMemoryLimiting() {
        int numNodes = randomIntBetween(1, 10);
        int currentlyRunningJobsPerNode = randomIntBetween(1, 100);
        int maxRunningJobsPerNode = currentlyRunningJobsPerNode + 1;
        // Be careful if changing this - in order for the error message to be exactly as expected
        // the value here must divide exactly into both (JOB_MEMORY_REQUIREMENT.getBytes() * 100) and
        // MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
        int maxMachineMemoryPercent = 20;
        long currentlyRunningJobMemory = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes() + currentlyRunningJobsPerNode
            * JOB_MEMORY_REQUIREMENT.getBytes();
        long machineMemory = currentlyRunningJobMemory * 100 / maxMachineMemoryPercent;

        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(machineMemory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(machineMemory / 2)
        );

        ClusterState.Builder cs = fillNodesWithRunningJobs(nodeAttr, numNodes, currentlyRunningJobsPerNode);

        String dataFrameAnalyticsId = "data_frame_analytics_id1000";

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            dataFrameAnalyticsId,
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            memoryTracker,
            0,
            node -> TransportStartDataFrameAnalyticsAction.TaskExecutor.nodeFilter(node, createTaskParams(dataFrameAnalyticsId))
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(
            maxRunningJobsPerNode,
            2,
            maxMachineMemoryPercent,
            MAX_JOB_BYTES,
            false
        );
        assertNull(result.getExecutorNode());
        assertThat(
            result.getExplanation(),
            containsString(
                "node has insufficient available memory. "
                    + "Available memory for ML ["
                    + currentlyRunningJobMemory
                    + " ("
                    + ByteSizeValue.ofBytes(currentlyRunningJobMemory)
                    + ")], memory required by existing jobs ["
                    + currentlyRunningJobMemory
                    + " ("
                    + ByteSizeValue.ofBytes(currentlyRunningJobMemory)
                    + ")], estimated memory required for this job ["
                    + JOB_MEMORY_REQUIREMENT.getBytes()
                    + " ("
                    + ByteSizeValue.ofBytes(JOB_MEMORY_REQUIREMENT.getBytes())
                    + ")]"
            )
        );
    }

    public void testSelectLeastLoadedMlNodeForDataFrameAnalyticsJob_firstJobTooBigMemoryLimiting() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = randomIntBetween(1, 100);
        int maxMachineMemoryPercent = 20;
        long firstJobTotalMemory = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes() + JOB_MEMORY_REQUIREMENT.getBytes();
        long machineMemory = (firstJobTotalMemory - 1) * 100 / maxMachineMemoryPercent;

        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(machineMemory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(machineMemory / 2)
        );

        ClusterState.Builder cs = fillNodesWithRunningJobs(nodeAttr, numNodes, 0);

        String dataFrameAnalyticsId = "data_frame_analytics_id1000";

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            dataFrameAnalyticsId,
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            memoryTracker,
            0,
            node -> TransportStartDataFrameAnalyticsAction.TaskExecutor.nodeFilter(node, createTaskParams(dataFrameAnalyticsId))
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(
            maxRunningJobsPerNode,
            2,
            maxMachineMemoryPercent,
            MAX_JOB_BYTES,
            false
        );
        assertNull(result.getExecutorNode());
        assertThat(
            result.getExplanation(),
            containsString(
                "node has insufficient available memory. "
                    + "Available memory for ML ["
                    + (firstJobTotalMemory - 1)
                    + " ("
                    + ByteSizeValue.ofBytes(firstJobTotalMemory - 1)
                    + ")], memory required by existing jobs [0 (0b)], estimated memory required for this job ["
                    + firstJobTotalMemory
                    + " ("
                    + ByteSizeValue.ofBytes(firstJobTotalMemory)
                    + ")]"
            )
        );
    }

    public void testSelectLeastLoadedMlNode_noMlNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Collections.emptyMap(),
                    ROLES_WITHOUT_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    Collections.emptyMap(),
                    ROLES_WITHOUT_ML
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();
        cs.nodes(nodes);
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id2", JOB_MEMORY_REQUIREMENT).build(new Date());

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(20, 2, 30, MAX_JOB_BYTES, false);
        assertTrue(result.getExplanation().contains("node isn't a machine learning node"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_maxConcurrentOpeningJobs() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000"
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name3",
                    "_node_id3",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id4", "_node_id2", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id5", "_node_id3", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.nodes(nodes);
        Metadata.Builder metadata = Metadata.builder();
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        csBuilder.metadata(metadata);

        Job job6 = BaseMlIntegTestCase.createFareQuoteJob("job_id6", JOB_MEMORY_REQUIREMENT).build(new Date());

        ClusterState cs = csBuilder.build();
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs,
            shuffled(cs.nodes().getAllNodes()),
            job6.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job6)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertEquals("_node_id3", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetadata.builder(tasks);
        OpenJobPersistentTasksExecutorTests.addJobTask(job6.getId(), "_node_id3", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metadata(Metadata.builder(cs.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks));
        cs = csBuilder.build();

        Job job7 = BaseMlIntegTestCase.createFareQuoteJob("job_id7", JOB_MEMORY_REQUIREMENT).build(new Date());
        jobNodeSelector = new JobNodeSelector(
            cs,
            shuffled(cs.nodes().getAllNodes()),
            job7.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job7)
        );
        result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertNull("no node selected, because OPENING state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("Node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetadata.builder(tasks);
        tasksBuilder.reassignTask(
            MlTasks.jobTaskId(job6.getId()),
            new PersistentTasksCustomMetadata.Assignment("_node_id3", "test assignment")
        );
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metadata(Metadata.builder(cs.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks));
        cs = csBuilder.build();
        jobNodeSelector = new JobNodeSelector(
            cs,
            shuffled(cs.nodes().getAllNodes()),
            job7.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job7)
        );
        result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertNull("no node selected, because stale task", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("Node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetadata.builder(tasks);
        tasksBuilder.updateTaskState(MlTasks.jobTaskId(job6.getId()), null);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metadata(Metadata.builder(cs.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks));
        cs = csBuilder.build();
        jobNodeSelector = new JobNodeSelector(
            cs,
            shuffled(cs.nodes().getAllNodes()),
            job7.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job7)
        );
        result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertNull("no node selected, because null state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("Node exceeds [2] the maximum number of jobs [2] in opening state"));
    }

    public void testSelectLeastLoadedMlNode_concurrentOpeningJobsAndStaleFailedJob() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000"
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name3",
                    "_node_id3",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id1", "_node_id1", JobState.fromString("failed"), tasksBuilder);
        // This will make the assignment stale for job_id1
        tasksBuilder.reassignTask(
            MlTasks.jobTaskId("job_id1"),
            new PersistentTasksCustomMetadata.Assignment("_node_id1", "test assignment")
        );
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id4", "_node_id2", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id5", "_node_id3", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id6", "_node_id3", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.nodes(nodes);
        Metadata.Builder metadata = Metadata.builder();
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        csBuilder.metadata(metadata);

        ClusterState cs = csBuilder.build();
        Job job7 = BaseMlIntegTestCase.createFareQuoteJob("job_id7", JOB_MEMORY_REQUIREMENT).build(new Date());

        // Assignment won't be possible if the stale failed job is treated as opening
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs,
            shuffled(cs.nodes().getAllNodes()),
            job7.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job7)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertEquals("_node_id1", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetadata.builder(tasks);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id7", "_node_id1", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metadata(Metadata.builder(cs.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks));
        cs = csBuilder.build();
        Job job8 = BaseMlIntegTestCase.createFareQuoteJob("job_id8", JOB_MEMORY_REQUIREMENT).build(new Date());
        jobNodeSelector = new JobNodeSelector(
            cs,
            shuffled(cs.nodes().getAllNodes()),
            job8.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job8)
        );
        result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertNull("no node selected, because OPENING state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("Node exceeds [2] the maximum number of jobs [2] in opening state"));
    }

    public void testSelectLeastLoadedMlNode_noCompatibleJobTypeNodes() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000"
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("incompatible_type_job", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();

        Job job = mock(Job.class);
        when(job.getId()).thenReturn("incompatible_type_job");
        when(job.getJobVersion()).thenReturn(Version.CURRENT);
        when(job.getJobType()).thenReturn("incompatible_type");
        when(job.getInitialResultsIndexName()).thenReturn("shared");

        cs.nodes(nodes);
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertThat(result.getExplanation(), containsString("node does not support jobs of type [incompatible_type]"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_reasonsAreInDeterministicOrder() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000"
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("incompatible_type_job", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();

        Job job = mock(Job.class);
        when(job.getId()).thenReturn("incompatible_type_job");
        when(job.getJobVersion()).thenReturn(Version.CURRENT);
        when(job.getJobType()).thenReturn("incompatible_type");
        when(job.getInitialResultsIndexName()).thenReturn("shared");

        cs.nodes(nodes);
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertThat(
            result.getExplanation(),
            equalTo(
                "Not opening job [incompatible_type_job] on node [{_node_name1}{version="
                    + Version.CURRENT
                    + "}], "
                    + "because this node does not support jobs of type [incompatible_type]|"
                    + "Not opening job [incompatible_type_job] on node [{_node_name2}{version="
                    + Version.CURRENT
                    + "}], "
                    + "because this node does not support jobs of type [incompatible_type]"
            )
        );
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_noNodesMatchingModelSnapshotMinVersion() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000"
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                new DiscoveryNode(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    ROLES_WITH_ML,
                    Version.fromString("6.2.0")
                )
            )
            .add(
                new DiscoveryNode(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    ROLES_WITH_ML,
                    Version.fromString("6.1.0")
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("job_with_incompatible_model_snapshot", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_with_incompatible_model_snapshot")
            .setModelSnapshotId("incompatible_snapshot")
            .setModelSnapshotMinVersion(Version.fromString("6.3.0"))
            .build(new Date());
        cs.nodes(nodes);
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertThat(result.getExplanation(), containsString("job's model snapshot requires a node of version [6.3.0] or higher"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_jobWithRules() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000"
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                new DiscoveryNode(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    ROLES_WITH_ML,
                    Version.fromString("6.2.0")
                )
            )
            .add(
                new DiscoveryNode(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    ROLES_WITH_ML,
                    Version.fromString("6.4.0")
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("job_with_rules", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();
        cs.nodes(nodes);
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);

        Job job = jobWithRules("job_with_rules");
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertNotNull(result.getExecutorNode());
    }

    public void testSelectMlNodeOnlyOutOfCandidates() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000"
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    ROLES_WITH_ML
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("job_with_rules", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();
        cs.nodes(nodes);
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);

        DiscoveryNode candidate = nodes.getNodes().get(randomBoolean() ? "_node_id1" : "_node_id2");

        Job job = jobWithRules("job_with_rules");
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            Collections.singletonList(candidate),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(10, 2, 30, MAX_JOB_BYTES, false);
        assertNotNull(result.getExecutorNode());
        assertThat(result.getExecutorNode(), equalTo(candidate.getId()));
    }

    public void testConsiderLazyAssignmentWithNoLazyNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Collections.emptyMap(),
                    ROLES_WITHOUT_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    Collections.emptyMap(),
                    ROLES_WITHOUT_ML
                )
            )
            .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        cs.nodes(nodes);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id1000", JOB_MEMORY_REQUIREMENT).build(new Date());
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.considerLazyAssignment(
            new PersistentTasksCustomMetadata.Assignment(null, "foo"),
            ByteSizeValue.ofGb(1).getBytes()
        );
        assertEquals("foo", result.getExplanation());
        assertNull(result.getExecutorNode());
    }

    public void testConsiderLazyAssignmentWithLazyNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Collections.emptyMap(),
                    ROLES_WITHOUT_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    Collections.emptyMap(),
                    ROLES_WITHOUT_ML
                )
            )
            .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        cs.nodes(nodes);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id1000", JOB_MEMORY_REQUIREMENT).build(new Date());
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            randomIntBetween(1, 3),
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.considerLazyAssignment(
            new PersistentTasksCustomMetadata.Assignment(null, "foo"),
            ByteSizeValue.ofGb(1).getBytes()
        );
        assertEquals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT.getExplanation(), result.getExplanation());
        assertNull(result.getExecutorNode());
    }

    public void testConsiderLazyAssignmentWithFilledLazyNodesAndVerticalScale() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Map.of(
                        MachineLearning.MACHINE_MEMORY_NODE_ATTR,
                        Long.toString(ByteSizeValue.ofGb(1).getBytes()),
                        MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
                        Long.toString(ByteSizeValue.ofMb(400).getBytes())
                    ),
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    Map.of(
                        MachineLearning.MACHINE_MEMORY_NODE_ATTR,
                        Long.toString(ByteSizeValue.ofGb(1).getBytes()),
                        MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
                        Long.toString(ByteSizeValue.ofMb(400).getBytes())
                    ),
                    ROLES_WITH_ML
                )
            )
            .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        cs.nodes(nodes);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id1000", JOB_MEMORY_REQUIREMENT).build(new Date());
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            randomIntBetween(1, 3),
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.considerLazyAssignment(
            new PersistentTasksCustomMetadata.Assignment(null, "foo"),
            ByteSizeValue.ofGb(64).getBytes()
        );
        assertEquals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT.getExplanation(), result.getExplanation());
        assertNull(result.getExecutorNode());
    }

    public void testMaximumPossibleNodeMemoryTooSmall() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = randomIntBetween(1, 100);
        int maxMachineMemoryPercent = 30;
        long machineMemory = (maxRunningJobsPerNode + 1) * JOB_MEMORY_REQUIREMENT.getBytes() * 100 / maxMachineMemoryPercent;

        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(machineMemory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(machineMemory / 2)
        );

        ClusterState.Builder cs = fillNodesWithRunningJobs(nodeAttr, numNodes, maxRunningJobsPerNode);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id1000", ByteSizeValue.ofMb(10)).build(new Date());
        when(memoryTracker.getJobMemoryRequirement(anyString(), eq("job_id1000"))).thenReturn(1000L);

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            randomIntBetween(1, 3),
            node -> nodeFilter(node, job)
        );
        PersistentTasksCustomMetadata.Assignment result = jobNodeSelector.selectNode(
            maxRunningJobsPerNode,
            2,
            maxMachineMemoryPercent,
            10L,
            false
        );
        assertNull(result.getExecutorNode());
        assertThat(
            result.getExplanation(),
            containsString(
                "[job_id1000] not waiting for node assignment as estimated job size "
                    + "[31458280] is greater than largest possible job size [3]"
            )
        );
    }

    public void testPerceivedCapacityAndMaxFreeMemory() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "not_ml_node_name",
                    "_node_id",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Collections.emptyMap(),
                    ROLES_WITHOUT_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "filled_ml_node_name",
                    "filled_ml_node_id",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    MapBuilder.<String, String>newMapBuilder()
                        .put(MachineLearning.MAX_JVM_SIZE_NODE_ATTR, "10")
                        .put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, Long.toString(ByteSizeValue.ofGb(30).getBytes()))
                        .map(),
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "not_filled_ml_node",
                    "not_filled_ml_node_id",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                    MapBuilder.<String, String>newMapBuilder()
                        .put(MachineLearning.MAX_JVM_SIZE_NODE_ATTR, "10")
                        .put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, Long.toString(ByteSizeValue.ofGb(30).getBytes()))
                        .map(),
                    ROLES_WITH_ML
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "not_filled_smaller_ml_node",
                    "not_filled_smaller_ml_node_id",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9303),
                    MapBuilder.<String, String>newMapBuilder()
                        .put(MachineLearning.MAX_JVM_SIZE_NODE_ATTR, "10")
                        .put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, Long.toString(ByteSizeValue.ofGb(10).getBytes()))
                        .map(),
                    ROLES_WITH_ML
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("one_job", "filled_ml_node_id", null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();
        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();
        cs.nodes(nodes);
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id2", JOB_MEMORY_REQUIREMENT).build(new Date());

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            cs.build(),
            shuffled(cs.nodes().getAllNodes()),
            job.getId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> nodeFilter(node, job)
        );
        Tuple<NativeMemoryCapacity, Long> capacityAndFreeMemory = jobNodeSelector.currentCapacityAndMaxFreeMemory(10, false, 1);
        assertThat(capacityAndFreeMemory.v2(), equalTo(ByteSizeValue.ofGb(3).getBytes()));
        // NativeMemoryCapacity holds memory excluding the per-node overhead for native code, so this must be subtracted once per node
        assertThat(
            capacityAndFreeMemory.v1(),
            equalTo(
                new NativeMemoryCapacity(
                    ByteSizeValue.ofGb(7).getBytes() - 3 * MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                    ByteSizeValue.ofGb(3).getBytes() - MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                    10L
                )
            )
        );
    }

    private ClusterState.Builder fillNodesWithRunningJobs(Map<String, String> nodeAttr, int numNodes, int numRunningJobsPerNode) {

        return fillNodesWithRunningJobs(nodeAttr, numNodes, numRunningJobsPerNode, JobState.OPENED, DataFrameAnalyticsState.STARTED);
    }

    private ClusterState.Builder fillNodesWithRunningJobs(
        Map<String, String> nodeAttr,
        int numNodes,
        int numRunningJobsPerNode,
        JobState anomalyDetectionJobState,
        DataFrameAnalyticsState dfAnalyticsJobState
    ) {

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        String[] jobIds = new String[numNodes * numRunningJobsPerNode];
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "_node_id" + i;
            TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i);
            nodes.add(DiscoveryNodeUtils.create("_node_name" + i, nodeId, address, nodeAttr, ROLES_WITH_ML));
            for (int j = 0; j < numRunningJobsPerNode; j++) {
                int id = j + (numRunningJobsPerNode * i);
                // Both anomaly detector jobs and data frame analytics jobs should count towards the limit
                if (randomBoolean()) {
                    jobIds[id] = "job_id" + id;
                    OpenJobPersistentTasksExecutorTests.addJobTask(jobIds[id], nodeId, anomalyDetectionJobState, tasksBuilder);
                } else {
                    jobIds[id] = "data_frame_analytics_id" + id;
                    addDataFrameAnalyticsJobTask(jobIds[id], nodeId, dfAnalyticsJobState, tasksBuilder);
                }
            }
        }
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();
        cs.nodes(nodes);
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);

        return cs;
    }

    static Collection<DiscoveryNode> shuffled(Collection<DiscoveryNode> nodes) {
        List<DiscoveryNode> toShuffle = new ArrayList<>(nodes);
        Randomness.shuffle(toShuffle);
        return toShuffle;
    }

    static void addDataFrameAnalyticsJobTask(
        String id,
        String nodeId,
        DataFrameAnalyticsState state,
        PersistentTasksCustomMetadata.Builder builder
    ) {
        addDataFrameAnalyticsJobTask(id, nodeId, state, builder, false, false);
    }

    static void addDataFrameAnalyticsJobTask(
        String id,
        String nodeId,
        DataFrameAnalyticsState state,
        PersistentTasksCustomMetadata.Builder builder,
        boolean isStale,
        boolean allowLazyStart
    ) {
        builder.addTask(
            MlTasks.dataFrameAnalyticsTaskId(id),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams(id, Version.CURRENT, allowLazyStart),
            new PersistentTasksCustomMetadata.Assignment(nodeId, "test assignment")
        );
        if (state != null) {
            builder.updateTaskState(
                MlTasks.dataFrameAnalyticsTaskId(id),
                new DataFrameAnalyticsTaskState(state, builder.getLastAllocationId() - (isStale ? 1 : 0), null)
            );
        }
    }

    private static TaskParams createTaskParams(String id) {
        return new TaskParams(id, Version.CURRENT, false);
    }
}
