/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.notifications.AuditorField;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportOpenJobActionTests extends ESTestCase {

    public void testValidate_jobMissing() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id1").build(), false);
        expectThrows(ResourceNotFoundException.class, () -> TransportOpenJobAction.validate("job_id2", mlBuilder.build()));
    }

    public void testValidate_jobMarkedAsDeleting() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        jobBuilder.setDeleting(true);
        mlBuilder.putJob(jobBuilder.build(), false);
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> TransportOpenJobAction.validate("job_id", mlBuilder.build()));
        assertEquals("Cannot open job [job_id] because it is being deleted", e.getMessage());
    }

    public void testValidate_jobWithoutVersion() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        mlBuilder.putJob(jobBuilder.build(), false);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> TransportOpenJobAction.validate("job_id", mlBuilder.build()));
        assertEquals("Cannot open job [job_id] because jobs created prior to version 5.5 are not supported", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }

    public void testValidate_givenValidJob() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        mlBuilder.putJob(jobBuilder.build(new Date()), false);
        TransportOpenJobAction.validate("job_id", mlBuilder.build());
    }

    public void testSelectLeastLoadedMlNode_byCount() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        // MachineLearning.MACHINE_MEMORY_NODE_ATTR not set, so this will fall back to allocating by count
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .add(new DiscoveryNode("_node_name3", "_node_id3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "job_id1", "job_id2", "job_id3", "job_id4");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id4", cs.build(), 2, 10, 30, logger);
        assertEquals("", result.getExplanation());
        assertEquals("_node_id3", result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_byMemory() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        nodeAttr.put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, "16000000000");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .add(new DiscoveryNode("_node_name3", "_node_id3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id1", "_node_id1", JobState.fromString("opened"), tasksBuilder);
        addJobTask("job_id2", "_node_id2", JobState.fromString("opened"), tasksBuilder);
        addJobTask("job_id3", "_node_id2", JobState.fromString("opened"), tasksBuilder);
        addJobTask("job_id4", "_node_id3", JobState.fromString("opened"), tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, jobId -> {
            // remember we add 100MB for the process overhead, so these model memory
            // limits correspond to estimated footprints of 102MB and 205MB
            long jobSize = (jobId.equals("job_id2") || jobId.equals("job_id3")) ? 2 : 105;
            return BaseMlIntegTestCase.createFareQuoteJob(jobId, new ByteSizeValue(jobSize, ByteSizeUnit.MB)).build(new Date());
        }, "job_id1", "job_id2", "job_id3", "job_id4", "job_id5");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id5", cs.build(), 2, 10, 30, logger);
        assertEquals("", result.getExplanation());
        assertEquals("_node_id2", result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_byMemoryWithFailedJobs() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        // this leaves just under 300MB per node available for ML jobs
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
        addJobTask("job_id1", "_node_id1", JobState.fromString("failed"), tasksBuilder);
        addJobTask("job_id2", "_node_id2", JobState.fromString("failed"), tasksBuilder);
        addJobTask("job_id3", "_node_id3", JobState.fromString("failed"), tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, jobId -> {
            // remember we add 100MB for the process overhead, so this model
            // memory limit corresponds to a job size of 250MB
            return BaseMlIntegTestCase.createFareQuoteJob(jobId, new ByteSizeValue(150, ByteSizeUnit.MB)).build(new Date());
        }, "job_id1", "job_id2", "job_id3", "job_id4");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        // if the memory of the failed jobs is wrongly included in the calculation then this job will not be allocated
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id4", cs.build(), 2, 10, 30, logger);
        assertEquals("", result.getExplanation());
        assertNotNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_maxCapacity() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = randomIntBetween(1, 100);

        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
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
                addJobTask(jobIds[id], nodeId, JobState.OPENED, tasksBuilder);
            }
        }
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, jobIds);
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id0", cs.build(), 2, maxRunningJobsPerNode, 30, logger);
        assertNull(result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because this node is full. Number of opened jobs [" + maxRunningJobsPerNode
                + "], xpack.ml.max_open_jobs [" + maxRunningJobsPerNode + "]"));
    }

    public void testSelectLeastLoadedMlNode_noMlNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "job_id1", "job_id2");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id2", cs.build(), 2, 10, 30, logger);
        assertTrue(result.getExplanation().contains("because this node isn't a ml node"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_maxConcurrentOpeningJobs() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .add(new DiscoveryNode("_node_name3", "_node_id3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        addJobTask("job_id4", "_node_id2", null, tasksBuilder);
        addJobTask("job_id5", "_node_id3", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.nodes(nodes);
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "job_id1", "job_id2", "job_id3", "job_id4", "job_id5", "job_id6", "job_id7");
        csBuilder.routingTable(routingTable.build());
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id6", cs, 2, 10, 30, logger);
        assertEquals("_node_id3", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        addJobTask("job_id6", "_node_id3", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id7", cs, 2, 10, 30, logger);
        assertNull("no node selected, because OPENING state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        tasksBuilder.reassignTask(MlTasks.jobTaskId("job_id6"), new Assignment("_node_id3", "test assignment"));
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id7", cs, 2, 10, 30, logger);
        assertNull("no node selected, because stale task", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        tasksBuilder.updateTaskState(MlTasks.jobTaskId("job_id6"), null);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id7", cs, 2, 10, 30, logger);
        assertNull("no node selected, because null state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));
    }

    public void testSelectLeastLoadedMlNode_concurrentOpeningJobsAndStaleFailedJob() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .add(new DiscoveryNode("_node_name3", "_node_id3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                nodeAttr, Collections.emptySet(), Version.CURRENT))
            .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id1", "_node_id1", JobState.fromString("failed"), tasksBuilder);
        // This will make the allocation stale for job_id1
        tasksBuilder.reassignTask(MlTasks.jobTaskId("job_id1"), new Assignment("_node_id1", "test assignment"));
        addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        addJobTask("job_id4", "_node_id2", null, tasksBuilder);
        addJobTask("job_id5", "_node_id3", null, tasksBuilder);
        addJobTask("job_id6", "_node_id3", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.nodes(nodes);
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "job_id1", "job_id2", "job_id3", "job_id4", "job_id5", "job_id6", "job_id7", "job_id8");
        csBuilder.routingTable(routingTable.build());
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        // Allocation won't be possible if the stale failed job is treated as opening
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id7", cs, 2, 10, 30, logger);
        assertEquals("_node_id1", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        addJobTask("job_id7", "_node_id1", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id8", cs, 2, 10, 30, logger);
        assertNull("no node selected, because OPENING state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));
    }

    public void testSelectLeastLoadedMlNode_noCompatibleJobTypeNodes() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("incompatible_type_job", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Function<String, Job> incompatibleJobCreator = jobId -> {
            Job job = mock(Job.class);
            when(job.getId()).thenReturn(jobId);
            when(job.getJobVersion()).thenReturn(Version.CURRENT);
            when(job.getJobType()).thenReturn("incompatible_type");
            when(job.getResultsIndexName()).thenReturn("shared");
            return job;
        };
        addJobAndIndices(metaData, routingTable, incompatibleJobCreator, "incompatible_type_job");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("incompatible_type_job", cs.build(), 2, 10, 30, logger);
        assertThat(result.getExplanation(), containsString("because this node does not support jobs of type [incompatible_type]"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_noNodesMatchingModelSnapshotMinVersion() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.V_6_2_0))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.V_6_1_0))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_with_incompatible_model_snapshot", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, jobId -> BaseMlIntegTestCase.createFareQuoteJob(jobId)
                .setModelSnapshotId("incompatible_snapshot")
                .setModelSnapshotMinVersion(Version.V_6_3_0)
                .build(new Date()), "job_with_incompatible_model_snapshot");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_with_incompatible_model_snapshot", cs.build(),
                2, 10, 30, logger);
        assertThat(result.getExplanation(), containsString(
                "because the job's model snapshot requires a node of version [6.3.0] or higher"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_jobWithRulesButNoNodeMeetsRequiredVersion() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.V_6_2_0))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.V_6_3_0))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_with_rules", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, jobWithRulesCreator(), "job_with_rules");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_with_rules", cs.build(),
                2, 10, 30, logger);
        assertThat(result.getExplanation(), containsString(
                "because jobs using custom_rules require a node of version [6.4.0] or higher"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_jobWithRulesAndNodeMeetsRequiredVersion() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.V_6_2_0))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.V_6_4_0))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_with_rules", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, jobWithRulesCreator(), "job_with_rules");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_with_rules", cs.build(),
                2, 10, 30, logger);
        assertNotNull(result.getExecutorNode());
    }

    public void testVerifyIndicesPrimaryShardsAreActive() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "job_id");

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        assertEquals(0, TransportOpenJobAction.verifyIndicesPrimaryShardsAreActive("job_id", cs).size());

        metaData = new MetaData.Builder(cs.metaData());
        routingTable = new RoutingTable.Builder(cs.routingTable());

        String indexToRemove = randomFrom(TransportOpenJobAction.indicesOfInterest(cs, "job_id"));
        if (randomBoolean()) {
            routingTable.remove(indexToRemove);
        } else {
            Index index = new Index(indexToRemove, "_uuid");
            ShardId shardId = new ShardId(index, 0);
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            routingTable.add(IndexRoutingTable.builder(index)
                    .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
        }

        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);
        List<String> result = TransportOpenJobAction.verifyIndicesPrimaryShardsAreActive("job_id", csBuilder.build());
        assertEquals(1, result.size());
        assertEquals(indexToRemove, result.get(0));
    }

    public void testMappingRequiresUpdateNoMapping() throws IOException {
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        ClusterState cs = csBuilder.build();
        String[] indices = new String[] { "no_index" };

        assertArrayEquals(new String[] { "no_index" }, TransportOpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateNullMapping() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("null_mapping", null));
        String[] indices = new String[] { "null_index" };
        assertArrayEquals(indices, TransportOpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateNoVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("no_version_field", "NO_VERSION_FIELD"));
        String[] indices = new String[] { "no_version_field" };
        assertArrayEquals(indices, TransportOpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateRecentMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_current", Version.CURRENT.toString()));
        String[] indices = new String[] { "version_current" };
        assertArrayEquals(new String[] {}, TransportOpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateMaliciousMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(
                Collections.singletonMap("version_current", Collections.singletonMap("nested", "1.0")));
        String[] indices = new String[] { "version_nested" };
        assertArrayEquals(indices, TransportOpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateBogusMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_bogus", "0.0"));
        String[] indices = new String[] { "version_bogus" };
        assertArrayEquals(indices, TransportOpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateNewerMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_newer", Version.CURRENT));
        String[] indices = new String[] { "version_newer" };
        assertArrayEquals(new String[] {}, TransportOpenJobAction.mappingRequiresUpdate(cs, indices, VersionUtils.getPreviousVersion(),
                logger));
    }

    public void testMappingRequiresUpdateNewerMappingVersionMinor() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_newer_minor", Version.CURRENT));
        String[] indices = new String[] { "version_newer_minor" };
        assertArrayEquals(new String[] {},
                TransportOpenJobAction.mappingRequiresUpdate(cs, indices, VersionUtils.getPreviousMinorVersion(), logger));
    }

    public void testNodeNameAndVersion() {
        TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("unrelated", "attribute");
        DiscoveryNode node = new DiscoveryNode("_node_name1", "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_name1}{version=" + node.getVersion() + "}", TransportOpenJobAction.nodeNameAndVersion(node));
    }

    public void testNodeNameAndMlAttributes() {
        TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
        SortedMap<String, String> attributes = new TreeMap<>();
        attributes.put("unrelated", "attribute");
        DiscoveryNode node = new DiscoveryNode("_node_name1", "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_name1}", TransportOpenJobAction.nodeNameAndMlAttributes(node));

        attributes.put("ml.machine_memory", "5");
        node = new DiscoveryNode("_node_name1", "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_name1}{ml.machine_memory=5}", TransportOpenJobAction.nodeNameAndMlAttributes(node));

        node = new DiscoveryNode(null, "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_id1}{ml.machine_memory=5}", TransportOpenJobAction.nodeNameAndMlAttributes(node));

        attributes.put("node.ml", "true");
        node = new DiscoveryNode("_node_name1", "_node_id1", ta, attributes, Collections.emptySet(), Version.CURRENT);
        assertEquals("{_node_name1}{ml.machine_memory=5}{node.ml=true}", TransportOpenJobAction.nodeNameAndMlAttributes(node));
    }

    public void testJobTaskMatcherMatch() {
        Task nonJobTask1 = mock(Task.class);
        Task nonJobTask2 = mock(Task.class);
        TransportOpenJobAction.JobTask jobTask1 = new TransportOpenJobAction.JobTask("ml-1",
                0, "persistent", "", null, null);
        TransportOpenJobAction.JobTask jobTask2 = new TransportOpenJobAction.JobTask("ml-2",
                1, "persistent", "", null, null);

        assertThat(OpenJobAction.JobTaskMatcher.match(nonJobTask1, "_all"), is(false));
        assertThat(OpenJobAction.JobTaskMatcher.match(nonJobTask2, "_all"), is(false));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask1, "_all"), is(true));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask2, "_all"), is(true));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask1, "ml-1"), is(true));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask2, "ml-1"), is(false));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask1, "ml-2"), is(false));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask2, "ml-2"), is(true));
    }

    public static void addJobTask(String jobId, String nodeId, JobState jobState, PersistentTasksCustomMetaData.Builder builder) {
        builder.addTask(MlTasks.jobTaskId(jobId), OpenJobAction.TASK_NAME, new OpenJobAction.JobParams(jobId),
                new Assignment(nodeId, "test assignment"));
        if (jobState != null) {
            builder.updateTaskState(MlTasks.jobTaskId(jobId), new JobTaskState(jobState, builder.getLastAllocationId()));
        }
    }

    private void addJobAndIndices(MetaData.Builder metaData, RoutingTable.Builder routingTable, String... jobIds) {
        addJobAndIndices(metaData, routingTable, jobId ->
                BaseMlIntegTestCase.createFareQuoteJob(jobId, new ByteSizeValue(2, ByteSizeUnit.MB)).build(new Date()), jobIds);
    }

    private void addJobAndIndices(MetaData.Builder metaData, RoutingTable.Builder routingTable, Function<String, Job> jobCreator,
                                  String... jobIds) {
        List<String> indices = new ArrayList<>();
        indices.add(AnomalyDetectorsIndex.jobStateIndexName());
        indices.add(MlMetaIndex.INDEX_NAME);
        indices.add(AuditorField.NOTIFICATIONS_INDEX);
        indices.add(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT);
        for (String indexName : indices) {
            IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
            indexMetaData.settings(Settings.builder()
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            );
            metaData.put(indexMetaData);
            Index index = new Index(indexName, "_uuid");
            ShardId shardId = new ShardId(index, 0);
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            shardRouting = shardRouting.moveToStarted();
            routingTable.add(IndexRoutingTable.builder(index)
                    .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
        }

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        for (String jobId : jobIds) {
            Job job = jobCreator.apply(jobId);
            mlMetadata.putJob(job, false);
        }
        metaData.putCustom(MlMetadata.TYPE, mlMetadata.build());
    }

    private ClusterState getClusterStateWithMappingsWithMetaData(Map<String, Object> namesAndVersions) throws IOException {
        MetaData.Builder metaDataBuilder = MetaData.builder();

        for (Map.Entry<String, Object> entry : namesAndVersions.entrySet()) {

            String indexName = entry.getKey();
            Object version = entry.getValue();

            IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
            indexMetaData.settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));

            Map<String, Object> mapping = new HashMap<>();
            Map<String, Object> properties = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                properties.put("field" + i, Collections.singletonMap("type", "string"));
            }
            mapping.put("properties", properties);

            Map<String, Object> meta = new HashMap<>();
            if (version != null && version.equals("NO_VERSION_FIELD") == false) {
                meta.put("version", version);
            }
            mapping.put("_meta", meta);

            indexMetaData.putMapping(new MappingMetaData(ElasticsearchMappings.DOC_TYPE, mapping));

            metaDataBuilder.put(indexMetaData);
        }
        MetaData metaData = metaDataBuilder.build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metaData(metaData);
        return csBuilder.build();
    }

    private static Function<String, Job> jobWithRulesCreator() {
        return jobId -> {
            DetectionRule rule = new DetectionRule.Builder(Collections.singletonList(
                    new RuleCondition(RuleCondition.AppliesTo.TYPICAL, Operator.LT, 100.0)
            )).build();

            Detector.Builder detector = new Detector.Builder("count", null);
            detector.setRules(Collections.singletonList(rule));
            AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
            DataDescription.Builder dataDescription = new DataDescription.Builder();
            Job.Builder job = new Job.Builder(jobId);
            job.setAnalysisConfig(analysisConfig);
            job.setDataDescription(dataDescription);
            return job.build(new Date());
        };
    }

}
