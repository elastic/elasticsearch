/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
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
import org.elasticsearch.xpack.core.ml.notifications.AuditorField;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportOpenJobActionTests extends ESTestCase {

    private MlMemoryTracker memoryTracker;
    private boolean isMemoryTrackerRecentlyRefreshed;

    @Before
    public void setup() {
        memoryTracker = mock(MlMemoryTracker.class);
        isMemoryTrackerRecentlyRefreshed = true;
        when(memoryTracker.isRecentlyRefreshed()).thenReturn(isMemoryTrackerRecentlyRefreshed);
    }

    public void testValidate_jobMissing() {
        expectThrows(ResourceNotFoundException.class, () -> TransportOpenJobAction.validate("job_id2", null));
    }

    public void testValidate_jobMarkedAsDeleting() {
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        jobBuilder.setDeleting(true);
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> TransportOpenJobAction.validate("job_id", jobBuilder.build()));
        assertEquals("Cannot open job [job_id] because it is being deleted", e.getMessage());
    }

    public void testValidate_jobWithoutVersion() {
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> TransportOpenJobAction.validate("job_id", jobBuilder.build()));
        assertEquals("Cannot open job [job_id] because jobs created prior to version 5.5 are not supported", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }

    public void testValidate_givenValidJob() {
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        TransportOpenJobAction.validate("job_id", jobBuilder.build(new Date()));
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
        cs.nodes(nodes);
        MetaData.Builder metaData = MetaData.builder();
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job.Builder jobBuilder = buildJobBuilder("job_id4");
        jobBuilder.setJobVersion(Version.CURRENT);

        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id4", jobBuilder.build(),
                cs.build(), 2, 10, 30, memoryTracker, isMemoryTrackerRecentlyRefreshed, logger);
        assertEquals("", result.getExplanation());
        assertEquals("_node_id3", result.getExecutorNode());
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
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id0", new ByteSizeValue(150, ByteSizeUnit.MB)).build(new Date());

        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id0", job, cs.build(), 2,
                maxRunningJobsPerNode, 30, memoryTracker, isMemoryTrackerRecentlyRefreshed, logger);
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
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id2", new ByteSizeValue(2, ByteSizeUnit.MB)).build(new Date());

        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id2", job, cs.build(), 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
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
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        csBuilder.metaData(metaData);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id6", new ByteSizeValue(2, ByteSizeUnit.MB)).build(new Date());

        ClusterState cs = csBuilder.build();
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id6", job, cs, 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
        assertEquals("_node_id3", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        addJobTask("job_id6", "_node_id3", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id7", job, cs, 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
        assertNull("no node selected, because OPENING state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        tasksBuilder.reassignTask(MlTasks.jobTaskId("job_id6"), new Assignment("_node_id3", "test assignment"));
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id7", job, cs, 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
        assertNull("no node selected, because stale task", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        tasksBuilder.updateTaskState(MlTasks.jobTaskId("job_id6"), null);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id7", job, cs, 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
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
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id7", new ByteSizeValue(2, ByteSizeUnit.MB)).build(new Date());

        // Allocation won't be possible if the stale failed job is treated as opening
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id7", job, cs, 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
        assertEquals("_node_id1", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        addJobTask("job_id7", "_node_id1", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = TransportOpenJobAction.selectLeastLoadedMlNode("job_id8", job, cs, 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
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

        Job job = mock(Job.class);
        when(job.getId()).thenReturn("incompatible_type_job");
        when(job.getJobVersion()).thenReturn(Version.CURRENT);
        when(job.getJobType()).thenReturn("incompatible_type");
        when(job.getInitialResultsIndexName()).thenReturn("shared");

        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("incompatible_type_job", job, cs.build(), 2, 10, 30,
            memoryTracker, isMemoryTrackerRecentlyRefreshed, logger);
        assertThat(result.getExplanation(), containsString("because this node does not support jobs of type [incompatible_type]"));
        assertNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_noNodesPriorTo_V_5_5() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.V_5_4_0))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.V_5_4_0))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("incompatible_type_job", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = BaseMlIntegTestCase.createFareQuoteJob("job_id7", new ByteSizeValue(2, ByteSizeUnit.MB)).build(new Date());

        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("incompatible_type_job", job, cs.build(), 2, 10, 30,
            memoryTracker, isMemoryTrackerRecentlyRefreshed, logger);
        assertThat(result.getExplanation(), containsString("because this node does not support machine learning jobs"));
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
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = jobWithRules("job_with_rules");
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_with_rules", job, cs.build(), 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
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
                        nodeAttr, Collections.emptySet(), Version.V_6_6_0))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_with_rules", "_node_id1", null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);

        Job job = jobWithRules("job_with_rules");
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("job_with_rules", job, cs.build(), 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
        assertNotNull(result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_indexJobsCannotBeAssignedToPre660Node() {
        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.V_6_5_0));


        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        cs.nodes(nodes);

        Job job = jobWithRules("post-v650-job");
        Assignment result = TransportOpenJobAction.selectLeastLoadedMlNode("post-v650-job", job, cs.build(), 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
        assertNull(result.getExecutorNode());
        assertThat(result.getExplanation(), containsString("Not opening job [post-v650-job] on node [_node_name1] version [6.5.0], " +
                "because this node does not support jobs of version "));

        nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name1", "_node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        nodeAttr, Collections.emptySet(), Version.V_6_5_0))
                .add(new DiscoveryNode("_node_name2", "_node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                        nodeAttr, Collections.emptySet(), Version.V_6_6_0));
        cs.nodes(nodes);
        result = TransportOpenJobAction.selectLeastLoadedMlNode("post-v650-job", job, cs.build(), 2, 10, 30, memoryTracker,
            isMemoryTrackerRecentlyRefreshed, logger);
        assertThat(result.getExplanation(), isEmptyOrNullString());
        assertEquals("_node_id2", result.getExecutorNode());
    }

    public void testVerifyIndicesPrimaryShardsAreActive() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metaData, routingTable);

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        assertEquals(0, TransportOpenJobAction.verifyIndicesPrimaryShardsAreActive(".ml-anomalies-shared", cs).size());

        metaData = new MetaData.Builder(cs.metaData());
        routingTable = new RoutingTable.Builder(cs.routingTable());

        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);
        String indexToRemove = randomFrom(resolver.concreteIndexNames(cs,
            IndicesOptions.lenientExpandOpen(),
            TransportOpenJobAction.indicesOfInterest(".ml-anomalies-shared")));
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
        List<String> result = TransportOpenJobAction.verifyIndicesPrimaryShardsAreActive(".ml-anomalies-shared", csBuilder.build());
        assertEquals(1, result.size());
        assertEquals(indexToRemove, result.get(0));
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

    public void testGetAssignment_GivenJobThatRequiresMigration() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY,
            Sets.newHashSet(MachineLearning.CONCURRENT_JOB_ALLOCATIONS, MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
                MachineLearning.MAX_LAZY_ML_NODES)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        TransportOpenJobAction.OpenJobPersistentTasksExecutor executor = new TransportOpenJobAction.OpenJobPersistentTasksExecutor(
                Settings.EMPTY, clusterService, mock(AutodetectProcessManager.class), mock(MlMemoryTracker.class), mock(Client.class));

        OpenJobAction.JobParams params = new OpenJobAction.JobParams("missing_job_field");
        assertEquals(TransportOpenJobAction.AWAITING_MIGRATION, executor.getAssignment(params, mock(ClusterState.class)));
    }

    // An index being unavailable should take precedence over waiting for a lazy node
    public void testGetAssignment_GivenUnavailableIndicesWithLazyNode() {
        Settings settings = Settings.builder().put(MachineLearning.MAX_LAZY_ML_NODES.getKey(), 1).build();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(settings,
            Sets.newHashSet(MachineLearning.CONCURRENT_JOB_ALLOCATIONS, MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
                MachineLearning.MAX_LAZY_ML_NODES)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metaData, routingTable);
        routingTable.remove(".ml-state");
        csBuilder.metaData(metaData);
        csBuilder.routingTable(routingTable.build());

        TransportOpenJobAction.OpenJobPersistentTasksExecutor executor = new TransportOpenJobAction.OpenJobPersistentTasksExecutor(
            settings, clusterService, mock(AutodetectProcessManager.class), mock(MlMemoryTracker.class), mock(Client.class));

        OpenJobAction.JobParams params = new OpenJobAction.JobParams("unavailable_index_with_lazy_node");
        params.setJob(mock(Job.class));
        assertEquals("Not opening job [unavailable_index_with_lazy_node], " +
            "because not all primary shards are active for the following indices [.ml-state]",
            executor.getAssignment(params, csBuilder.build()).getExplanation());
    }

    public static void addJobTask(String jobId, String nodeId, JobState jobState, PersistentTasksCustomMetaData.Builder builder) {
        addJobTask(jobId, nodeId, jobState, builder, false);
    }

    public static void addJobTask(String jobId, String nodeId, JobState jobState, PersistentTasksCustomMetaData.Builder builder,
                                  boolean isStale) {
        builder.addTask(MlTasks.jobTaskId(jobId), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(jobId),
            new Assignment(nodeId, "test assignment"));
        if (jobState != null) {
            builder.updateTaskState(MlTasks.jobTaskId(jobId),
                new JobTaskState(jobState, builder.getLastAllocationId() - (isStale ? 1 : 0)));
        }
    }

    private void addIndices(MetaData.Builder metaData, RoutingTable.Builder routingTable) {
        List<String> indices = new ArrayList<>();
        indices.add(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX);
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
            if (indexName.equals(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX)) {
                indexMetaData.putAlias(new AliasMetaData.Builder(AnomalyDetectorsIndex.jobStateIndexWriteAlias()));
            }
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
    }

    private static Job jobWithRules(String jobId) {
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
    }

}
