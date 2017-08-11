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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobTaskStatus;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenJobActionTests extends ESTestCase {

    public void testValidate_jobMissing() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id1").build(), false);
        expectThrows(ResourceNotFoundException.class, () -> OpenJobAction.validate("job_id2", mlBuilder.build()));
    }

    public void testValidate_jobMarkedAsDeleted() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        jobBuilder.setDeleted(true);
        mlBuilder.putJob(jobBuilder.build(), false);
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> OpenJobAction.validate("job_id", mlBuilder.build()));
        assertEquals("Cannot open job [job_id] because it has been marked as deleted", e.getMessage());
    }

    public void testValidate_jobWithoutVersion() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        mlBuilder.putJob(jobBuilder.build(), false);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> OpenJobAction.validate("job_id", mlBuilder.build()));
        assertEquals("Cannot open job [job_id] because jobs created prior to version 5.5 are not supported", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }

    public void testValidate_givenValidJob() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        mlBuilder.putJob(jobBuilder.build(new Date()), false);
        OpenJobAction.validate("job_id", mlBuilder.build());
    }

    public void testSelectLeastLoadedMlNode() {
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
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "job_id1", "job_id2", "job_id3", "job_id4");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = OpenJobAction.selectLeastLoadedMlNode("job_id4", cs.build(), 2, 10, logger);
        assertEquals("_node_id3", result.getExecutorNode());
    }

    public void testSelectLeastLoadedMlNode_maxCapacity() {
        int numNodes = randomIntBetween(1, 10);
        int maxRunningJobsPerNode = randomIntBetween(1, 100);

        Map<String, String> nodeAttr = new HashMap<>();
        nodeAttr.put(MachineLearning.ML_ENABLED_NODE_ATTR, "true");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "_node_id" + i;
            TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i);
            nodes.add(new DiscoveryNode("_node_name" + i, nodeId, address, nodeAttr, Collections.emptySet(), Version.CURRENT));
            for (int j = 0; j < maxRunningJobsPerNode; j++) {
                long id = j + (maxRunningJobsPerNode * i);
                addJobTask("job_id" + id, nodeId, JobState.OPENED, tasksBuilder);
            }
        }
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "job_id1", "job_id2");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = OpenJobAction.selectLeastLoadedMlNode("job_id2", cs.build(), 2, maxRunningJobsPerNode, logger);
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
        Assignment result = OpenJobAction.selectLeastLoadedMlNode("job_id2", cs.build(), 2, 10, logger);
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

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
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
        Assignment result = OpenJobAction.selectLeastLoadedMlNode("job_id6", cs, 2, 10, logger);
        assertEquals("_node_id3", result.getExecutorNode());

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        addJobTask("job_id6", "_node_id3", null, tasksBuilder);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = OpenJobAction.selectLeastLoadedMlNode("job_id7", cs, 2, 10, logger);
        assertNull("no node selected, because OPENING state", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        tasksBuilder.reassignTask(MlMetadata.jobTaskId("job_id6"), new Assignment("_node_id3", "test assignment"));
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = OpenJobAction.selectLeastLoadedMlNode("job_id7", cs, 2, 10, logger);
        assertNull("no node selected, because stale task", result.getExecutorNode());
        assertTrue(result.getExplanation().contains("because node exceeds [2] the maximum number of jobs [2] in opening state"));

        tasksBuilder = PersistentTasksCustomMetaData.builder(tasks);
        tasksBuilder.updateTaskStatus(MlMetadata.jobTaskId("job_id6"), null);
        tasks = tasksBuilder.build();

        csBuilder = ClusterState.builder(cs);
        csBuilder.metaData(MetaData.builder(cs.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasks));
        cs = csBuilder.build();
        result = OpenJobAction.selectLeastLoadedMlNode("job_id7", cs, 2, 10, logger);
        assertNull("no node selected, because null state", result.getExecutorNode());
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
        Assignment result = OpenJobAction.selectLeastLoadedMlNode("incompatible_type_job", cs.build(), 2, 10, logger);
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
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "incompatible_type_job");
        cs.nodes(nodes);
        metaData.putCustom(PersistentTasksCustomMetaData.TYPE, tasks);
        cs.metaData(metaData);
        cs.routingTable(routingTable.build());
        Assignment result = OpenJobAction.selectLeastLoadedMlNode("incompatible_type_job", cs.build(), 2, 10, logger);
        assertThat(result.getExplanation(), containsString("because this node does not support jobs of version [" + Version.CURRENT + "]"));
        assertNull(result.getExecutorNode());
    }

    public void testVerifyIndicesPrimaryShardsAreActive() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addJobAndIndices(metaData, routingTable, "job_id");

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);

        ClusterState cs = csBuilder.build();
        assertEquals(0, OpenJobAction.verifyIndicesPrimaryShardsAreActive("job_id", cs).size());

        metaData = new MetaData.Builder(cs.metaData());
        routingTable = new RoutingTable.Builder(cs.routingTable());

        String indexToRemove = randomFrom(OpenJobAction.indicesOfInterest(cs, "job_id"));
        if (randomBoolean()) {
            routingTable.remove(indexToRemove);
        } else {
            Index index = new Index(indexToRemove, "_uuid");
            ShardId shardId = new ShardId(index, 0);
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            routingTable.add(IndexRoutingTable.builder(index)
                    .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
        }

        csBuilder.routingTable(routingTable.build());
        csBuilder.metaData(metaData);
        List<String> result = OpenJobAction.verifyIndicesPrimaryShardsAreActive("job_id", csBuilder.build());
        assertEquals(1, result.size());
        assertEquals(indexToRemove, result.get(0));
    }

    public void testMappingRequiresUpdateNoMapping() throws IOException {
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        ClusterState cs = csBuilder.build();
        String[] indices = new String[] { "no_index" };

        assertArrayEquals(new String[] { "no_index" }, OpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateNullMapping() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("null_mapping", null));
        String[] indices = new String[] { "null_index" };
        assertArrayEquals(indices, OpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateNoVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("no_version_field", "NO_VERSION_FIELD"));
        String[] indices = new String[] { "no_version_field" };
        assertArrayEquals(indices, OpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateRecentMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_current", Version.CURRENT.toString()));
        String[] indices = new String[] { "version_current" };
        assertArrayEquals(new String[] {}, OpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateMaliciousMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(
                Collections.singletonMap("version_current", Collections.singletonMap("nested", "1.0")));
        String[] indices = new String[] { "version_nested" };
        assertArrayEquals(indices, OpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateOldMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_54", Version.V_5_4_0.toString()));
        String[] indices = new String[] { "version_54" };
        assertArrayEquals(indices, OpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateBogusMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_bogus", "0.0"));
        String[] indices = new String[] { "version_bogus" };
        assertArrayEquals(indices, OpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public void testMappingRequiresUpdateNewerMappingVersion() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_newer", Version.CURRENT));
        String[] indices = new String[] { "version_newer" };
        assertArrayEquals(new String[] {}, OpenJobAction.mappingRequiresUpdate(cs, indices, VersionUtils.getPreviousVersion(), logger));
    }

    public void testMappingRequiresUpdateNewerMappingVersionMinor() throws IOException {
        ClusterState cs = getClusterStateWithMappingsWithMetaData(Collections.singletonMap("version_newer_minor", Version.CURRENT));
        String[] indices = new String[] { "version_newer_minor" };
        assertArrayEquals(new String[] {},
                OpenJobAction.mappingRequiresUpdate(cs, indices, VersionUtils.getPreviousMinorVersion(), logger));
    }

    public void testMappingRequiresUpdateSomeVersionMix() throws IOException {
        Map<String, Object> versionMix = new HashMap<String, Object>();
        versionMix.put("version_54", Version.V_5_4_0);
        versionMix.put("version_current", Version.CURRENT);
        versionMix.put("version_null", null);
        versionMix.put("version_current2", Version.CURRENT);
        versionMix.put("version_bogus", "0.0.0");
        versionMix.put("version_current3", Version.CURRENT);
        versionMix.put("version_bogus2", "0.0.0");

        ClusterState cs = getClusterStateWithMappingsWithMetaData(versionMix);
        String[] indices = new String[] { "version_54", "version_null", "version_bogus", "version_bogus2" };
        assertArrayEquals(indices, OpenJobAction.mappingRequiresUpdate(cs, indices, Version.CURRENT, logger));
    }

    public static void addJobTask(String jobId, String nodeId, JobState jobState, PersistentTasksCustomMetaData.Builder builder) {
        builder.addTask(MlMetadata.jobTaskId(jobId), OpenJobAction.TASK_NAME, new OpenJobAction.JobParams(jobId),
                new Assignment(nodeId, "test assignment"));
        if (jobState != null) {
            builder.updateTaskStatus(MlMetadata.jobTaskId(jobId), new JobTaskStatus(jobState, builder.getLastAllocationId()));
        }
    }

    private void addJobAndIndices(MetaData.Builder metaData, RoutingTable.Builder routingTable, String... jobIds) {
        addJobAndIndices(metaData, routingTable, jobId -> BaseMlIntegTestCase.createFareQuoteJob(jobId).build(new Date()), jobIds);
    }

    private void addJobAndIndices(MetaData.Builder metaData, RoutingTable.Builder routingTable, Function<String, Job> jobCreator,
                                  String... jobIds) {
        List<String> indices = new ArrayList<>();
        indices.add(AnomalyDetectorsIndex.jobStateIndexName());
        indices.add(MlMetaIndex.INDEX_NAME);
        indices.add(Auditor.NOTIFICATIONS_INDEX);
        indices.add(AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT);
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
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE,
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

}
