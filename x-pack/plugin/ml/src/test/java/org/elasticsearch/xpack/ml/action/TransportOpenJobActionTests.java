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
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportOpenJobActionTests extends ESTestCase {

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
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
        String indexToRemove = randomFrom(indexNameExpressionResolver.concreteIndexNames(cs, IndicesOptions.lenientExpandOpen(),
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
                MachineLearning.MAX_LAZY_ML_NODES, MachineLearning.MAX_OPEN_JOBS_PER_NODE)
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
                MachineLearning.MAX_LAZY_ML_NODES, MachineLearning.MAX_OPEN_JOBS_PER_NODE)
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
                new JobTaskState(jobState, builder.getLastAllocationId() - (isStale ? 1 : 0), null));
        }
    }

    private void addIndices(MetaData.Builder metaData, RoutingTable.Builder routingTable) {
        List<String> indices = new ArrayList<>();
        indices.add(AnomalyDetectorsIndex.configIndexName());
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

    public static Job jobWithRules(String jobId) {
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
