/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.task;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
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
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutor.validateJobAndId;
import static org.elasticsearch.xpack.ml.task.AbstractJobPersistentTasksExecutor.AWAITING_MIGRATION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenJobPersistentTasksExecutorTests extends ESTestCase {

    private ClusterService clusterService;
    private AutodetectProcessManager autodetectProcessManager;
    private DatafeedConfigProvider datafeedConfigProvider;
    private Client client;
    private MlMemoryTracker mlMemoryTracker;

    @Before
    public void setUpMocks() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        Settings settings = Settings.builder().put("node.name", "OpenJobPersistentTasksExecutorTests").build();
        ClusterSettings clusterSettings = new ClusterSettings(settings,
            new HashSet<>(Arrays.asList(InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                ClusterService.USER_DEFINED_METADATA,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                MachineLearning.CONCURRENT_JOB_ALLOCATIONS,
                MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
                MachineLearning.MAX_LAZY_ML_NODES,
                MachineLearning.MAX_ML_NODE_SIZE,
                MachineLearning.MAX_OPEN_JOBS_PER_NODE,
                MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT)));
        clusterService = new ClusterService(settings, clusterSettings, tp);
        autodetectProcessManager = mock(AutodetectProcessManager.class);
        datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        client = mock(Client.class);
        mlMemoryTracker = mock(MlMemoryTracker.class);
    }

    public void testValidate_jobMissing() {
        expectThrows(ResourceNotFoundException.class, () -> validateJobAndId("job_id2", null));
    }

    public void testValidate_jobMarkedAsDeleting() {
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        jobBuilder.setDeleting(true);
        Exception e = expectThrows(ElasticsearchStatusException.class,
            () -> validateJobAndId("job_id", jobBuilder.build()));
        assertEquals("Cannot open job [job_id] because it is executing [delete]", e.getMessage());
    }

    public void testValidate_blockedReset() {
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        jobBuilder.setBlocked(new Blocked(Blocked.Reason.REVERT, null));
        Exception e = expectThrows(ElasticsearchStatusException.class,
            () -> validateJobAndId("job_id", jobBuilder.build()));
        assertEquals("Cannot open job [job_id] because it is executing [revert]", e.getMessage());
    }

    public void testValidate_jobWithoutVersion() {
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> validateJobAndId("job_id", jobBuilder.build()));
        assertEquals("Cannot open job [job_id] because jobs created prior to version 5.5 are not supported", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }

    public void testValidate_givenValidJob() {
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        validateJobAndId("job_id", jobBuilder.build(new Date()));
    }

    public void testGetAssignment_GivenJobThatRequiresMigration() {
        OpenJobPersistentTasksExecutor executor = createExecutor(Settings.EMPTY);

        OpenJobAction.JobParams params = new OpenJobAction.JobParams("missing_job_field");
        assertEquals(AWAITING_MIGRATION, executor.getAssignment(params, Collections.emptyList(), mock(ClusterState.class)));
    }

    // An index being unavailable should take precedence over waiting for a lazy node
    public void testGetAssignment_GivenUnavailableIndicesWithLazyNode() {
        Settings settings = Settings.builder().put(MachineLearning.MAX_LAZY_ML_NODES.getKey(), 1).build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metadata, routingTable);
        routingTable.remove(".ml-state");
        csBuilder.metadata(metadata);
        csBuilder.routingTable(routingTable.build());

        OpenJobPersistentTasksExecutor executor = createExecutor(settings);

        OpenJobAction.JobParams params = new OpenJobAction.JobParams("unavailable_index_with_lazy_node");
        params.setJob(mock(Job.class));
        assertEquals("Not opening [unavailable_index_with_lazy_node], " +
                "because not all primary shards are active for the following indices [.ml-state]",
            executor.getAssignment(params, csBuilder.nodes().getAllNodes(), csBuilder.build()).getExplanation());
    }

    public void testGetAssignment_GivenLazyJobAndNoGlobalLazyNodes() {
        Settings settings = Settings.builder().put(MachineLearning.MAX_LAZY_ML_NODES.getKey(), 0).build();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addIndices(metadata, routingTable);
        csBuilder.metadata(metadata);
        csBuilder.routingTable(routingTable.build());

        OpenJobPersistentTasksExecutor executor = createExecutor(settings);

        Job job = mock(Job.class);
        when(job.allowLazyOpen()).thenReturn(true);
        OpenJobAction.JobParams params = new OpenJobAction.JobParams("lazy_job");
        params.setJob(job);
        PersistentTasksCustomMetadata.Assignment assignment = executor.getAssignment(params,
            csBuilder.nodes().getAllNodes(), csBuilder.build());
        assertNotNull(assignment);
        assertNull(assignment.getExecutorNode());
        assertEquals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT.getExplanation(), assignment.getExplanation());
    }

    public void testGetAssignment_GivenResetInProgress() {
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        Metadata.Builder metadata = Metadata.builder();
        MlMetadata mlMetadata = new MlMetadata.Builder().isResetMode(true).build();
        csBuilder.metadata(metadata.putCustom(MlMetadata.TYPE, mlMetadata));

        OpenJobPersistentTasksExecutor executor = createExecutor(Settings.EMPTY);

        Job job = mock(Job.class);
        OpenJobAction.JobParams params = new OpenJobAction.JobParams("job_during_reset");
        params.setJob(job);
        PersistentTasksCustomMetadata.Assignment assignment = executor.getAssignment(params,
            csBuilder.nodes().getAllNodes(), csBuilder.build());
        assertNotNull(assignment);
        assertNull(assignment.getExecutorNode());
        assertEquals(MlTasks.RESET_IN_PROGRESS.getExplanation(), assignment.getExplanation());
    }

    public static void addJobTask(String jobId, String nodeId, JobState jobState, PersistentTasksCustomMetadata.Builder builder) {
        addJobTask(jobId, nodeId, jobState, builder, false);
    }

    public static void addJobTask(String jobId, String nodeId, JobState jobState, PersistentTasksCustomMetadata.Builder builder,
                                  boolean isStale) {
        builder.addTask(MlTasks.jobTaskId(jobId), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(jobId),
            new PersistentTasksCustomMetadata.Assignment(nodeId, "test assignment"));
        if (jobState != null) {
            builder.updateTaskState(MlTasks.jobTaskId(jobId),
                new JobTaskState(jobState, builder.getLastAllocationId() - (isStale ? 1 : 0), null));
        }
    }

    private void addIndices(Metadata.Builder metadata, RoutingTable.Builder routingTable) {
        List<String> indices = new ArrayList<>();
        indices.add(MlConfigIndex.indexName());
        indices.add(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX);
        indices.add(MlMetaIndex.indexName());
        indices.add(NotificationsIndex.NOTIFICATIONS_INDEX);
        indices.add(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT);
        for (String indexName : indices) {
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
            indexMetadata.settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
            if (indexName.equals(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX)) {
                indexMetadata.putAlias(new AliasMetadata.Builder(AnomalyDetectorsIndex.jobStateIndexWriteAlias()));
            }
            metadata.put(indexMetadata);
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

    private OpenJobPersistentTasksExecutor createExecutor(Settings settings) {
        return new OpenJobPersistentTasksExecutor(
            settings, clusterService, autodetectProcessManager, datafeedConfigProvider, mlMemoryTracker, client,
            TestIndexNameExpressionResolver.newInstance());
    }
}
