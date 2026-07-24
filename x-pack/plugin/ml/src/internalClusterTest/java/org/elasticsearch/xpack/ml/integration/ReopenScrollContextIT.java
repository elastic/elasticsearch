/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.persistent.PersistentTasksClusterService.isUnassignedOrMisassigned;
import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.xpack.core.ml.MlTasks.DATAFEED_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.JOB_TASK_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration regression: datafeed-attached AD jobs recover to {@link JobState#OPENED} and datafeeds resume
 * after full-fleet reassignment under a constrained {@code search.max_open_scroll_context}.
 * <p>
 * Per-attempt slice reduction and capacity-aware reopen backoff are covered by unit tests in
 * {@link org.elasticsearch.xpack.ml.job.persistence.JobDataDeleterTests} and
 * {@link org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests}. Internal-cluster ML
 * result/annotation indices are single-shard, so revert-on-open {@code AUTO_SLICES} resolves to one slice here;
 * this IT validates end-to-end recovery after node failure, not scroll-context exhaustion.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReopenScrollContextIT extends BaseMlIntegTestCase {

    private static final int NUM_JOBS = 8;
    private static final int SCROLL_CONTEXT_BUDGET = 5;
    private static final String JOB_ID_PREFIX = "reopen-scroll-";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(), NUM_JOBS)
            .build();
    }

    public void testDatafeedAttachedJobsShouldRecoverAfterReassignmentWithStarvedScrollBudget() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startMasterOnlyNode();
        String coLocatedMlNode = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        ClusterUpdateSettingsRequest scrollBudgetRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        scrollBudgetRequest.persistentSettings(
            Settings.builder().put(SearchService.MAX_OPEN_SCROLL_CONTEXT.getKey(), SCROLL_CONTEXT_BUDGET).build()
        );
        client().admin().cluster().updateSettings(scrollBudgetRequest).actionGet();

        List<String> jobIds = IntStream.range(0, NUM_JOBS).mapToObj(i -> JOB_ID_PREFIX + i).toList();
        for (int i = 0; i < NUM_JOBS; i++) {
            String jobId = jobIds.get(i);
            String indexName = "data-" + i;
            String datafeedId = jobId + "-datafeed";
            setupSourceIndexAndJobWithDatafeed(jobId, datafeedId, indexName);
        }

        assertBusy(() -> assertAllJobsCoLocatedOnNode(jobIds, coLocatedMlNode), 30, TimeUnit.SECONDS);

        // Jobs happily running on the first ML node do not rebalance when a second ML node joins.
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();
        assertBusy(() -> assertAllJobsCoLocatedOnNode(jobIds, coLocatedMlNode), 30, TimeUnit.SECONDS);

        setMlIndicesDelayedNodeLeftTimeoutToZero();
        ensureGreen();

        for (String jobId : jobIds) {
            String datafeedId = jobId + "-datafeed";
            StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(datafeedId, 0L);
            client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
            assertBusy(
                () -> { assertThat(getDatafeedStats(datafeedId).getDatafeedState(), equalTo(DatafeedState.STARTED)); },
                30,
                TimeUnit.SECONDS
            );
        }

        for (String jobId : jobIds) {
            indexModelSnapshotFromCurrentJobStats(jobId);
        }
        client().admin().indices().prepareFlush().get();

        Map<String, String> jobNodesBeforeFailure = jobIds.stream()
            .collect(Collectors.toMap(jobId -> jobId, jobId -> getJobStats(jobId).getNode().getName()));

        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName()
        );
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(200));

        internalCluster().stopNode(coLocatedMlNode);
        ensureStableCluster();

        awaitClusterState(state -> {
            List<PersistentTasksCustomMetadata.PersistentTask<?>> tasks = findTasks(state, Set.of(DATAFEED_TASK_NAME, JOB_TASK_NAME));
            if (tasks == null || tasks.size() != NUM_JOBS * 2) {
                return false;
            }
            for (PersistentTasksCustomMetadata.PersistentTask<?> task : tasks) {
                if (isUnassignedOrMisassigned(task.getAssignment(), state.nodes())) {
                    return false;
                }
            }
            return true;
        });

        assertBusy(() -> {
            for (String jobId : jobIds) {
                GetJobsStatsAction.Response.JobStats jobStats = getJobStats(jobId);
                JobState state = jobStats.getState();
                assertThat("job [" + jobId + "] must not fail during reopen", state, not(equalTo(JobState.FAILED)));
                assertThat("job [" + jobId + "] must reopen after reassignment", state, equalTo(JobState.OPENED));
                assertThat("job [" + jobId + "] must be reassigned", jobStats.getNode(), is(notNullValue()));
                assertThat(
                    "job [" + jobId + "] must not remain on the stopped node",
                    jobStats.getNode().getName(),
                    not(equalTo(coLocatedMlNode))
                );
                assertThat(
                    "job [" + jobId + "] must change assignment after node failure",
                    jobStats.getNode().getName(),
                    not(equalTo(jobNodesBeforeFailure.get(jobId)))
                );

                String datafeedId = jobId + "-datafeed";
                GetDatafeedsStatsAction.Response.DatafeedStats datafeedStats = getDatafeedStats(datafeedId);
                assertThat("datafeed [" + datafeedId + "] must resume", datafeedStats.getDatafeedState(), equalTo(DatafeedState.STARTED));
                assertThat("datafeed [" + datafeedId + "] must be reassigned", datafeedStats.getNode(), is(notNullValue()));
                assertThat(
                    "datafeed [" + datafeedId + "] must not remain on the stopped node",
                    datafeedStats.getNode().getName(),
                    not(equalTo(coLocatedMlNode))
                );
            }
        }, 5, TimeUnit.MINUTES);
    }

    private void assertAllJobsCoLocatedOnNode(List<String> jobIds, String expectedNode) {
        Set<String> nodes = jobIds.stream().map(jobId -> getJobStats(jobId).getNode().getName()).collect(Collectors.toSet());
        assertEquals("all jobs must run on a single ML node", 1, nodes.size());
        assertEquals(expectedNode, nodes.iterator().next());
    }

    private void setupSourceIndexAndJobWithDatafeed(String jobId, String datafeedId, String indexName) throws Exception {
        client().admin().indices().prepareCreate(indexName).setMapping("time", "type=date").get();
        long numDocs = randomIntBetween(32, 256);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000L;
        long twoWeeksAgo = weekAgo - 604800000L;
        indexDocs(logger, indexName, numDocs, twoWeeksAgo, weekAgo);

        Job.Builder job = createScheduledJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        DatafeedConfig datafeedConfig = createDatafeed(
            datafeedId,
            jobId,
            Collections.singletonList(indexName),
            TimeValue.timeValueSeconds(1)
        );
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(datafeedConfig)).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        }, 30, TimeUnit.SECONDS);
    }

    private void indexModelSnapshotFromCurrentJobStats(String jobId) throws Exception {
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(jobId);
        DataCounts dataCounts = jobStats.getDataCounts();

        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(jobId).setLatestResultTimeStamp(dataCounts.getLatestRecordTimeStamp())
            .setLatestRecordTimeStamp(dataCounts.getLatestRecordTimeStamp())
            .setMinVersion(MlConfigVersion.CURRENT)
            .setSnapshotId(jobId + "_mock_snapshot")
            .setTimestamp(new Date())
            .setModelSizeStats(new ModelSizeStats.Builder(jobId).build())
            .build();

        try (XContentBuilder xContentBuilder = JsonXContent.contentBuilder()) {
            modelSnapshot.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            indexRequest.id(ModelSnapshot.documentId(modelSnapshot));
            indexRequest.source(xContentBuilder);
            client().index(indexRequest).actionGet();
        }

        JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setModelSnapshotId(modelSnapshot.getSnapshotId()).build();
        client().execute(UpdateJobAction.INSTANCE, new UpdateJobAction.Request(jobId, jobUpdate)).actionGet();
    }
}
