/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.junit.After;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.getDataCounts;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.getDatafeedStats;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.indexDocs;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SetUpgradeModeIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanup() throws Exception {
        cleanUp();
    }

    public void testEnableUpgradeMode() throws Exception {
        String jobId = "realtime-job-test-enable-upgrade-mode";
        String datafeedId = jobId + "-datafeed";
        startRealtime(jobId);

        assertThat(upgradeMode(), is(false));

        // Assert appropriate task state and assignment numbers
        assertThat(
            clusterAdmin().prepareListTasks()
                .setActions(MlTasks.JOB_TASK_NAME + "[c]", MlTasks.DATAFEED_TASK_NAME + "[c]")
                .get()
                .getTasks(),
            hasSize(2)
        );

        ClusterState masterClusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).all().get().getState();

        PersistentTasksCustomMetadata persistentTasks = masterClusterState.getMetadata()
            .getProject()
            .custom(PersistentTasksCustomMetadata.TYPE);
        assertThat(persistentTasks.findTasks(MlTasks.DATAFEED_TASK_NAME, task -> true), hasSize(1));
        assertThat(persistentTasks.findTasks(MlTasks.JOB_TASK_NAME, task -> true), hasSize(1));

        // Set the upgrade mode setting
        setUpgradeModeTo(true);

        masterClusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).all().get().getState();

        // Assert state for tasks still exists and that the upgrade setting is set
        persistentTasks = masterClusterState.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        assertThat(persistentTasks.findTasks(MlTasks.DATAFEED_TASK_NAME, task -> true), hasSize(1));
        assertThat(persistentTasks.findTasks(MlTasks.JOB_TASK_NAME, task -> true), hasSize(1));

        assertThat(
            clusterAdmin().prepareListTasks()
                .setActions(MlTasks.JOB_TASK_NAME + "[c]", MlTasks.DATAFEED_TASK_NAME + "[c]")
                .get()
                .getTasks(),
            is(empty())
        );

        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(jobId).get(0);
        assertThat(jobStats.getState(), is(equalTo(JobState.OPENED)));
        assertThat(jobStats.getAssignmentExplanation(), is(equalTo(AWAITING_UPGRADE.getExplanation())));
        assertThat(jobStats.getNode(), is(nullValue()));

        GetDatafeedsStatsAction.Response.DatafeedStats datafeedStats = getDatafeedStats(datafeedId);
        assertThat(datafeedStats.getDatafeedState(), is(equalTo(DatafeedState.STARTED)));
        assertThat(datafeedStats.getAssignmentExplanation(), is(equalTo(AWAITING_UPGRADE.getExplanation())));
        assertThat(datafeedStats.getNode(), is(nullValue()));

        // Disable the setting
        setUpgradeModeTo(false);

        masterClusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).all().get().getState();

        persistentTasks = masterClusterState.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        assertThat(persistentTasks.findTasks(MlTasks.DATAFEED_TASK_NAME, task -> true), hasSize(1));
        assertThat(persistentTasks.findTasks(MlTasks.JOB_TASK_NAME, task -> true), hasSize(1));

        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareListTasks()
                    .setActions(MlTasks.JOB_TASK_NAME + "[c]", MlTasks.DATAFEED_TASK_NAME + "[c]")
                    .get()
                    .getTasks(),
                hasSize(2)
            )
        );

        jobStats = getJobStats(jobId).get(0);
        assertThat(jobStats.getState(), is(equalTo(JobState.OPENED)));
        assertThat(jobStats.getAssignmentExplanation(), is(not(equalTo(AWAITING_UPGRADE.getExplanation()))));

        datafeedStats = getDatafeedStats(datafeedId);
        assertThat(datafeedStats.getDatafeedState(), is(equalTo(DatafeedState.STARTED)));
        assertThat(datafeedStats.getAssignmentExplanation(), is(not(equalTo(AWAITING_UPGRADE.getExplanation()))));
    }

    public void testJobOpenActionInUpgradeMode() {
        String jobId = "job-should-not-open";
        Job.Builder job = createScheduledJob(jobId);
        putJob(job);

        setUpgradeModeTo(true);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> openJob(jobId));
        assertThat(e.getMessage(), is(equalTo("Cannot perform cluster:admin/xpack/ml/job/open action while upgrade mode is enabled")));
        assertThat(e.status(), is(equalTo(RestStatus.TOO_MANY_REQUESTS)));
    }

    public void testAnomalyDetectionActionsInUpgradeMode() {
        setUpgradeModeTo(true);

        String jobId = "job_id";
        expectThrowsUpgradeModeException(() -> putJob(createScheduledJob(jobId)));
        expectThrowsUpgradeModeException(() -> updateJob(jobId, null));
        expectThrowsUpgradeModeException(() -> deleteJob(jobId));
        expectThrowsUpgradeModeException(() -> openJob(jobId));
        expectThrowsUpgradeModeException(() -> flushJob(jobId, false));
        expectThrowsUpgradeModeException(() -> closeJob(jobId));
        expectThrowsUpgradeModeException(() -> persistJob(jobId));
        expectThrowsUpgradeModeException(() -> forecast(jobId, null, null));

        String snapshotId = "snapshot_id";
        expectThrowsUpgradeModeException(() -> revertModelSnapshot(jobId, snapshotId, false));

        String datafeedId = "datafeed_id";
        expectThrowsUpgradeModeException(() -> putDatafeed(createDatafeed(datafeedId, jobId, Collections.singletonList("index"))));
        expectThrowsUpgradeModeException(() -> updateDatafeed(new DatafeedUpdate.Builder(datafeedId).build()));
        expectThrowsUpgradeModeException(() -> deleteDatafeed(datafeedId));
        expectThrowsUpgradeModeException(() -> startDatafeed(datafeedId, 0, null));
        expectThrowsUpgradeModeException(() -> stopDatafeed(datafeedId));

        String filterId = "filter_id";
        expectThrowsUpgradeModeException(() -> putMlFilter(MlFilter.builder(filterId).build()));

        String calendarId = "calendar_id";
        expectThrowsUpgradeModeException(() -> putCalendar(calendarId, Collections.singletonList(jobId), ""));
    }

    private void startRealtime(String jobId) throws Exception {
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long lastWeek = now - 604800000;
        indexDocs(logger, "data", numDocs1, lastWeek, now);

        Job.Builder job = createScheduledJob(jobId);
        putJob(job);
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data"));
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, null);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), is(equalTo(numDocs1)));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), is(equalTo(0L)));
        });

        long numDocs2 = randomIntBetween(2, 64);
        now = System.currentTimeMillis();
        indexDocs(logger, "data", numDocs2, now + 5000, now + 6000);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), is(equalTo(numDocs1 + numDocs2)));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), is(equalTo(0L)));
        }, 30, TimeUnit.SECONDS);
    }

    private static void expectThrowsUpgradeModeException(ThrowingRunnable actionInvocation) {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, actionInvocation);
        assertThat(e.getMessage(), containsString("upgrade mode is enabled"));
        assertThat(e.status(), is(equalTo(RestStatus.TOO_MANY_REQUESTS)));
    }
}
