/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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

        // Assert appropriate task state and assignment numbers
        assertThat(client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(MlTasks.JOB_TASK_NAME + "[c]", MlTasks.DATAFEED_TASK_NAME + "[c]")
            .get()
            .getTasks()
            .size(), equalTo(2));

        ClusterState masterClusterState = client().admin().cluster().prepareState().all().get().getState();

        PersistentTasksCustomMetaData persistentTasks = masterClusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        assertThat(persistentTasks.findTasks(MlTasks.DATAFEED_TASK_NAME, task -> true).size(), equalTo(1));
        assertThat(persistentTasks.findTasks(MlTasks.JOB_TASK_NAME, task -> true).size(), equalTo(1));
        assertThat(MlMetadata.getMlMetadata(masterClusterState).isUpgradeMode(), equalTo(false));

        // Set the upgrade mode setting
        AcknowledgedResponse response = client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(true))
            .actionGet();

        assertThat(response.isAcknowledged(), equalTo(true));

        masterClusterState = client().admin().cluster().prepareState().all().get().getState();

        // Assert state for tasks still exists and that the upgrade setting is set
        persistentTasks = masterClusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        assertThat(persistentTasks.findTasks(MlTasks.DATAFEED_TASK_NAME, task -> true).size(), equalTo(1));
        assertThat(persistentTasks.findTasks(MlTasks.JOB_TASK_NAME, task -> true).size(), equalTo(1));
        assertThat(MlMetadata.getMlMetadata(masterClusterState).isUpgradeMode(), equalTo(true));

        assertThat(client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(MlTasks.JOB_TASK_NAME + "[c]", MlTasks.DATAFEED_TASK_NAME + "[c]")
            .get()
            .getTasks(), is(empty()));

        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(jobId).get(0);
        assertThat(jobStats.getState(), equalTo(JobState.OPENED));
        assertThat(jobStats.getAssignmentExplanation(), equalTo(AWAITING_UPGRADE.getExplanation()));
        assertThat(jobStats.getNode(), is(nullValue()));

        GetDatafeedsStatsAction.Response.DatafeedStats datafeedStats = getDatafeedStats(datafeedId);
        assertThat(datafeedStats.getDatafeedState(), equalTo(DatafeedState.STARTED));
        assertThat(datafeedStats.getAssignmentExplanation(), equalTo(AWAITING_UPGRADE.getExplanation()));
        assertThat(datafeedStats.getNode(), is(nullValue()));

        Job.Builder job = createScheduledJob("job-should-not-open");
        registerJob(job);
        putJob(job);
        ElasticsearchStatusException statusException = expectThrows(ElasticsearchStatusException.class, () -> openJob(job.getId()));
        assertThat(statusException.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(statusException.getMessage(), equalTo("Cannot open jobs when upgrade mode is enabled"));

        //Disable the setting
        response = client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(false))
            .actionGet();

        assertThat(response.isAcknowledged(), equalTo(true));

        masterClusterState = client().admin().cluster().prepareState().all().get().getState();

        persistentTasks = masterClusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        assertThat(persistentTasks.findTasks(MlTasks.DATAFEED_TASK_NAME, task -> true).size(), equalTo(1));
        assertThat(persistentTasks.findTasks(MlTasks.JOB_TASK_NAME, task -> true).size(), equalTo(1));
        assertThat(MlMetadata.getMlMetadata(masterClusterState).isUpgradeMode(), equalTo(false));

        assertBusy(() -> assertThat(client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(MlTasks.JOB_TASK_NAME + "[c]", MlTasks.DATAFEED_TASK_NAME + "[c]")
            .get()
            .getTasks()
            .size(), equalTo(2)));

        jobStats = getJobStats(jobId).get(0);
        assertThat(jobStats.getState(), equalTo(JobState.OPENED));
        assertThat(jobStats.getAssignmentExplanation(), not(equalTo(AWAITING_UPGRADE.getExplanation())));

        datafeedStats = getDatafeedStats(datafeedId);
        assertThat(datafeedStats.getDatafeedState(), equalTo(DatafeedState.STARTED));
        assertThat(datafeedStats.getAssignmentExplanation(), not(equalTo(AWAITING_UPGRADE.getExplanation())));
    }

    private void startRealtime(String jobId) throws Exception {
        client().admin().indices().prepareCreate("data")
            .addMapping("type", "time", "type=date")
            .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long lastWeek = now - 604800000;
        indexDocs(logger, "data", numDocs1, lastWeek, now);

        Job.Builder job = createScheduledJob(jobId);
        registerJob(job);
        putJob(job);
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data"));
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, null);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs1));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        });

        long numDocs2 = randomIntBetween(2, 64);
        now = System.currentTimeMillis();
        indexDocs(logger, "data", numDocs2, now + 5000, now + 6000);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs1 + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        }, 30, TimeUnit.SECONDS);
    }

}
