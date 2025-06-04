/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.junit.After;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeedBuilder;
import static org.hamcrest.CoreMatchers.equalTo;

public class JobAndDatafeedResilienceIT extends MlNativeAutodetectIntegTestCase {

    private final String index = "empty_index";

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void testCloseOpenJobWithMissingConfig() throws Exception {
        final String jobId = "job-with-missing-config";
        Job.Builder job = createJob(jobId, TimeValue.timeValueMinutes(5), "count", null);

        putJob(job);
        openJob(job.getId());

        client().prepareDelete(MlConfigIndex.indexName(), Job.documentId(jobId)).get();
        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> {
            CloseJobAction.Request request = new CloseJobAction.Request(jobId);
            request.setAllowNoMatch(false);
            client().execute(CloseJobAction.INSTANCE, request).actionGet();
        });
        assertThat(ex.getMessage(), equalTo("No known job with id 'job-with-missing-config'"));

        forceCloseJob(jobId);
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareListTasks().setActions(MlTasks.JOB_TASK_NAME + "[c]").get().getTasks().size(),
                equalTo(0)
            )
        );
    }

    public void testStopStartedDatafeedWithMissingConfig() throws Exception {
        client().admin().indices().prepareCreate(index).setMapping("time", "type=date", "value", "type=long").get();
        final String jobId = "job-with-missing-datafeed-with-config";
        Job.Builder job = createJob(jobId, TimeValue.timeValueMinutes(5), "count", null);

        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilder(
            job.getId() + "-datafeed",
            job.getId(),
            Collections.singletonList(index)
        );
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();

        putJob(job);
        openJob(job.getId());

        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, null);

        client().prepareDelete(MlConfigIndex.indexName(), DatafeedConfig.documentId(datafeedConfig.getId())).get();
        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> {
            StopDatafeedAction.Request request = new StopDatafeedAction.Request(datafeedConfig.getId());
            request.setAllowNoMatch(false);
            client().execute(StopDatafeedAction.INSTANCE, request).actionGet();
        });
        assertThat(ex.getMessage(), equalTo("No datafeed with id [job-with-missing-datafeed-with-config-datafeed] exists"));

        forceStopDatafeed(datafeedConfig.getId());
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareListTasks().setActions(MlTasks.DATAFEED_TASK_NAME + "[c]").get().getTasks().size(),
                equalTo(0)
            )
        );
        closeJob(jobId);
        waitUntilJobIsClosed(jobId);
    }

    public void testGetJobStats() throws Exception {
        final String jobId1 = "job-with-missing-config-stats";
        final String jobId2 = "job-with-config-stats";

        Job.Builder job1 = createJob(jobId1, TimeValue.timeValueMinutes(5), "count", null);
        Job.Builder job2 = createJob(jobId2, TimeValue.timeValueMinutes(5), "count", null);

        putJob(job1);
        openJob(job1.getId());
        putJob(job2);
        openJob(job2.getId());

        client().prepareDelete(MlConfigIndex.indexName(), Job.documentId(jobId1)).get();
        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        List<GetJobsStatsAction.Response.JobStats> jobStats = client().execute(
            GetJobsStatsAction.INSTANCE,
            new GetJobsStatsAction.Request("*")
        ).get().getResponse().results();
        assertThat(jobStats.size(), equalTo(2));
        assertThat(jobStats.get(0).getJobId(), equalTo(jobId2));
        assertThat(jobStats.get(1).getJobId(), equalTo(jobId1));
        forceCloseJob(jobId1);
        closeJob(jobId2);
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareListTasks().setActions(MlTasks.JOB_TASK_NAME + "[c]").get().getTasks().size(),
                equalTo(0)
            )
        );
    }

    public void testGetDatafeedStats() throws Exception {
        client().admin().indices().prepareCreate(index).setMapping("time", "type=date", "value", "type=long").get();
        final String jobId1 = "job-with-datafeed-missing-config-stats";
        final String jobId2 = "job-with-datafeed-config-stats";

        Job.Builder job1 = createJob(jobId1, TimeValue.timeValueMinutes(5), "count", null);
        Job.Builder job2 = createJob(jobId2, TimeValue.timeValueMinutes(5), "count", null);

        putJob(job1);
        openJob(job1.getId());
        putJob(job2);
        openJob(job2.getId());

        DatafeedConfig.Builder datafeedConfigBuilder1 = createDatafeedBuilder(
            job1.getId() + "-datafeed",
            job1.getId(),
            Collections.singletonList(index)
        );
        DatafeedConfig datafeedConfig1 = datafeedConfigBuilder1.build();

        putDatafeed(datafeedConfig1);
        startDatafeed(datafeedConfig1.getId(), 0L, null);

        DatafeedConfig.Builder datafeedConfigBuilder2 = createDatafeedBuilder(
            job2.getId() + "-datafeed",
            job2.getId(),
            Collections.singletonList(index)
        );
        DatafeedConfig datafeedConfig2 = datafeedConfigBuilder2.build();

        putDatafeed(datafeedConfig2);
        startDatafeed(datafeedConfig2.getId(), 0L, null);

        client().prepareDelete(MlConfigIndex.indexName(), DatafeedConfig.documentId(datafeedConfig1.getId())).get();
        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        List<GetDatafeedsStatsAction.Response.DatafeedStats> dfStats = client().execute(
            GetDatafeedsStatsAction.INSTANCE,
            new GetDatafeedsStatsAction.Request("*")
        ).get().getResponse().results();
        assertThat(dfStats.size(), equalTo(2));
        assertThat(dfStats.get(0).getDatafeedId(), equalTo(datafeedConfig2.getId()));
        assertThat(dfStats.get(1).getDatafeedId(), equalTo(datafeedConfig1.getId()));

        forceStopDatafeed(datafeedConfig1.getId());
        stopDatafeed(datafeedConfig2.getId());
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareListTasks().setActions(MlTasks.DATAFEED_TASK_NAME + "[c]").get().getTasks().size(),
                equalTo(0)
            )
        );
        closeJob(jobId1);
        closeJob(jobId2);
        waitUntilJobIsClosed(jobId1);
        waitUntilJobIsClosed(jobId2);
    }

    private CloseJobAction.Response forceCloseJob(String jobId) {
        CloseJobAction.Request request = new CloseJobAction.Request(jobId);
        request.setForce(true);
        return client().execute(CloseJobAction.INSTANCE, request).actionGet();
    }

    private StopDatafeedAction.Response forceStopDatafeed(String datafeedId) {
        StopDatafeedAction.Request request = new StopDatafeedAction.Request(datafeedId);
        request.setForce(true);
        return client().execute(StopDatafeedAction.INSTANCE, request).actionGet();
    }

    private Job.Builder createJob(String id, TimeValue bucketSpan, String function, String field) {
        return createJob(id, bucketSpan, function, field, null);
    }

    private Job.Builder createJob(String id, TimeValue bucketSpan, String function, String field, String summaryCountField) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);

        Detector.Builder d = new Detector.Builder(function, field);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        analysisConfig.setSummaryCountFieldName(summaryCountField);

        Job.Builder builder = new Job.Builder();
        builder.setId(id);
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
    }

}
