/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for datafeed retry resilience during system-initiated reassignments.
 *
 * <p>Unit tests in {@link org.elasticsearch.xpack.ml.datafeed.DatafeedRunnerTests} cover retry
 * decision logic; these tests verify end-to-end behavior is unchanged for user starts and that
 * datafeeds recover after node failure (exercising the reassignment retry path).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DatafeedRetryResilienceIT extends BaseMlIntegTestCase {

    public void testNormalDatafeedStartStop_smokeTest() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);

        String jobId = "datafeed-retry-resilience-smoke-job";
        String datafeedId = jobId + "-datafeed";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        DatafeedConfig config = createDatafeed(datafeedId, jobId, Collections.singletonList("data"));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(config)).actionGet();

        ensureYellow();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L)).actionGet();

        assertBusy(() -> {
            GetDatafeedsStatsAction.Response stats = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(datafeedId)
            ).actionGet();
            assertThat(stats.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STARTED));
        }, 30, TimeUnit.SECONDS);

        client().execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId)).actionGet();
        client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId)).actionGet();

        assertBusy(() -> {
            GetJobsStatsAction.Response stats = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertThat(stats.getResponse().results().get(0).getState(), equalTo(JobState.CLOSED));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * When the node running a datafeed stops, the datafeed persistent task is reassigned and should
     * return to {@link DatafeedState#STARTED} on another node (retrying transient failures during the
     * STARTED state write on reassignment).
     */
    public void testDatafeedReopensAfterNodeFailure() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureStableCluster(2);

        String jobId = "datafeed-retry-resilience-failover-job";
        String datafeedId = jobId + "-datafeed";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        DatafeedConfig config = createDatafeed(datafeedId, jobId, Collections.singletonList("data"));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(config)).actionGet();

        ensureYellow();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L)).actionGet();

        String origNode = awaitJobOpenedAndAssigned(jobId, null);
        assertNotNull(origNode);
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response stats = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(datafeedId)
            ).actionGet();
            assertThat(stats.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STARTED));
        }, 30, TimeUnit.SECONDS);

        setMlIndicesDelayedNodeLeftTimeoutToZero();
        ensureGreen();

        internalCluster().stopNode(origNode);
        ensureStableCluster(1);

        awaitJobOpenedAndAssigned(jobId, null);
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response stats = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(datafeedId)
            ).actionGet();
            assertThat(stats.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STARTED));
        }, 2, TimeUnit.MINUTES);

        GetJobsStatsAction.Response jobStats = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
            .actionGet();
        assertThat(jobStats.getResponse().results().get(0).getState(), equalTo(JobState.OPENED));
    }
}
