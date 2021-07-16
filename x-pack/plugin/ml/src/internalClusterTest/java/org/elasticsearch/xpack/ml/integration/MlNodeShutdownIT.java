/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Build;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MlNodeShutdownIT extends BaseMlIntegTestCase {

    public void testJobsVacateShuttingDownNode() throws Exception {

        // TODO: delete this condition when the shutdown API is always available
        assumeTrue("shutdown API is behind a snapshot-only feature flag", Build.CURRENT.isSnapshot());

        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster();

        // index some datafeed data
        client().admin().indices().prepareCreate("data")
            .setMapping("time", "type=date")
            .get();
        long numDocs1 = randomIntBetween(50, 100);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        // Open 6 jobs.  Since there are 3 nodes in the cluster we should get 2 jobs per node.

        setupJobAndDatafeed("shutdown-job-1", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-2", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-3", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-4", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-5", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-6", ByteSizeValue.ofMb(2));

        // Call the shutdown API for one non-master node.
        String nodeNameToShutdown = Arrays.stream(internalCluster().getNodeNames())
            .filter(nodeName -> internalCluster().getMasterName().equals(nodeName) == false).findFirst().get();
        SetOnce<String> nodeIdToShutdown = new SetOnce<>();

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(Metadata.ALL)).actionGet();
            QueryPage<GetJobsStatsAction.Response.JobStats> jobStats = statsResponse.getResponse();
            assertThat(jobStats, notNullValue());
            long numJobsOnNodeToShutdown = jobStats.results().stream()
                .filter(stats -> stats.getNode() != null && nodeNameToShutdown.equals(stats.getNode().getName())).count();
            long numJobsOnOtherNodes = jobStats.results().stream()
                .filter(stats -> stats.getNode() != null && nodeNameToShutdown.equals(stats.getNode().getName()) == false).count();
            assertThat(numJobsOnNodeToShutdown, is(2L));
            assertThat(numJobsOnOtherNodes, is(4L));
            nodeIdToShutdown.set(jobStats.results().stream()
                .filter(stats -> stats.getNode() != null && nodeNameToShutdown.equals(stats.getNode().getName()))
                .map(stats -> stats.getNode().getId()).findFirst().get());
        });

        client().execute(PutShutdownNodeAction.INSTANCE,
            new PutShutdownNodeAction.Request(nodeIdToShutdown.get(), randomFrom(SingleNodeShutdownMetadata.Type.values()), "just testing"))
            .actionGet();

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(Metadata.ALL)).actionGet();
            QueryPage<GetJobsStatsAction.Response.JobStats> jobStats = statsResponse.getResponse();
            assertThat(jobStats, notNullValue());
            long numJobsOnNodeToShutdown = jobStats.results().stream()
                .filter(stats -> stats.getNode() != null && nodeNameToShutdown.equals(stats.getNode().getName())).count();
            long numJobsOnOtherNodes = jobStats.results().stream()
                .filter(stats -> stats.getNode() != null && nodeNameToShutdown.equals(stats.getNode().getName()) == false).count();
            assertThat(numJobsOnNodeToShutdown, is(0L));
            assertThat(numJobsOnOtherNodes, is(6L));
        });
    }

    private void setupJobAndDatafeed(String jobId, ByteSizeValue modelMemoryLimit) throws Exception {
        Job.Builder job = createScheduledJob(jobId, modelMemoryLimit);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        String datafeedId = jobId;
        DatafeedConfig config = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        }, 30, TimeUnit.SECONDS);

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
    }

    private void ensureStableCluster() {
        ensureStableCluster(internalCluster().getNodeNames().length, TimeValue.timeValueSeconds(60));
    }
}
