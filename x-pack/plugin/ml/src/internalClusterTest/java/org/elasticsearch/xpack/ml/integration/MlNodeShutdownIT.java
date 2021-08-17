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
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
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

        // Index some source data for the datafeeds.
        createSourceData();

        // Open 6 jobs.  Since there are 3 nodes in the cluster we should get 2 jobs per node.
        setupJobAndDatafeed("shutdown-job-1", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-2", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-3", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-4", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-5", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-job-6", ByteSizeValue.ofMb(2));

        // Choose a node to shut down.  Choose a non-master node most of the time, as ML nodes in Cloud
        // will never be master, and Cloud is where the node shutdown API will primarily be used.
        String nodeNameToShutdown = rarely() ? internalCluster().getMasterName() : Arrays.stream(internalCluster().getNodeNames())
            .filter(nodeName -> internalCluster().getMasterName().equals(nodeName) == false).findFirst().get();
        SetOnce<String> nodeIdToShutdown = new SetOnce<>();

        // Wait for the desired initial state of 2 jobs running on each node.
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

        // Call the shutdown API for the chosen node.
        client().execute(
            PutShutdownNodeAction.INSTANCE,
            new PutShutdownNodeAction.Request(
                nodeIdToShutdown.get(),
                randomFrom(SingleNodeShutdownMetadata.Type.values()),
                "just testing",
                null
            )
        ).actionGet();

        // Wait for the desired end state of all 6 jobs running on nodes that are not shutting down.
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
        }, 30, TimeUnit.SECONDS);
    }

    public void testCloseJobVacatingShuttingDownNode() throws Exception {

        // TODO: delete this condition when the shutdown API is always available
        assumeTrue("shutdown API is behind a snapshot-only feature flag", Build.CURRENT.isSnapshot());

        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster();

        // Index some source data for the datafeeds.
        createSourceData();

        // Open 6 jobs.  Since there are 3 nodes in the cluster we should get 2 jobs per node.
        setupJobAndDatafeed("shutdown-close-job-1", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-close-job-2", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-close-job-3", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-close-job-4", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-close-job-5", ByteSizeValue.ofMb(2));
        setupJobAndDatafeed("shutdown-close-job-6", ByteSizeValue.ofMb(2));

        // Choose a node to shut down, and one job on that node to close after the shutdown request has been sent.
        // Choose a non-master node most of the time, as ML nodes in Cloud will never be master, and Cloud is where
        // the node shutdown API will primarily be used.
        String nodeNameToShutdown = rarely() ? internalCluster().getMasterName() : Arrays.stream(internalCluster().getNodeNames())
            .filter(nodeName -> internalCluster().getMasterName().equals(nodeName) == false).findFirst().get();
        SetOnce<String> nodeIdToShutdown = new SetOnce<>();
        SetOnce<String> jobIdToClose = new SetOnce<>();

        // Wait for the desired initial state of 2 jobs running on each node.
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
            jobIdToClose.set(jobStats.results().stream()
                .filter(stats -> stats.getNode() != null && nodeNameToShutdown.equals(stats.getNode().getName()))
                .map(GetJobsStatsAction.Response.JobStats::getJobId).findAny().get());
        });

        // Call the shutdown API for the chosen node.
        client().execute(
            PutShutdownNodeAction.INSTANCE,
            new PutShutdownNodeAction.Request(
                nodeIdToShutdown.get(),
                randomFrom(SingleNodeShutdownMetadata.Type.values()),
                "just testing",
                null
            )
        )
            .actionGet();

        if (randomBoolean()) {
            // This isn't waiting for something to happen - just adding timing variation
            // to increase the chance of subtle race conditions occurring.
            Thread.sleep(randomIntBetween(1, 10));
        }

        // There are several different scenarios for this request:
        // 1. It might arrive at the original node that is shutting down before the job has transitioned into the
        //    vacating state.  Then it's just a normal close that node shut down should not interfere with.
        // 2. It might arrive at the original node that is shutting down while the job is vacating, but early enough
        //    that the vacate can be promoted to a close (since the early part of the work they do is the same).
        // 3. It might arrive at the original node that is shutting down while the job is vacating, but too late
        //    to promote the vacate to a close (since the request to unassign the persistent task has already been
        //    sent to the master node).  In this case fallback code in the job task should delete the persistent
        //    task to effectively force-close the job on its new node.
        // 4. It might arrive after the job has been unassigned from its original node after vacating but before it's
        //    been assigned to a new node.  In this case the close job action will delete the persistent task.
        // 5. It might arrive after the job has been assigned to its new node.  In this case it's just a normal close
        //    on a node that isn't even shutting down.
        client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobIdToClose.get())).actionGet();

        // Wait for the desired end state of the 5 jobs that were not closed running on nodes that are not shutting
        // down, and the closed job not running anywhere.
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
            assertThat(numJobsOnOtherNodes, is(5L)); // 5 rather than 6 because we closed one
        }, 30, TimeUnit.SECONDS);
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

    private void createSourceData() {
        client().admin().indices().prepareCreate("data")
            .setMapping("time", "type=date")
            .get();
        long numDocs = randomIntBetween(50, 100);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs, twoWeeksAgo, weekAgo);
    }
}
