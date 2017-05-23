/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.xpack.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.getDataCounts;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.indexDocs;
import static org.hamcrest.Matchers.equalTo;

public class DatafeedJobsIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanup() throws Exception {
        cleanUp();
    }

    public void testLookbackOnly() throws Exception {
        client().admin().indices().prepareCreate("data-1")
                .addMapping("type", "time", "type=date")
                .get();
        long numDocs = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "data-1", numDocs, twoWeeksAgo, oneWeekAgo);

        client().admin().indices().prepareCreate("data-2")
                .addMapping("type", "time", "type=date")
                .get();
        ClusterHealthResponse r = client().admin().cluster().prepareHealth("data-1", "data-2").setWaitForYellowStatus().get();
        long numDocs2 = randomIntBetween(32, 2048);
        indexDocs(logger, "data-2", numDocs2, oneWeekAgo, now);

        Job.Builder job = createScheduledJob("lookback-job");
        registerJob(job);
        PutJobAction.Response putJobResponse = putJob(job);
        assertTrue(putJobResponse.isAcknowledged());
        assertThat(putJobResponse.getResponse().getJobVersion(), equalTo(Version.CURRENT));
        openJob(job.getId());
        assertBusy(() -> {
            assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED);
        });

        List<String> t = new ArrayList<>(2);
        t.add("data-1");
        t.add("data-2");
        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), t);
        registerDatafeed(datafeedConfig);
        assertTrue(putDatafeed(datafeedConfig).isAcknowledged());

        startDatafeed(datafeedConfig.getId(), 0L, now);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));

            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedConfig.getId());
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        }, 60, TimeUnit.SECONDS);

        waitUntilJobIsClosed(job.getId());
    }

    public void testRealtime() throws Exception {
        String jobId = "realtime-job";
        String datafeedId = jobId + "-datafeed";
        startRealtime(jobId);

        try {
            StopDatafeedAction.Response stopJobResponse = stopDatafeed(datafeedId);
            assertTrue(stopJobResponse.isStopped());
        } catch (Exception e) {
            NodesHotThreadsResponse nodesHotThreadsResponse = client().admin().cluster().prepareNodesHotThreads().get();
            int i = 0;
            for (NodeHotThreads nodeHotThreads : nodesHotThreadsResponse.getNodes()) {
                logger.info(i++ + ":\n" +nodeHotThreads.getHotThreads());
            }
            throw e;
        }
        assertBusy(() -> {
            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
    }

    public void testRealtime_multipleStopCalls() throws Exception {
        String jobId = "realtime-job-multiple-stop";
        final String datafeedId = jobId + "-datafeed";
        startRealtime(jobId);

        ConcurrentMapLong<AssertionError> exceptions = ConcurrentCollections.newConcurrentMapLong();

        // It's practically impossible to assert that a stop request has waited
        // for a concurrently executing request to finish before returning.
        // But we can assert the data feed has stopped after the request returns.
        Runnable stopDataFeed = () -> {
            StopDatafeedAction.Response stopJobResponse = stopDatafeed(datafeedId);
            if (stopJobResponse.isStopped() == false) {
                exceptions.put(Thread.currentThread().getId(), new AssertionError("Job is not stopped"));
            }

            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            if (response.getResponse().results().get(0).getDatafeedState() != DatafeedState.STOPPED) {
                exceptions.put(Thread.currentThread().getId(),
                        new AssertionError("Expected STOPPED datafeed state got "
                                + response.getResponse().results().get(0).getDatafeedState()));
            }
        };

        // The idea is to hit the situation where one request waits for
        // the other to complete. This is difficult to schedule but
        // hopefully it will happen in CI
        int numThreads = 5;
        Thread [] threads = new Thread[numThreads];
        for (int i=0; i<numThreads; i++) {
            threads[i] = new Thread(stopDataFeed);
        }
        for (int i=0; i<numThreads; i++) {
            threads[i].start();
        }
        for (int i=0; i<numThreads; i++) {
            threads[i].join();
        }

        if (exceptions.isEmpty() == false) {
            throw exceptions.values().iterator().next();
        }
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
        assertTrue(putJob(job).isAcknowledged());
        openJob(job.getId());
        assertBusy(() -> {
            assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED);
        });

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data"));
        registerDatafeed(datafeedConfig);
        assertTrue(putDatafeed(datafeedConfig).isAcknowledged());

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
