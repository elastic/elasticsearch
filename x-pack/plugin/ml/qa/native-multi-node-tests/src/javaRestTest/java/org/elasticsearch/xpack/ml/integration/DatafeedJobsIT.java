/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.hamcrest.Matcher;
import org.junit.After;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeedBuilder;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.getDataCounts;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.indexDocs;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class DatafeedJobsIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testLookbackOnly() throws Exception {
        client().admin().indices().prepareCreate("data-1")
            .setMapping("time", "type=date")
            .get();
        long numDocs = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "data-1", numDocs, twoWeeksAgo, oneWeekAgo);

        client().admin().indices().prepareCreate("data-2")
            .setMapping("time", "type=date")
            .get();
        client().admin().cluster().prepareHealth("data-1", "data-2").setWaitForYellowStatus().get();
        long numDocs2 = randomIntBetween(32, 2048);
        indexDocs(logger, "data-2", numDocs2, oneWeekAgo, now);

        Job.Builder job = createScheduledJob("lookback-job");
        registerJob(job);
        PutJobAction.Response putJobResponse = putJob(job);
        assertThat(putJobResponse.getResponse().getJobVersion(), equalTo(Version.CURRENT));
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        List<String> t = new ArrayList<>(2);
        t.add("data-1");
        t.add("data-2");
        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), t);
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);

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

    public void testLookbackOnlyDataStream() throws Exception {
        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"time\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }," +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }" +
            "      }\n" +
            "    }";
        createDataStreamAndTemplate("datafeed_data_stream", mapping);
        long numDocs = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "datafeed_data_stream", numDocs, twoWeeksAgo, oneWeekAgo);

        client().admin().cluster().prepareHealth("datafeed_data_stream").setWaitForYellowStatus().get();

        Job.Builder job = createScheduledJob("lookback-data-stream-job");
        registerJob(job);
        PutJobAction.Response putJobResponse = putJob(job);
        assertThat(putJobResponse.getResponse().getJobVersion(), equalTo(Version.CURRENT));
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed",
            job.getId(),
            Collections.singletonList("datafeed_data_stream"));
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);

        startDatafeed(datafeedConfig.getId(), 0L, now);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));

            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedConfig.getId());
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        }, 60, TimeUnit.SECONDS);

        waitUntilJobIsClosed(job.getId());
    }

    public void testDatafeedTimingStats_DatafeedRecreated() throws Exception {
        client().admin().indices().prepareCreate("data")
            .setMapping("time", "type=date")
            .get();
        long numDocs = randomIntBetween(32, 2048);
        Instant now = Instant.now();
        indexDocs(logger, "data", numDocs, now.minus(Duration.ofDays(14)).toEpochMilli(), now.toEpochMilli());

        Job.Builder job = createScheduledJob("lookback-job-datafeed-recreated");

        String datafeedId = "lookback-datafeed-datafeed-recreated";
        DatafeedConfig datafeedConfig = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"));

        registerJob(job);
        putJob(job);

        CheckedRunnable<Exception> openAndRunJob = () -> {
            openJob(job.getId());
            assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));
            registerDatafeed(datafeedConfig);
            putDatafeed(datafeedConfig);
            // Datafeed did not do anything yet, hence search_count is equal to 0.
            assertDatafeedStats(datafeedId, DatafeedState.STOPPED, job.getId(), equalTo(0L));
            startDatafeed(datafeedId, 0L, now.toEpochMilli());
            assertBusy(() -> {
                assertThat(getDataCounts(job.getId()).getProcessedRecordCount(), equalTo(numDocs));
                // Datafeed processed numDocs documents so search_count must be greater than 0.
                assertDatafeedStats(datafeedId, DatafeedState.STOPPED, job.getId(), greaterThan(0L));
            }, 60, TimeUnit.SECONDS);
            deleteDatafeed(datafeedId);
            waitUntilJobIsClosed(job.getId());
        };

        openAndRunJob.run();
        openAndRunJob.run();
    }

    public void testDatafeedTimingStats_QueryDelayUpdated_TimingStatsNotReset() throws Exception {
        client().admin().indices().prepareCreate("data")
            .setMapping("time", "type=date")
            .get();
        long numDocs = randomIntBetween(32, 2048);
        Instant now = Instant.now();
        indexDocs(logger, "data", numDocs, now.minus(Duration.ofDays(14)).toEpochMilli(), now.toEpochMilli());

        Job.Builder job = createScheduledJob("lookback-job-query-delay-updated");
        registerJob(job);
        putJob(job);

        String datafeedId = "lookback-datafeed-query-delay-updated";
        DatafeedConfig datafeedConfig = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"));
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);

        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));
        // Datafeed did not do anything yet, hence search_count is equal to 0.
        assertDatafeedStats(datafeedId, DatafeedState.STOPPED, job.getId(), equalTo(0L));
        startDatafeed(datafeedId, 0L, now.toEpochMilli());
        assertBusy(() -> {
            assertThat(getDataCounts(job.getId()).getProcessedRecordCount(), equalTo(numDocs));
            // Datafeed processed numDocs documents so search_count must be greater than 0.
            assertDatafeedStats(datafeedId, DatafeedState.STOPPED, job.getId(), greaterThan(0L));
        }, 60, TimeUnit.SECONDS);
        waitUntilJobIsClosed(job.getId());

        // Change something different than jobId, here: queryDelay.
        updateDatafeed(new DatafeedUpdate.Builder(datafeedId).setQueryDelay(TimeValue.timeValueSeconds(777)).build());
        // Search_count is still greater than 0 (i.e. has not been reset by datafeed update)
        assertDatafeedStats(datafeedId, DatafeedState.STOPPED, job.getId(), greaterThan(0L));
    }

    private void assertDatafeedStats(String datafeedId, DatafeedState state, String jobId, Matcher<Long> searchCountMatcher) {
        GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
        assertThat(response.getResponse().results(), hasSize(1));
        GetDatafeedsStatsAction.Response.DatafeedStats stats = response.getResponse().results().get(0);
        assertThat(stats.getDatafeedState(), equalTo(state));
        assertThat(stats.getTimingStats().getJobId(), equalTo(jobId));
        assertThat(stats.getTimingStats().getSearchCount(), searchCountMatcher);
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

    public void testRealtime_noDataAndAutoStop() throws Exception {
        String jobId = "realtime-job-auto-stop";
        String datafeedId = jobId + "-datafeed";
        startRealtime(jobId, randomIntBetween(1, 3));

        // Datafeed should auto-stop...
        assertBusy(() -> {
            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });

        // ...and should have auto-closed the job too
        assertBusy(() -> {
            GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
            GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getState(), equalTo(JobState.CLOSED));
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

    public void testRealtime_givenSimultaneousStopAndForceDelete() throws Throwable {
        String jobId = "realtime-job-stop-and-force-delete";
        final String datafeedId = jobId + "-datafeed";
        startRealtime(jobId);

        AtomicReference<Throwable> exception = new AtomicReference<>();

        // The UI now force deletes datafeeds, which means they can be deleted while running.
        // The first step is to isolate the datafeed.  But if it was already being stopped then
        // the datafeed may not be running by the time the isolate action is executed.  This
        // test will sometimes (depending on thread scheduling) achieve this situation and ensure
        // the code is robust to it.
        Thread deleteDatafeedThread = new Thread(() -> {
            try {
                DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request(datafeedId);
                request.setForce(true);
                AcknowledgedResponse response = client().execute(DeleteDatafeedAction.INSTANCE, request).actionGet();
                if (response.isAcknowledged()) {
                    GetDatafeedsStatsAction.Request statsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
                    expectThrows(ResourceNotFoundException.class,
                            () -> client().execute(GetDatafeedsStatsAction.INSTANCE, statsRequest).actionGet());
                } else {
                    exception.set(new AssertionError("Job is not deleted"));
                }
            } catch (AssertionError | Exception e) {
                exception.set(e);
            }
        });
        deleteDatafeedThread.start();

        try {
            stopDatafeed(datafeedId);
        } catch (ResourceNotFoundException e) {
            // This is OK - it means the thread running the delete fully completed before the stop started to execute
        } finally {
            deleteDatafeedThread.join();
        }

        if (exception.get() != null) {
            throw exception.get();
        }
    }

    public void testRealtime_GivenProcessIsKilled() throws Exception {
        String jobId = "realtime-job-given-process-is-killed";
        String datafeedId = jobId + "-datafeed";
        startRealtime(jobId);

        KillProcessAction.Request killRequest = new KillProcessAction.Request(jobId);
        client().execute(KillProcessAction.INSTANCE, killRequest).actionGet();

        assertBusy(() -> {
            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
    }

    /**
     * Stopping a lookback closes the associated job _after_ the stop call returns.
     * This test ensures that a kill request submitted during this close doesn't
     * put the job into the "failed" state.
     */
    public void testStopLookbackFollowedByProcessKill() throws Exception {
        client().admin().indices().prepareCreate("data")
                .setMapping("time", "type=date")
                .get();
        long numDocs = randomIntBetween(1024, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "data", numDocs, twoWeeksAgo, oneWeekAgo);

        Job.Builder job = createScheduledJob("lookback-job-stopped-then-killed");
        registerJob(job);
        PutJobAction.Response putJobResponse = putJob(job);
        assertThat(putJobResponse.getResponse().getJobVersion(), equalTo(Version.CURRENT));
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        List<String> t = Collections.singletonList("data");
        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilder(job.getId() + "-datafeed", job.getId(), t);
        // Use lots of chunks so we have time to stop the lookback before it completes
        datafeedConfigBuilder.setChunkingConfig(ChunkingConfig.newManual(new TimeValue(1, TimeUnit.SECONDS)));
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, now);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), greaterThan(0L));
        }, 60, TimeUnit.SECONDS);

        stopDatafeed(datafeedConfig.getId());

        // At this point, stopping the datafeed will have submitted a request for the job to close.
        // Depending on thread scheduling, the following kill request might overtake it.  The Thread.sleep()
        // call here makes it more likely; to make it inevitable for testing also add a Thread.sleep(10)
        // immediately before the checkProcessIsAlive() call in AutodetectCommunicator.close().
        Thread.sleep(randomIntBetween(1, 9));

        KillProcessAction.Request killRequest = new KillProcessAction.Request(job.getId());
        client().execute(KillProcessAction.INSTANCE, killRequest).actionGet();

        // This should close very quickly, as we killed the process.  If the job goes into the "failed"
        // state that's wrong and this test will fail.
        waitUntilJobIsClosed(job.getId(), TimeValue.timeValueSeconds(2));
    }

    private void startRealtime(String jobId) throws Exception {
        startRealtime(jobId, null);
    }

    private void startRealtime(String jobId, Integer maxEmptySearches) throws Exception {
        client().admin().indices().prepareCreate("data")
                .setMapping("time", "type=date")
                .get();
        long now = System.currentTimeMillis();
        long numDocs1;
        if (maxEmptySearches == null) {
            numDocs1 = randomIntBetween(32, 2048);
            long lastWeek = now - 604800000;
            indexDocs(logger, "data", numDocs1, lastWeek, now);
        } else {
            numDocs1 = 0;
        }

        Job.Builder job = createScheduledJob(jobId);
        registerJob(job);
        putJob(job);
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig.Builder datafeedConfigBuilder =
            createDatafeedBuilder(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data"));
        if (maxEmptySearches != null) {
            datafeedConfigBuilder.setMaxEmptySearches(maxEmptySearches);
        }
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, null);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs1));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        });

        now = System.currentTimeMillis();
        long numDocs2;
        if (maxEmptySearches == null) {
            numDocs2 = randomIntBetween(2, 64);
            indexDocs(logger, "data", numDocs2, now + 5000, now + 6000);
        } else {
            numDocs2 = 0;
        }

        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs1 + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        }, 30, TimeUnit.SECONDS);
    }
}
