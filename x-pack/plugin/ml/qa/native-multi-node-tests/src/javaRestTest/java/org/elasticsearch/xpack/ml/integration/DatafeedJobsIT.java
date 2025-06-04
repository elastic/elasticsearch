/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Level;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.hamcrest.Matcher;
import org.junit.After;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeedBuilder;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.getDataCounts;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.indexDocs;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class DatafeedJobsIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanup() {
        updateClusterSettings(Settings.builder().putNull("logger.org.elasticsearch.xpack.ml.datafeed"));
        cleanUp();
        // Race conditions between closing and killing tasks in these tests,
        // sometimes result in lingering persistent close tasks, which cause
        // subsequent tests to fail. Therefore, they're explicitly cancelled.
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setActions("*close*");
        cancelTasksRequest.setWaitForCompletion(true);
        client().execute(TransportCancelTasksAction.TYPE, cancelTasksRequest).actionGet();
    }

    public void testLookbackOnly() throws Exception {
        client().admin().indices().prepareCreate("data-1").setMapping("time", "type=date").get();
        long numDocs = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "data-1", numDocs, twoWeeksAgo, oneWeekAgo);

        client().admin().indices().prepareCreate("data-2").setMapping("time", "type=date").get();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, "data-1", "data-2").setWaitForYellowStatus().get();
        long numDocs2 = randomIntBetween(32, 2048);
        indexDocs(logger, "data-2", numDocs2, oneWeekAgo, now);

        Job.Builder job = createScheduledJob("lookback-job");
        PutJobAction.Response putJobResponse = putJob(job);
        assertThat(putJobResponse.getResponse().getJobVersion(), equalTo(MlConfigVersion.CURRENT));
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        // Having a pattern with missing indices is acceptable
        List<String> indices = Arrays.asList("data-*", "missing-*");
        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), indices);
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
        String mapping = """
            {
                  "properties": {
                    "time": {
                      "type": "date"
                    },        "@timestamp": {
                      "type": "date"
                    }      }
                }""";
        createDataStreamAndTemplate("datafeed_data_stream", mapping);
        long numDocs = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "datafeed_data_stream", numDocs, twoWeeksAgo, oneWeekAgo);

        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, "datafeed_data_stream").setWaitForYellowStatus().get();

        Job.Builder job = createScheduledJob("lookback-data-stream-job");
        PutJobAction.Response putJobResponse = putJob(job);
        assertThat(putJobResponse.getResponse().getJobVersion(), equalTo(MlConfigVersion.CURRENT));
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig datafeedConfig = createDatafeed(
            job.getId() + "-datafeed",
            job.getId(),
            Collections.singletonList("datafeed_data_stream")
        );
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

    public void testLookbackOnlyRuntimeMapping() throws Exception {
        client().admin().indices().prepareCreate("data-1").setMapping("time", "type=date").get();
        long numDocs = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "data-1", numDocs, twoWeeksAgo, oneWeekAgo);

        DataDescription.Builder dataDescription = new DataDescription.Builder();

        Detector.Builder d = new Detector.Builder("count", null);
        // day_of_week is a runtime field.
        // count by day of week does not make much sense but is good enough for this test
        d.setByFieldName("day_of_week");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));

        Job.Builder jobBuilder = new Job.Builder();
        jobBuilder.setId("lookback-job-with-rt-fields");
        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);

        putJob(jobBuilder);
        openJob(jobBuilder.getId());
        assertBusy(() -> assertEquals(getJobStats(jobBuilder.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig.Builder dfBuilder = new DatafeedConfig.Builder(jobBuilder.getId() + "-datafeed", jobBuilder.getId());
        dfBuilder.setQueryDelay(TimeValue.timeValueSeconds(1));
        dfBuilder.setFrequency(TimeValue.timeValueSeconds(1));
        dfBuilder.setIndices(Collections.singletonList("data-1"));

        Map<String, Object> properties = new HashMap<>();
        properties.put("type", "long");
        properties.put("script", "emit(doc['time'].value.getDayOfWeekEnum().value)");
        Map<String, Object> fields = new HashMap<>();
        fields.put("day_of_week", properties);
        dfBuilder.setRuntimeMappings(fields);

        DatafeedConfig datafeedConfig = dfBuilder.build();

        putDatafeed(datafeedConfig);

        startDatafeed(datafeedConfig.getId(), 0L, now);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(jobBuilder.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
            assertThat(dataCounts.getMissingFieldCount(), equalTo(0L));

            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedConfig.getId());
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        }, 60, TimeUnit.SECONDS);

        waitUntilJobIsClosed(jobBuilder.getId());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/63973")
    public void testDatafeedTimingStats_DatafeedRecreated() throws Exception {
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs = randomIntBetween(32, 2048);
        Instant now = Instant.now();
        indexDocs(logger, "data", numDocs, now.minus(Duration.ofDays(14)).toEpochMilli(), now.toEpochMilli());

        Job.Builder job = createScheduledJob("lookback-job-datafeed-recreated");

        String datafeedId = "lookback-datafeed-datafeed-recreated";
        DatafeedConfig datafeedConfig = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"));

        putJob(job);

        CheckedRunnable<Exception> openAndRunJob = () -> {
            openJob(job.getId());
            assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

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
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs = randomIntBetween(32, 2048);
        Instant now = Instant.now();
        indexDocs(logger, "data", numDocs, now.minus(Duration.ofDays(14)).toEpochMilli(), now.toEpochMilli());

        Job.Builder job = createScheduledJob("lookback-job-query-delay-updated");
        putJob(job);

        String datafeedId = "lookback-datafeed-query-delay-updated";
        DatafeedConfig datafeedConfig = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"));
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

    public void testStopAndRestartCompositeDatafeed() throws Exception {
        updateClusterSettings(Settings.builder().put("logger.org.elasticsearch.xpack.ml.datafeed", "TRACE"));
        String indexName = "stop-restart-data";
        client().admin().indices().prepareCreate("stop-restart-data").setMapping("time", "type=date").get();
        long numDocs = randomIntBetween(32, 2048);
        final long intervalMillis = TimeValue.timeValueHours(1).millis();
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(
            logger,
            indexName,
            1,
            Intervals.alignToCeil(twoWeeksAgo, intervalMillis),
            Intervals.alignToCeil(twoWeeksAgo, intervalMillis) + 1
        );
        indexDocs(
            logger,
            indexName,
            numDocs - 1,
            Intervals.alignToCeil(twoWeeksAgo, intervalMillis) + intervalMillis,
            Intervals.alignToFloor(oneWeekAgo, intervalMillis)
        );
        long numDocs2 = randomIntBetween(32, 2048);
        indexDocs(
            logger,
            indexName,
            numDocs2,
            Intervals.alignToCeil(oneWeekAgo, intervalMillis),
            Intervals.alignToFloor(now, intervalMillis)
        );
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).setWaitForYellowStatus().get();

        String scrollJobId = "stop-restart-scroll";
        Job.Builder scrollJob = createScheduledJob(scrollJobId);
        putJob(scrollJob);
        openJob(scrollJobId);
        assertBusy(() -> assertEquals(getJobStats(scrollJobId).get(0).getState(), JobState.OPENED));

        DatafeedConfig datafeedConfig = createDatafeedBuilder(scrollJobId + "-datafeed", scrollJobId, Collections.singletonList(indexName))
            .setChunkingConfig(ChunkingConfig.newManual(new TimeValue(1, TimeUnit.SECONDS)))
            .build();
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, null);

        // Wait until we have processed data
        assertBusy(() -> assertThat(getDataCounts(scrollJobId).getProcessedRecordCount(), greaterThan(0L)));
        stopDatafeed(datafeedConfig.getId());
        assertBusy(() -> assertThat(getJobStats(scrollJobId).get(0).getState(), is(oneOf(JobState.CLOSED, JobState.OPENED))));
        // If we are not OPENED, then we are closed and shouldn't restart as the datafeed finished running through the data
        if (getJobStats(scrollJobId).get(0).getState().equals(JobState.OPENED)) {
            updateDatafeed(new DatafeedUpdate.Builder().setId(datafeedConfig.getId()).setChunkingConfig(ChunkingConfig.newAuto()).build());
            startDatafeed(
                datafeedConfig.getId(),
                randomLongBetween(0, getDataCounts(scrollJobId).getLatestRecordTimeStamp().getTime()),
                now
            );
            waitUntilJobIsClosed(scrollJobId);
        }

        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(scrollJobId);
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        }, 60, TimeUnit.SECONDS);

        String compositeJobId = "stop-restart-composite";
        Job.Builder compositeJob = createScheduledJob(compositeJobId);
        compositeJob.setAnalysisConfig(new AnalysisConfig.Builder(compositeJob.getAnalysisConfig()).setSummaryCountFieldName("doc_count"));
        putJob(compositeJob);
        openJob(compositeJobId);
        assertBusy(() -> assertEquals(getJobStats(compositeJobId).get(0).getState(), JobState.OPENED));

        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
        aggs.addAggregator(
            AggregationBuilders.composite(
                "buckets",
                Collections.singletonList(
                    new DateHistogramValuesSourceBuilder("timebucket").fixedInterval(new DateHistogramInterval("1h")).field("time")
                )
                // Set size to 1 so that start stop actually doesn't page through all the results too quickly
            ).subAggregation(AggregationBuilders.max("time").field("time")).size(1)
        );
        DatafeedConfig compositeDatafeedConfig = createDatafeedBuilder(
            compositeJobId + "-datafeed",
            compositeJobId,
            Collections.singletonList(indexName)
        ).setParsedAggregations(aggs).setFrequency(TimeValue.timeValueHours(1)).build();
        putDatafeed(compositeDatafeedConfig);
        startDatafeed(compositeDatafeedConfig.getId(), 0L, null);

        // Wait until we have processed data
        assertBusy(() -> assertThat(getDataCounts(compositeJobId).getProcessedRecordCount(), greaterThan(0L)));
        stopDatafeed(compositeDatafeedConfig.getId());
        assertBusy(() -> assertThat(getJobStats(compositeJobId).get(0).getState(), is(oneOf(JobState.CLOSED, JobState.OPENED))));
        // If we are not OPENED, then we are closed and shouldn't restart as the datafeed finished running through the data
        if (getJobStats(compositeJobId).get(0).getState().equals(JobState.OPENED)) {
            aggs = new AggregatorFactories.Builder();
            aggs.addAggregator(
                AggregationBuilders.composite(
                    "buckets",
                    Collections.singletonList(
                        new DateHistogramValuesSourceBuilder("timebucket").fixedInterval(new DateHistogramInterval("1h")).field("time")
                    )
                ).subAggregation(AggregationBuilders.max("time").field("time")).size(100)
            );
            updateDatafeed(new DatafeedUpdate.Builder().setId(compositeDatafeedConfig.getId()).setParsedAggregations(aggs).build());
            startDatafeed(
                compositeDatafeedConfig.getId(),
                randomLongBetween(0, getDataCounts(compositeJobId).getLatestRecordTimeStamp().getTime()),
                now
            );
            waitUntilJobIsClosed(compositeJobId);
        }

        List<Bucket> scrollBuckets = getBuckets(scrollJobId);
        List<Bucket> compositeBuckets = getBuckets(compositeJobId);
        assertThat(
            "scroll bucket size " + scrollBuckets + " does not equal composite bucket size" + compositeBuckets,
            compositeBuckets.size(),
            equalTo(scrollBuckets.size())
        );
        for (int i = 0; i < scrollBuckets.size(); i++) {
            Bucket scrollBucket = scrollBuckets.get(i);
            Bucket compositeBucket = compositeBuckets.get(i);
            try {
                assertThat(
                    "scroll buckets " + scrollBuckets + " composite buckets " + compositeBuckets,
                    compositeBucket.getTimestamp(),
                    equalTo(scrollBucket.getTimestamp())
                );
                assertThat(
                    "composite bucket ["
                        + compositeBucket.getTimestamp()
                        + "] ["
                        + compositeBucket.getEventCount()
                        + "] does not equal"
                        + " scroll bucket ["
                        + scrollBucket.getTimestamp()
                        + "] ["
                        + scrollBucket.getEventCount()
                        + "]",
                    compositeBucket.getEventCount(),
                    equalTo(scrollBucket.getEventCount())
                );
            } catch (AssertionError ae) {
                String originalMessage = ae.getMessage();
                try {
                    SearchSourceBuilder builder = new SearchSourceBuilder().query(
                        QueryBuilders.rangeQuery("time")
                            .gte(scrollBucket.getTimestamp().getTime())
                            .lte(scrollBucket.getTimestamp().getTime() + TimeValue.timeValueHours(1).getMillis())
                    ).size(10_000);
                    SearchHits hits = client().search(new SearchRequest().indices(indexName).source(builder)).actionGet().getHits();
                    fail("Hits: " + Strings.arrayToDelimitedString(hits.getHits(), "\n") + " \n failure: " + originalMessage);
                } catch (ElasticsearchException ee) {
                    fail("could not search indices for better info. Original failure: " + originalMessage);
                }
            }
        }
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
            HotThreads.logLocalHotThreads(logger, Level.INFO, "hot threads at failure", ReferenceDocs.LOGGING);
            throw e;
        }
        assertBusy(() -> {
            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
    }

    public void testCloseJobStopsRealtimeDatafeed() throws Exception {
        String jobId = "realtime-close-job";
        String datafeedId = jobId + "-datafeed";
        startRealtime(jobId);

        try {
            CloseJobAction.Response closeJobResponse = closeJob(jobId);
            assertTrue(closeJobResponse.isClosed());
        } catch (Exception e) {
            HotThreads.logLocalHotThreads(logger, Level.INFO, "hot threads at failure", ReferenceDocs.LOGGING);
            throw e;
        }
        assertBusy(() -> {
            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
    }

    public void testCloseJobStopsLookbackOnlyDatafeed() throws Exception {
        String jobId = "lookback-close-job";
        String datafeedId = jobId + "-datafeed";
        boolean useForce = randomBoolean();

        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs = randomIntBetween(1024, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "data", numDocs, twoWeeksAgo, oneWeekAgo);

        Job.Builder job = createScheduledJob(jobId);
        PutJobAction.Response putJobResponse = putJob(job);
        assertThat(putJobResponse.getResponse().getJobVersion(), equalTo(MlConfigVersion.CURRENT));
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilder(datafeedId, jobId, Collections.singletonList("data"));
        // Use lots of chunks to maximise the chance that we can close the job before the lookback completes
        datafeedConfigBuilder.setChunkingConfig(ChunkingConfig.newManual(new TimeValue(1, TimeUnit.SECONDS)));
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, now);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), greaterThan(0L));
        }, 60, TimeUnit.SECONDS);

        try {
            CloseJobAction.Response closeJobResponse = closeJob(jobId, useForce);
            assertTrue(closeJobResponse.isClosed());
        } catch (Exception e) {
            HotThreads.logLocalHotThreads(logger, Level.INFO, "hot threads at failure", ReferenceDocs.LOGGING);
            throw e;
        }
        GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
        assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));

        if (useForce) {
            // It's possible that the datafeed ran to completion before we force closed the job.
            // (We tried to avoid this by setting a small chunk size, but it's not impossible.)
            // If the datafeed ran to completion then there could legitimately be a model snapshot
            // even though we force closed the job, so we cannot assert in that case.
            if (getDataCounts(job.getId()).getProcessedRecordCount() < numDocs) {
                assertThat(getModelSnapshots(jobId), hasSize(0));
            }
        } else {
            assertThat(getModelSnapshots(jobId), hasSize(1));
        }
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

        Map<Long, AssertionError> exceptions = ConcurrentCollections.newConcurrentMap();

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
                exceptions.put(
                    Thread.currentThread().getId(),
                    new AssertionError("Expected STOPPED datafeed state got " + response.getResponse().results().get(0).getDatafeedState())
                );
            }
        };

        // The idea is to hit the situation where one request waits for
        // the other to complete. This is difficult to schedule but
        // hopefully it will happen in CI
        int numThreads = 5;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(stopDataFeed);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
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
        // The first step is to isolate the datafeed. But if it was already being stopped then
        // the datafeed may not be running by the time the isolate action is executed. This
        // test will sometimes (depending on thread scheduling) achieve this situation and ensure
        // the code is robust to it.
        Thread deleteDatafeedThread = new Thread(() -> {
            try {
                DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request(datafeedId);
                request.setForce(true);
                AcknowledgedResponse response = client().execute(DeleteDatafeedAction.INSTANCE, request).actionGet();
                if (response.isAcknowledged()) {
                    GetDatafeedsStatsAction.Request statsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
                    expectThrows(
                        ResourceNotFoundException.class,
                        () -> client().execute(GetDatafeedsStatsAction.INSTANCE, statsRequest).actionGet()
                    );
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
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs = randomIntBetween(1024, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "data", numDocs, twoWeeksAgo, oneWeekAgo);

        Job.Builder job = createScheduledJob("lookback-job-stopped-then-killed");
        PutJobAction.Response putJobResponse = putJob(job);
        assertThat(putJobResponse.getResponse().getJobVersion(), equalTo(MlConfigVersion.CURRENT));
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        List<String> t = Collections.singletonList("data");
        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilder(job.getId() + "-datafeed", job.getId(), t);
        // Use lots of chunks so we have time to stop the lookback before it completes
        datafeedConfigBuilder.setChunkingConfig(ChunkingConfig.newManual(new TimeValue(1, TimeUnit.SECONDS)));
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, now);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), greaterThan(0L));
        }, 60, TimeUnit.SECONDS);

        stopDatafeed(datafeedConfig.getId());

        // At this point, stopping the datafeed will have submitted a request for the job to close.
        // Depending on thread scheduling, the following kill request might overtake it. The Thread.sleep()
        // call here makes it more likely; to make it inevitable for testing also add a Thread.sleep(10)
        // immediately before the checkProcessIsAlive() call in AutodetectCommunicator.close().
        Thread.sleep(randomIntBetween(1, 9));

        KillProcessAction.Request killRequest = new KillProcessAction.Request(job.getId());
        client().execute(KillProcessAction.INSTANCE, killRequest).actionGet();

        // This should close very quickly, as we killed the process. If the job goes into the "failed"
        // state that's wrong and this test will fail.
        waitUntilJobIsClosed(job.getId(), TimeValue.timeValueSeconds(2));
    }

    private void startRealtime(String jobId) throws Exception {
        startRealtime(jobId, null);
    }

    private void startRealtime(String jobId, Integer maxEmptySearches) throws Exception {
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
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
        putJob(job);
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilder(
            job.getId() + "-datafeed",
            job.getId(),
            Collections.singletonList("data")
        );
        if (maxEmptySearches != null) {
            datafeedConfigBuilder.setMaxEmptySearches(maxEmptySearches);
        }
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
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

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/105239")
    public void testStartDatafeed_GivenTimeout_Returns408() throws Exception {
        client().admin().indices().prepareCreate("data-1").setMapping("time", "type=date").get();
        long numDocs = 100;
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        indexDocs(logger, "data-1", numDocs, oneWeekAgo, now);

        String jobId = "job-for-start-datafeed-timeout";
        String datafeedId = jobId + "-datafeed";

        Job.Builder job = createScheduledJob(jobId);
        putJob(job);
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilder(
            job.getId() + "-datafeed",
            job.getId(),
            Collections.singletonList("data-1")
        );
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        putDatafeed(datafeedConfig);

        StartDatafeedAction.Request request = new StartDatafeedAction.Request(datafeedId, oneWeekAgo);
        request.getParams().setTimeout(TimeValue.timeValueNanos(1L));

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> client().execute(StartDatafeedAction.INSTANCE, request).actionGet()
        );

        assertThat(e.status(), equalTo(RestStatus.REQUEST_TIMEOUT));
    }

    public void testStartDatafeed_GivenNegativeStartTime_Returns408() throws Exception {
        client().admin().indices().prepareCreate("data-1").setMapping("time", "type=date").get();
        long numDocs = 100;
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long negativeStartTime = -1000;
        indexDocs(logger, "data-1", numDocs, oneWeekAgo, now);

        String jobId = "job-for-start-datafeed-timeout";
        String datafeedId = jobId + "-datafeed";

        Job.Builder job = createScheduledJob(jobId);
        putJob(job);
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilder(
            job.getId() + "-datafeed",
            job.getId(),
            Collections.singletonList("data-1")
        );
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        putDatafeed(datafeedConfig);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new StartDatafeedAction.Request(datafeedId, negativeStartTime)
        );

        assertThat(e.getMessage(), equalTo("[start] must not be negative [-1000]."));
    }

    public void testStart_GivenAggregateMetricDoubleWithoutAggs() throws Exception {
        final String index = "index-with-aggregate-metric-double";
        String mapping = """
            {
              "properties": {
                "time": {
                  "type": "date"
                },
                "presum": {
                  "type": "aggregate_metric_double",
                  "metrics": [ "min", "max", "sum", "value_count" ],
                  "default_metric": "max"
                }
              }
            }""";
        client().admin().indices().prepareCreate(index).setMapping(mapping).get();

        DataDescription.Builder dataDescription = new DataDescription.Builder();

        Detector.Builder d = new Detector.Builder("avg", "presum");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));

        Job.Builder jobBuilder = new Job.Builder();
        jobBuilder.setId("job-with-aggregate-metric-double");
        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);

        putJob(jobBuilder);
        openJob(jobBuilder.getId());
        assertBusy(() -> assertEquals(getJobStats(jobBuilder.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig.Builder dfBuilder = new DatafeedConfig.Builder(jobBuilder.getId() + "-datafeed", jobBuilder.getId());
        dfBuilder.setIndices(Collections.singletonList(index));

        DatafeedConfig datafeedConfig = dfBuilder.build();

        putDatafeed(datafeedConfig);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> startDatafeed(datafeedConfig.getId(), 0L, null)
        );

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            e.getMessage(),
            containsString("field [presum] is of type [aggregate_metric_double] and cannot be used in a datafeed without aggregations")
        );
    }
}
