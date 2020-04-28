/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.junit.After;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class DatafeedWithAggsIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanup(){
        cleanUp();
    }

    public void testRealtime() throws Exception {
        String dataIndex = "datafeed-with-aggs-rt-data";

        // A job with a bucket_span of 2s
        String jobId = "datafeed-with-aggs-rt-job";
        DataDescription.Builder dataDescription = new DataDescription.Builder();

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueSeconds(2));
        analysisConfig.setSummaryCountFieldName("doc_count");

        Job.Builder jobBuilder = new Job.Builder();
        jobBuilder.setId(jobId);

        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);

        // Datafeed with aggs
        String datafeedId = jobId + "-feed";
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedBuilder.setQueryDelay(TimeValue.timeValueMillis(100));
        datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(1));
        datafeedBuilder.setIndices(Collections.singletonList(dataIndex));

        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
        aggs.addAggregator(AggregationBuilders.dateHistogram("time").field("time")
            .fixedInterval(new DateHistogramInterval("1000ms"))
            .subAggregation(AggregationBuilders.max("time").field("time")));
        datafeedBuilder.setParsedAggregations(aggs);

        DatafeedConfig datafeed = datafeedBuilder.build();

        // Create stuff and open job
        registerJob(jobBuilder);
        putJob(jobBuilder);
        registerDatafeed(datafeed);
        putDatafeed(datafeed);
        openJob(jobId);

        // Now let's index the data
        client().admin().indices().prepareCreate(dataIndex)
            .setMapping("time", "type=date")
            .get();

        // Index a doc per second from a minute ago to a minute later
        long now = System.currentTimeMillis();
        long aMinuteAgo = now - TimeValue.timeValueMinutes(1).millis();
        long aMinuteLater = now + TimeValue.timeValueMinutes(1).millis();
        long curTime = aMinuteAgo;
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        while (curTime < aMinuteLater) {
            IndexRequest indexRequest = new IndexRequest(dataIndex);
            indexRequest.source("time", curTime);
            bulkRequestBuilder.add(indexRequest);
            curTime += TimeValue.timeValueSeconds(1).millis();
        }
        BulkResponse bulkResponse = bulkRequestBuilder
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index docs: " + bulkResponse.buildFailureMessage());
        }

        // And start datafeed in real-time mode
        startDatafeed(datafeedId, 0L, null);

        // Wait until we finalize a bucket after now
        assertBusy(() -> {
            GetBucketsAction.Request getBucketsRequest = new GetBucketsAction.Request(jobId);
            getBucketsRequest.setExcludeInterim(true);
            getBucketsRequest.setSort("timestamp");
            getBucketsRequest.setDescending(true);
            List<Bucket> buckets = getBuckets(getBucketsRequest);
            assertThat(buckets.size(), greaterThanOrEqualTo(1));
            assertThat(buckets.get(0).getTimestamp().getTime(), greaterThan(now));
        }, 30, TimeUnit.SECONDS);

        // Wrap up
        StopDatafeedAction.Response stopJobResponse = stopDatafeed(datafeedId);
        assertTrue(stopJobResponse.isStopped());
        assertBusy(() -> {
            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
        closeJob(jobId);

        // Assert we have not dropped any data - final buckets should contain 2 events each
        GetBucketsAction.Request getBucketsRequest = new GetBucketsAction.Request(jobId);
        getBucketsRequest.setExcludeInterim(true);
        List<Bucket> buckets = getBuckets(getBucketsRequest);
        for (Bucket bucket : buckets) {
            if (bucket.getEventCount() != 2) {
                fail("Bucket [" + bucket.getTimestamp().getTime() + "] has [" + bucket.getEventCount() + "] when 2 were expected");
            }
        }
    }
}
