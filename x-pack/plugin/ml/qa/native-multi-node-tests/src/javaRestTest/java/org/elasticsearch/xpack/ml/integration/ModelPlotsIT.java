/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ModelPlotsIT extends MlNativeAutodetectIntegTestCase {

    private static final String DATA_INDEX = "model-plots-test-data";

    @Before
    public void setUpData() {
        client().admin().indices().prepareCreate(DATA_INDEX)
                .setMapping("time", "type=date,format=epoch_millis", "user", "type=keyword")
                .get();

        List<String> users = Arrays.asList("user_1", "user_2", "user_3");

        // We are going to create data for last day
        long nowMillis = System.currentTimeMillis();
        int totalBuckets = 24;
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            long timestamp = nowMillis - TimeValue.timeValueHours(totalBuckets - bucket).getMillis();
            for (String user : users) {
                IndexRequest indexRequest = new IndexRequest(DATA_INDEX);
                indexRequest.source("time", timestamp, "user", user);
                bulkRequestBuilder.add(indexRequest);
            }
        }

        BulkResponse bulkResponse = bulkRequestBuilder
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        assertThat(bulkResponse.hasFailures(), is(false));
    }

    @After
    public void tearDownData() {
        client().admin().indices().prepareDelete(DATA_INDEX).get();
        cleanUp();
    }

    public void testPartitionFieldWithoutTerms() throws Exception {
        Job.Builder job = jobWithPartitionUser("model-plots-it-test-partition-field-without-terms");
        job.setModelPlotConfig(new ModelPlotConfig());
        putJob(job);
        String datafeedId = job.getId() + "-feed";
        DatafeedConfig datafeed = newDatafeed(datafeedId, job.getId());
        putDatafeed(datafeed);
        openJob(job.getId());
        startDatafeed(datafeedId, 0, System.currentTimeMillis());
        waitUntilJobIsClosed(job.getId());

        // As the initial time is random, there's a chance the first record is
        // aligned on a bucket start. Thus we check the buckets are in [23, 24]
        assertThat(getBuckets(job.getId()).size(), greaterThanOrEqualTo(23));
        assertThat(getBuckets(job.getId()).size(), lessThanOrEqualTo(24));

        Set<String> modelPlotTerms = modelPlotTerms(job.getId(), "partition_field_value");
        assertThat(modelPlotTerms, containsInAnyOrder("user_1", "user_2", "user_3"));
    }

    public void testPartitionFieldWithTerms() throws Exception {
        Job.Builder job = jobWithPartitionUser("model-plots-it-test-partition-field-with-terms");
        job.setModelPlotConfig(new ModelPlotConfig(true, "user_2,user_3", false));
        putJob(job);
        String datafeedId = job.getId() + "-feed";
        DatafeedConfig datafeed = newDatafeed(datafeedId, job.getId());
        putDatafeed(datafeed);
        openJob(job.getId());
        startDatafeed(datafeedId, 0, System.currentTimeMillis());
        waitUntilJobIsClosed(job.getId());

        // As the initial time is random, there's a chance the first record is
        // aligned on a bucket start. Thus we check the buckets are in [23, 24]
        assertThat(getBuckets(job.getId()).size(), greaterThanOrEqualTo(23));
        assertThat(getBuckets(job.getId()).size(), lessThanOrEqualTo(24));

        Set<String> modelPlotTerms = modelPlotTerms(job.getId(), "partition_field_value");
        assertThat(modelPlotTerms, containsInAnyOrder("user_2", "user_3"));
    }

    public void testByFieldWithTerms() throws Exception {
        Job.Builder job = jobWithByUser("model-plots-it-test-by-field-with-terms");
        job.setModelPlotConfig(new ModelPlotConfig(true, "user_2,user_3", false));
        putJob(job);
        String datafeedId = job.getId() + "-feed";
        DatafeedConfig datafeed = newDatafeed(datafeedId, job.getId());
        putDatafeed(datafeed);
        openJob(job.getId());
        startDatafeed(datafeedId, 0, System.currentTimeMillis());
        waitUntilJobIsClosed(job.getId());

        // As the initial time is random, there's a chance the first record is
        // aligned on a bucket start. Thus we check the buckets are in [23, 24]
        assertThat(getBuckets(job.getId()).size(), greaterThanOrEqualTo(23));
        assertThat(getBuckets(job.getId()).size(), lessThanOrEqualTo(24));

        Set<String> modelPlotTerms = modelPlotTerms(job.getId(), "by_field_value");
        assertThat(modelPlotTerms, containsInAnyOrder("user_2", "user_3"));
    }

    private static Job.Builder jobWithPartitionUser(String id) {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setPartitionFieldName("user");
        return newJobBuilder(id, detector.build());
    }

    private static Job.Builder jobWithByUser(String id) {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setByFieldName("user");
        return newJobBuilder(id, detector.build());
    }

    private static Job.Builder newJobBuilder(String id, Detector detector) {
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = new Job.Builder(id);
        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);
        return jobBuilder;
    }

    private static DatafeedConfig newDatafeed(String datafeedId, String jobId) {
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedConfig.setIndices(Arrays.asList(DATA_INDEX));
        return datafeedConfig.build();
    }

    private Set<String> modelPlotTerms(String jobId, String fieldName) {
        SearchResponse searchResponse = client().prepareSearch(".ml-anomalies-" + jobId)
                .setQuery(QueryBuilders.termQuery("result_type", "model_plot"))
                .addAggregation(AggregationBuilders.terms("model_plot_terms").field(fieldName))
                .get();

        Terms aggregation = searchResponse.getAggregations().get("model_plot_terms");
        return aggregation.getBuckets().stream().map(agg -> agg.getKeyAsString()).collect(Collectors.toSet());
    }
}
