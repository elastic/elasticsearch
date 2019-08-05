/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.IndexerState;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.client.dataframe.transforms.TimeSyncConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.SingleGroupSource;
import org.elasticsearch.client.dataframe.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class DataFrameTransformIT extends DataFrameIntegTestCase {

    @After
    public void cleanTransforms() throws IOException {
        cleanUp();
    }

    public void testDataFrameTransformCrud() throws Exception {
        String indexName = "basic-crud-reviews";
        createReviewsIndex(indexName, 100);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", TermsGroupSource.builder().setField("user_id").build());
        groups.put("by-business", TermsGroupSource.builder().setField("business_id").build());

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        DataFrameTransformConfig config = createTransformConfig("data-frame-transform-crud",
            groups,
            aggs,
            "reviews-by-user-business-day",
            indexName);

        assertTrue(putDataFrameTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startDataFrameTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        waitUntilCheckpoint(config.getId(), 1L);

        // It will eventually be stopped
        assertBusy(() -> assertThat(getDataFrameTransformStats(config.getId())
                .getTransformsStats().get(0).getCheckpointingInfo().getNext().getIndexerState(), equalTo(IndexerState.STOPPED)));
        stopDataFrameTransform(config.getId());

        DataFrameTransformConfig storedConfig = getDataFrameTransform(config.getId()).getTransformConfigurations().get(0);
        assertThat(storedConfig.getVersion(), equalTo(Version.CURRENT));
        Instant now = Instant.now();
        assertTrue("[create_time] is not before current time", storedConfig.getCreateTime().isBefore(now));
        deleteDataFrameTransform(config.getId());
    }

    public void testContinuousDataFrameTransformCrud() throws Exception {
        String indexName = "continuous-crud-reviews";
        createReviewsIndex(indexName, 100);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", TermsGroupSource.builder().setField("user_id").build());
        groups.put("by-business", TermsGroupSource.builder().setField("business_id").build());

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        DataFrameTransformConfig config = createTransformConfigBuilder("data-frame-transform-crud",
            groups,
            aggs,
            "reviews-by-user-business-day",
            QueryBuilders.matchAllQuery(),
            indexName)
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .build();

        assertTrue(putDataFrameTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startDataFrameTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        waitUntilCheckpoint(config.getId(), 1L);
        assertThat(getDataFrameTransformStats(config.getId()).getTransformsStats().get(0).getTaskState(),
                equalTo(DataFrameTransformTaskState.STARTED));

        long docsIndexed = getDataFrameTransformStats(config.getId())
            .getTransformsStats()
            .get(0)
            .getIndexerStats()
            .getNumDocuments();

        DataFrameTransformConfig storedConfig = getDataFrameTransform(config.getId()).getTransformConfigurations().get(0);
        assertThat(storedConfig.getVersion(), equalTo(Version.CURRENT));
        Instant now = Instant.now();
        assertTrue("[create_time] is not before current time", storedConfig.getCreateTime().isBefore(now));

        // index some more docs
        long timeStamp = Instant.now().toEpochMilli() - 1_000;
        long user = 42;
        BulkRequest bulk = new BulkRequest(indexName);
        for (int i = 0; i < 25; i++) {
            int stars = (i + 20) % 5;
            long business = (i + 100) % 50;

            StringBuilder sourceBuilder = new StringBuilder();
            sourceBuilder.append("{\"user_id\":\"")
                .append("user_")
                .append(user)
                .append("\",\"count\":")
                .append(i)
                .append(",\"business_id\":\"")
                .append("business_")
                .append(business)
                .append("\",\"stars\":")
                .append(stars)
                .append(",\"timestamp\":")
                .append(timeStamp)
                .append("}");
            bulk.add(new IndexRequest().source(sourceBuilder.toString(), XContentType.JSON));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkIndexDocs(bulk);

        waitUntilCheckpoint(config.getId(), 2L);

        // Assert that we wrote the new docs
        assertThat(getDataFrameTransformStats(config.getId())
            .getTransformsStats()
            .get(0)
            .getIndexerStats()
            .getNumDocuments(), greaterThan(docsIndexed));

        stopDataFrameTransform(config.getId());
        deleteDataFrameTransform(config.getId());
    }
}
