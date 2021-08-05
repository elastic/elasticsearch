/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.feature.ResetFeaturesRequest;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TestFeatureResetIT extends TransformIntegTestCase {

    @Before
    public void setLogging() throws IOException {
        Request settingsRequest = new Request("PUT", "/_cluster/settings");
        settingsRequest.setJsonEntity(
            "{\"transient\": {"
                + "\"logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer\": \"debug\","
                + "\"logger.org.elasticsearch.xpack.transform\": \"trace\"}}"
        );
        client().performRequest(settingsRequest);
    }

    @After
    public void cleanup() throws Exception {
        cleanUp();
    }

    @SuppressWarnings("unchecked")
    public void testTransformFeatureReset() throws Exception {
        String indexName = "basic-crud-reviews";
        String transformId = "batch-transform-feature-reset";
        createReviewsIndex(indexName, 100, 100, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", TermsGroupSource.builder().setField("user_id").build());
        groups.put("by-business", TermsGroupSource.builder().setField("business_id").build());

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config =
            createTransformConfigBuilder(transformId, "reviews-by-user-business-day", QueryBuilders.matchAllQuery(), indexName)
                .setPivotConfig(createPivotConfig(groups, aggs))
                .build();

        assertTrue(putTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        transformId = "continuous-transform-feature-reset";
        config =
            createTransformConfigBuilder(transformId, "reviews-by-user-business-day-cont", QueryBuilders.matchAllQuery(), indexName)
                .setPivotConfig(createPivotConfig(groups, aggs))
                .setSyncConfig(TimeSyncConfig.builder().setField("timestamp").setDelay(TimeValue.timeValueSeconds(1)).build())
                .build();

        assertTrue(putTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());
        TestRestHighLevelClient highLevelClient = new TestRestHighLevelClient();
        highLevelClient.features().resetFeatures(new ResetFeaturesRequest(), RequestOptions.DEFAULT);

        Response response = adminClient().performRequest(new Request("GET", "/_cluster/state?metric=metadata"));
        Map<String, Object> metadata = (Map<String, Object>)ESRestTestCase.entityAsMap(response).get("metadata");
        assertThat(metadata, is(not(nullValue())));

        // after a successful reset we completely remove the transform metadata
        Map<String, Object> transformMetadata = (Map<String, Object>)metadata.get("transform");
        assertThat(transformMetadata, is(nullValue()));

        // assert transforms are gone
        assertThat(getTransform("_all").getCount(), equalTo(0L));

        // assert transform indices are gone
        assertThat(
            ESRestTestCase.entityAsMap(adminClient().performRequest(new Request("GET", ".transform-*"))),
            is(anEmptyMap())
        );
    }

}
