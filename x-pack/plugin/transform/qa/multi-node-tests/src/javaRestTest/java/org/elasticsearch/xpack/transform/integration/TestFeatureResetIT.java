/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.integration;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
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

public class TestFeatureResetIT extends TransformRestTestCase {

    @Before
    public void setLogging() throws IOException {
        Request settingsRequest = new Request("PUT", "/_cluster/settings");
        settingsRequest.setJsonEntity("""
            {
              "persistent": {
                "logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer": "debug",
                "logger.org.elasticsearch.xpack.transform": "trace",
                "logger.org.elasticsearch.xpack.transform.notifications": "debug",
                "logger.org.elasticsearch.xpack.transform.transforms": "debug"
              }
            }""");
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
        groups.put("by-user", new TermsGroupSource("user_id", null, false));
        groups.put("by-business", new TermsGroupSource("business_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config = createTransformConfigBuilder(
            transformId,
            "reviews-by-user-business-day",
            QueryConfig.matchAll(),
            indexName
        ).setPivotConfig(createPivotConfig(groups, aggs)).build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        String continuousTransformId = "continuous-transform-feature-reset";
        config = createTransformConfigBuilder(continuousTransformId, "reviews-by-user-business-day-cont", QueryConfig.matchAll(), indexName)
            .setPivotConfig(createPivotConfig(groups, aggs))
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .build();

        putTransform(continuousTransformId, Strings.toString(config), RequestOptions.DEFAULT);

        // Sleep for a few seconds so that we cover transform being stopped at various stages.
        Thread.sleep(randomLongBetween(0, 5_000));

        startTransform(continuousTransformId, RequestOptions.DEFAULT);

        assertOK(client().performRequest(new Request(HttpPost.METHOD_NAME, "/_features/_reset")));

        Response response = adminClient().performRequest(new Request("GET", "/_cluster/state?metric=metadata"));
        Map<String, Object> metadata = (Map<String, Object>) ESRestTestCase.entityAsMap(response).get("metadata");
        assertThat(metadata, is(not(nullValue())));

        // after a successful reset we completely remove the transform metadata
        Map<String, Object> transformMetadata = (Map<String, Object>) metadata.get("transform");
        assertThat(transformMetadata, is(nullValue()));

        // assert transforms are gone
        assertThat((Integer) getTransforms("_all").get("count"), equalTo(0));

        // assert transform documents are gone
        Map<String, Object> transformIndicesContents = ESRestTestCase.entityAsMap(
            adminClient().performRequest(new Request("GET", ".transform-*/_search"))
        );
        assertThat(
            "Indices contents were: " + transformIndicesContents,
            XContentMapValues.extractValue(transformIndicesContents, "hits", "total", "value"),
            is(equalTo(0))
        );

        // assert transform indices are gone
        Map<String, Object> transformIndices = ESRestTestCase.entityAsMap(adminClient().performRequest(new Request("GET", ".transform-*")));
        assertThat("Indices were: " + transformIndices, transformIndices, is(anEmptyMap()));
    }
}
