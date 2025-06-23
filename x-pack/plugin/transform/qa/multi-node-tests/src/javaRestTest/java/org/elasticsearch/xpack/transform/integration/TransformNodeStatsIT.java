/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.junit.After;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class TransformNodeStatsIT extends TransformRestTestCase {

    private static final int NUM_USERS = 28;

    static Integer getUserIdForRow(int row) {
        return row % NUM_USERS;
    }

    static String getDateStringForRow(int row) {
        int day = (11 + (row / 100)) % 28;
        int hour = 10 + (row % 13);
        int min = 10 + (row % 49);
        int sec = 10 + (row % 49);
        return "2017-01-" + (day < 10 ? "0" + day : day) + "T" + hour + ":" + min + ":" + sec + "Z";
    }

    @After
    public void cleanTransforms() throws Exception {
        cleanUp();
    }

    @SuppressWarnings("unchecked")
    public void testTransformNodeStats() throws Exception {
        var transformId = "transform-node-stats";
        createTransform("basic-stats-reviews", transformId);

        var nodesInfo = getNodesInfo(adminClient());
        assertThat("Nodes were: " + nodesInfo, nodesInfo.size(), is(equalTo(3)));

        var response = entityAsMap(getNodeStats());
        assertThat(response, hasKey("total"));
        assertThat(
            "Response was: " + response,
            (int) XContentMapValues.extractValue(response, "total", "scheduler", "registered_transform_count"),
            is(equalTo(1))
        );
        for (String nodeId : nodesInfo.keySet()) {
            assertThat(response, hasKey(nodeId));
            assertThat(
                "Response was: " + response,
                (int) XContentMapValues.extractValue(response, nodeId, "scheduler", "registered_transform_count"),
                is(greaterThanOrEqualTo(0))
            );
        }
    }

    private void createTransform(String indexName, String transformId) throws Exception {
        createReviewsIndex(indexName, 100, NUM_USERS, TransformNodeStatsIT::getUserIdForRow, TransformNodeStatsIT::getDateStringForRow);

        var groups = Map.of(
            "by-day",
            createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null),
            "by-user",
            new TermsGroupSource("user_id", null, false),
            "by-business",
            new TermsGroupSource("business_id", null, false)
        );

        var aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        var config = createTransformConfigBuilder(transformId, "reviews-by-user-business-day", QueryConfig.matchAll(), indexName)
            .setPivotConfig(createPivotConfig(groups, aggs))
            .setSyncConfig(new TimeSyncConfig("timestamp", null))
            .build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        waitUntilCheckpoint(config.getId(), 1L);
    }
}
