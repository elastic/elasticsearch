/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.dataframe.DataFrameField.INDEX_DOC_TYPE;
import static org.elasticsearch.xpack.dataframe.DataFrameInfoTransportAction.PROVIDED_STATS;

public class DataFrameUsageIT extends DataFrameRestTestCase {

    @Before
    public void createIndexes() throws IOException {
        createReviewsIndex();
    }

    public void testUsage() throws Exception {
        Response usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));

        Map<?, ?> usageAsMap = entityAsMap(usageResponse);
        assertTrue((boolean) XContentMapValues.extractValue("data_frame.available", usageAsMap));
        assertTrue((boolean) XContentMapValues.extractValue("data_frame.enabled", usageAsMap));
        // no transforms, no stats
        assertEquals(null, XContentMapValues.extractValue("data_frame.transforms", usageAsMap));
        assertEquals(null, XContentMapValues.extractValue("data_frame.stats", usageAsMap));

        // create transforms
        createPivotReviewsTransform("test_usage", "pivot_reviews", null);
        createPivotReviewsTransform("test_usage_no_stats", "pivot_reviews_no_stats", null);
        usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));
        usageAsMap = entityAsMap(usageResponse);
        assertEquals(2, XContentMapValues.extractValue("data_frame.transforms._all", usageAsMap));
        assertEquals(2, XContentMapValues.extractValue("data_frame.transforms.stopped", usageAsMap));

        startAndWaitForTransform("test_usage", "pivot_reviews");
        stopDataFrameTransform("test_usage", false);

        Request statsExistsRequest = new Request("GET",
            DataFrameInternalIndex.INDEX_NAME+"/_search?q=" +
                INDEX_DOC_TYPE.getPreferredName() + ":" +
                DataFrameTransformStateAndStats.NAME);
        // Verify that we have one stat document
        assertBusy(() -> {
            Map<String, Object> hasStatsMap = entityAsMap(client().performRequest(statsExistsRequest));
            assertEquals(1, XContentMapValues.extractValue("hits.total.value", hasStatsMap));
        });

        Request getRequest = new Request("GET", DATAFRAME_ENDPOINT + "test_usage/_stats");
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        Map<String, Integer> expectedStats = new HashMap<>();
        for(String statName : PROVIDED_STATS) {
            @SuppressWarnings("unchecked")
            List<Integer> specificStatistic = ((List<Integer>)XContentMapValues.extractValue("transforms.stats." + statName, stats));
            assertNotNull(specificStatistic);
            Integer statistic = (specificStatistic).get(0);
            expectedStats.put(statName, statistic);
        }

        usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));

        usageAsMap = entityAsMap(usageResponse);
        // we should see some stats
        assertEquals(2, XContentMapValues.extractValue("data_frame.transforms._all", usageAsMap));
        // TODO: Adjust when continuous is supported
        assertEquals(2, XContentMapValues.extractValue("data_frame.transforms.stopped", usageAsMap));
        for(String statName : PROVIDED_STATS) {
            assertEquals("Incorrect stat " +  statName,
                    expectedStats.get(statName), XContentMapValues.extractValue("data_frame.stats." + statName, usageAsMap));
        }
    }
}
