/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.dataframe.DataFrameField.INDEX_DOC_TYPE;
import static org.elasticsearch.xpack.dataframe.DataFrameFeatureSet.PROVIDED_STATS;

public class DataFrameUsageIT extends DataFrameRestTestCase {
    private boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;
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
        createPivotReviewsTransform("test_usage_no_task", "pivot_reviews_no_task", null);
        createPivotReviewsTransform("test_usage_no_stats_or_task", "pivot_reviews_no_stats_or_task", null);
        usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));
        usageAsMap = entityAsMap(usageResponse);
        assertEquals(3, XContentMapValues.extractValue("data_frame.transforms._all", usageAsMap));
        assertEquals(3, XContentMapValues.extractValue("data_frame.transforms.stopped", usageAsMap));

        startAndWaitForTransform("test_usage_no_task", "pivot_reviews_no_task");
        stopDataFrameTransform("test_usage_no_task", false);
        // Remove the task, we should still have the transform and its stat doc
        client().performRequest(new Request("POST", "_tasks/_cancel?actions="+ DataFrameField.TASK_NAME+"*"));

        startAndWaitForTransform("test_usage", "pivot_reviews");

        Request statsExistsRequest = new Request("GET",
            DataFrameInternalIndex.INDEX_NAME+"/_search?q=" +
                INDEX_DOC_TYPE.getPreferredName() + ":" +
                DataFrameTransformStateAndStats.NAME);
        // Verify that we have our two stats documents
        assertBusy(() -> {
            Map<String, Object> hasStatsMap = entityAsMap(client().performRequest(statsExistsRequest));
            assertEquals(2, XContentMapValues.extractValue("hits.total.value", hasStatsMap));
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

        getRequest = new Request("GET", DATAFRAME_ENDPOINT + "test_usage_no_task/_stats");
        stats = entityAsMap(client().performRequest(getRequest));
        for(String statName : PROVIDED_STATS) {
            @SuppressWarnings("unchecked")
            List<Integer> specificStatistic = ((List<Integer>)XContentMapValues.extractValue("transforms.stats." + statName, stats));
            assertNotNull(specificStatistic);
            Integer statistic = (specificStatistic).get(0);
            expectedStats.merge(statName, statistic, Integer::sum);
        }

        usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));

        usageAsMap = entityAsMap(usageResponse);
        // we should see some stats
        assertEquals(3, XContentMapValues.extractValue("data_frame.transforms._all", usageAsMap));
        // TODO: due to auto-stop we only see stopped data frames
        assertEquals(3, XContentMapValues.extractValue("data_frame.transforms.stopped", usageAsMap));
        for(String statName : PROVIDED_STATS) {
            assertEquals("Incorrect stat " +  statName,
                    expectedStats.get(statName), XContentMapValues.extractValue("data_frame.stats." + statName, usageAsMap));
        }
    }
}
