/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.transform.TransformField.INDEX_DOC_TYPE;
import static org.elasticsearch.xpack.transform.TransformInfoTransportAction.PROVIDED_STATS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class TransformUsageIT extends TransformRestTestCase {

    @Before
    public void createIndexes() throws IOException {
        createReviewsIndex();
    }

    public void testUsage() throws Exception {
        Response usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));

        Map<?, ?> usageAsMap = entityAsMap(usageResponse);
        assertTrue((boolean) XContentMapValues.extractValue("transform.available", usageAsMap));
        assertTrue((boolean) XContentMapValues.extractValue("transform.enabled", usageAsMap));
        // no transforms, no stats
        assertNull(XContentMapValues.extractValue("transform.transforms", usageAsMap));
        assertNull(XContentMapValues.extractValue("transform.feature_counts", usageAsMap));
        assertNull(XContentMapValues.extractValue("transform.stats", usageAsMap));

        // create transforms
        createPivotReviewsTransform("test_usage", "pivot_reviews", null);
        createPivotReviewsTransform("test_usage_no_stats", "pivot_reviews_no_stats", null);
        createContinuousPivotReviewsTransform("test_usage_continuous", "pivot_reviews_continuous", null);
        createLatestReviewsTransform("test_usage_latest", "latest_reviews");
        usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));
        usageAsMap = entityAsMap(usageResponse);
        assertEquals(4, XContentMapValues.extractValue("transform.transforms._all", usageAsMap));
        assertEquals(4, XContentMapValues.extractValue("transform.transforms.stopped", usageAsMap));
        assertEquals(3, XContentMapValues.extractValue("transform.feature_counts.pivot", usageAsMap));
        assertEquals(1, XContentMapValues.extractValue("transform.feature_counts.latest", usageAsMap));
        assertEquals(0, XContentMapValues.extractValue("transform.feature_counts.retention_policy", usageAsMap));
        assertEquals(1, XContentMapValues.extractValue("transform.feature_counts.sync", usageAsMap));

        startAndWaitForTransform("test_usage", "pivot_reviews");
        stopTransform("test_usage", false);

        Request statsExistsRequest = new Request(
            "GET",
            TransformInternalIndexConstants.LATEST_INDEX_NAME
                + "/_search?q="
                + INDEX_DOC_TYPE.getPreferredName()
                + ":"
                + TransformStoredDoc.NAME
        );
        statsExistsRequest.setOptions(
            expectWarnings(
                "this request accesses system indices: ["
                    + TransformInternalIndexConstants.LATEST_INDEX_NAME
                    + "], but in a future major version, direct access to system indices will "
                    + "be prevented by default"
            )
        );
        // Verify that we have one stat document
        assertBusy(() -> {
            Map<String, Object> hasStatsMap = entityAsMap(client().performRequest(statsExistsRequest));
            assertEquals(1, XContentMapValues.extractValue("hits.total.value", hasStatsMap));
        });

        startAndWaitForContinuousTransform("test_usage_continuous", "pivot_reviews_continuous", null);

        Request getRequest = new Request("GET", getTransformEndpoint() + "test_usage/_stats");
        Map<String, Object> stats = entityAsMap(client().performRequest(getRequest));
        Map<String, Double> expectedStats = new HashMap<>();
        for (String statName : PROVIDED_STATS) {
            @SuppressWarnings("unchecked")
            List<Object> specificStatistic = (List<Object>) (XContentMapValues.extractValue("transforms.stats." + statName, stats));
            assertNotNull(specificStatistic);
            expectedStats.put(statName, extractStatsAsDouble(specificStatistic.get(0)));
        }

        getRequest = new Request("GET", getTransformEndpoint() + "test_usage_continuous/_stats");
        stats = entityAsMap(client().performRequest(getRequest));
        for (String statName : PROVIDED_STATS) {
            @SuppressWarnings("unchecked")
            List<Object> specificStatistic = (List<Object>) (XContentMapValues.extractValue("transforms.stats." + statName, stats));
            assertNotNull(specificStatistic);
            expectedStats.compute(statName, (key, value) -> value + extractStatsAsDouble(specificStatistic.get(0)));
        }

        // Simply because we wait for continuous to reach checkpoint 1, does not mean that the statistics are written yet.
        // Since we search against the indices for the statistics, we need to ensure they are written, so we will wait for that
        // to be the case.
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "_xpack/usage"));
            Map<String, Object> statsMap = entityAsMap(response);
            // we should see some stats
            assertEquals(4, XContentMapValues.extractValue("transform.transforms._all", statsMap));
            assertEquals(3, XContentMapValues.extractValue("transform.transforms.stopped", statsMap));
            assertEquals(1, XContentMapValues.extractValue("transform.transforms.started", statsMap));
            assertEquals(3, XContentMapValues.extractValue("transform.feature_counts.pivot", statsMap));
            assertEquals(1, XContentMapValues.extractValue("transform.feature_counts.latest", statsMap));
            assertEquals(0, XContentMapValues.extractValue("transform.feature_counts.retention_policy", statsMap));
            assertEquals(1, XContentMapValues.extractValue("transform.feature_counts.sync", statsMap));
            for (String statName : PROVIDED_STATS) {
                // the trigger count can be off: e.g. if the scheduler kicked in before usage has been called,
                // or if the scheduler triggered later, but state hasn't been persisted (by design)
                // however, we know that as we have 2 transforms, the trigger count must be greater or equal to 2
                if (statName.equals(TransformIndexerStats.NUM_INVOCATIONS.getPreferredName())) {
                    assertThat(
                        "Incorrect stat " + statName + ", got: " + statsMap.get("transform"),
                        extractStatsAsDouble(XContentMapValues.extractValue("transform.stats." + statName, statsMap)),
                        greaterThanOrEqualTo(2.0)
                    );
                } else {
                    assertThat(
                        "Incorrect stat " + statName + ", got: " + statsMap.get("transform"),
                        extractStatsAsDouble(XContentMapValues.extractValue("transform.stats." + statName, statsMap)),
                        equalTo(expectedStats.get(statName).doubleValue())
                    );
                }
            }
            // Refresh the index so that statistics are searchable
            refreshAllIndices();
        }, 60, TimeUnit.SECONDS);

        stopTransform("test_usage_continuous", false);

        usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));
        usageAsMap = entityAsMap(usageResponse);

        assertEquals(4, XContentMapValues.extractValue("transform.transforms._all", usageAsMap));
        assertEquals(4, XContentMapValues.extractValue("transform.transforms.stopped", usageAsMap));
        assertEquals(3, XContentMapValues.extractValue("transform.feature_counts.pivot", usageAsMap));
        assertEquals(1, XContentMapValues.extractValue("transform.feature_counts.latest", usageAsMap));
        assertEquals(0, XContentMapValues.extractValue("transform.feature_counts.retention_policy", usageAsMap));
        assertEquals(1, XContentMapValues.extractValue("transform.feature_counts.sync", usageAsMap));
    }

    private static double extractStatsAsDouble(Object statsObject) {
        if (statsObject instanceof Integer) {
            return ((Integer) statsObject).doubleValue();
        } else if (statsObject instanceof Double) {
            return (Double) statsObject;
        }
        fail("unexpected value type for stats");
        return 0;
    }
}
