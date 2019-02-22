/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

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

    public void testUsage() throws IOException {
        Response usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));

        Map<?, ?> usageAsMap = entityAsMap(usageResponse);
        assertTrue((boolean) XContentMapValues.extractValue("data_frame.available", usageAsMap));
        assertTrue((boolean) XContentMapValues.extractValue("data_frame.enabled", usageAsMap));
        // no transforms, no stats
        assertEquals(null, XContentMapValues.extractValue("data_frame.transforms", usageAsMap));
        assertEquals(null, XContentMapValues.extractValue("data_frame.stats", usageAsMap));

        // create a transform
        createPivotReviewsTransform("test_usage", "pivot_reviews", null);

        usageResponse = client().performRequest(new Request("GET", "_xpack/usage"));

        usageAsMap = entityAsMap(usageResponse);
        // we should see some stats
        assertEquals(1, XContentMapValues.extractValue("data_frame.transforms._all", usageAsMap));
        assertEquals(0, XContentMapValues.extractValue("data_frame.stats.index_failures", usageAsMap));
    }
}
