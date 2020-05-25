/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

public class TransformMetadataIT extends TransformRestTestCase {

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

    public void testMetadata() throws Exception {
        long testStarted = System.currentTimeMillis();
        createPivotReviewsTransform("test_meta", "pivot_reviews", null);
        startAndWaitForTransform("test_meta", "pivot_reviews");

        Response mappingResponse = client().performRequest(new Request("GET", "pivot_reviews/_mapping"));

        Map<?, ?> mappingAsMap = entityAsMap(mappingResponse);
        assertEquals(Version.CURRENT.toString(),
                XContentMapValues.extractValue("pivot_reviews.mappings._meta._transform.version.created", mappingAsMap));
        assertTrue((Long) XContentMapValues.extractValue("pivot_reviews.mappings._meta._transform.creation_date_in_millis",
                mappingAsMap) < System.currentTimeMillis());
        assertTrue((Long) XContentMapValues.extractValue("pivot_reviews.mappings._meta._transform.creation_date_in_millis",
                mappingAsMap) > testStarted);
        assertEquals("test_meta",
                XContentMapValues.extractValue("pivot_reviews.mappings._meta._transform.transform", mappingAsMap));
        assertEquals("transform",
                XContentMapValues.extractValue("pivot_reviews.mappings._meta.created_by", mappingAsMap));
    }

}
