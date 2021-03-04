/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.indices.GetIndexRequest.Feature;
import org.elasticsearch.test.ESTestCase;

public class GetIndexRequestTests extends ESTestCase {

    public void testIndices() {
        String[] indices = generateRandomStringArray(5, 5, false, true);
        GetIndexRequest request = new GetIndexRequest(indices);
        assertArrayEquals(indices, request.indices());
    }

    public void testFeatures() {
        int numFeature = randomIntBetween(0, 3);
        Feature[] features = new Feature[numFeature];
        for (int i = 0; i < numFeature; i++) {
            features[i] = randomFrom(GetIndexRequest.DEFAULT_FEATURES);
        }
        GetIndexRequest request = new GetIndexRequest().addFeatures(features);
        assertArrayEquals(features, request.features());
    }

    public void testLocal() {
        boolean local = randomBoolean();
        GetIndexRequest request = new GetIndexRequest().local(local);
        assertEquals(local, request.local());
    }

    public void testHumanReadable() {
        boolean humanReadable = randomBoolean();
        GetIndexRequest request = new GetIndexRequest().humanReadable(humanReadable);
        assertEquals(humanReadable, request.humanReadable());
    }

    public void testIncludeDefaults() {
        boolean includeDefaults = randomBoolean();
        GetIndexRequest request = new GetIndexRequest().includeDefaults(includeDefaults);
        assertEquals(includeDefaults, request.includeDefaults());
    }

    public void testIndicesOptions() {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        GetIndexRequest request = new GetIndexRequest().indicesOptions(indicesOptions);
        assertEquals(indicesOptions, request.indicesOptions());
    }

}
