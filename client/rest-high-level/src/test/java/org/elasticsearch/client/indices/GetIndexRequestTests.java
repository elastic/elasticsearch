/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
