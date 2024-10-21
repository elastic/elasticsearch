/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestTests;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GetIndexRequestTests extends ESTestCase {

    public void testFeaturesFromRequest() {
        int numFeatures = randomIntBetween(1, GetIndexRequest.DEFAULT_FEATURES.length);
        List<String> featureNames = new ArrayList<>();
        List<GetIndexRequest.Feature> expectedFeatures = new ArrayList<>();
        for (int k = 0; k < numFeatures; k++) {
            GetIndexRequest.Feature feature = randomValueOtherThanMany(
                f -> featureNames.contains(f.name()),
                () -> randomFrom(GetIndexRequest.DEFAULT_FEATURES)
            );
            featureNames.add(feature.name());
            expectedFeatures.add(feature);
        }

        RestRequest request = RestRequestTests.contentRestRequest("", Map.of("features", String.join(",", featureNames)));
        GetIndexRequest.Feature[] featureArray = GetIndexRequest.Feature.fromRequest(request);
        assertThat(featureArray, arrayContainingInAnyOrder(expectedFeatures.toArray(GetIndexRequest.Feature[]::new)));
    }

    public void testDuplicateFeatures() {
        int numFeatures = randomIntBetween(1, 5);
        GetIndexRequest.Feature feature = randomFrom(GetIndexRequest.DEFAULT_FEATURES);
        List<String> featureList = new ArrayList<>();
        for (int k = 0; k < numFeatures; k++) {
            featureList.add(feature.name());
        }
        RestRequest request = RestRequestTests.contentRestRequest("", Map.of("features", String.join(",", featureList)));
        GetIndexRequest.Feature[] features = GetIndexRequest.Feature.fromRequest(request);
        assertThat(features.length, equalTo(1));
        assertThat(features[0], equalTo(feature));
    }

    public void testMissingFeatures() {
        RestRequest request = RestRequestTests.contentRestRequest("", Map.of());
        GetIndexRequest.Feature[] features = GetIndexRequest.Feature.fromRequest(request);
        assertThat(features, arrayContainingInAnyOrder(GetIndexRequest.DEFAULT_FEATURES));
    }

    public void testInvalidFeatures() {
        int numFeatures = randomIntBetween(1, 4);
        List<String> invalidFeatures = new ArrayList<>();
        for (int k = 0; k < numFeatures; k++) {
            invalidFeatures.add(randomAlphaOfLength(5));
        }

        RestRequest request = RestRequestTests.contentRestRequest("", Map.of("features", String.join(",", invalidFeatures)));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> GetIndexRequest.Feature.fromRequest(request));
        assertThat(e.getMessage(), containsString(Strings.format("Invalid features specified [%s]", String.join(",", invalidFeatures))));
    }

    public void testIndicesOptions() {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        assertThat(
            getIndexRequest.indicesOptions().concreteTargetOptions(),
            equalTo(IndicesOptions.strictExpandOpen().concreteTargetOptions())
        );
        assertThat(getIndexRequest.indicesOptions().wildcardOptions(), equalTo(IndicesOptions.strictExpandOpen().wildcardOptions()));
        assertThat(getIndexRequest.indicesOptions().gatekeeperOptions(), equalTo(IndicesOptions.strictExpandOpen().gatekeeperOptions()));
        assertThat(getIndexRequest.indicesOptions().selectorOptions(), equalTo(IndicesOptions.SelectorOptions.ALL_APPLICABLE));
    }
}
