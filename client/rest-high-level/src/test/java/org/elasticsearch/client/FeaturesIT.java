/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.client.feature.GetFeaturesRequest;
import org.elasticsearch.client.feature.GetFeaturesResponse;
import org.elasticsearch.client.feature.ResetFeaturesRequest;
import org.elasticsearch.client.feature.ResetFeaturesResponse;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class FeaturesIT extends ESRestHighLevelClientTestCase {
    public void testGetFeatures() throws IOException {
        GetFeaturesRequest request = new GetFeaturesRequest();

        GetFeaturesResponse response = execute(request,
            highLevelClient().features()::getFeatures, highLevelClient().features()::getFeaturesAsync);

        assertThat(response, notNullValue());
        assertThat(response.getFeatures(), notNullValue());
        assertThat(response.getFeatures().size(), greaterThan(1));
        assertTrue(response.getFeatures().stream().anyMatch(feature -> "tasks".equals(feature.getFeatureName())));
    }

    public void testResetFeatures() throws IOException {
        ResetFeaturesRequest request = new ResetFeaturesRequest();

        ResetFeaturesResponse response = execute(request,
            highLevelClient().features()::resetFeatures, highLevelClient().features()::resetFeaturesAsync);

        assertThat(response, notNullValue());
        assertThat(response.getFeatures(), notNullValue());
        assertThat(response.getFeatures().size(), greaterThan(1));
        assertTrue(response.getFeatures().stream().anyMatch(
            feature -> "tasks".equals(feature.getFeatureName()) && "SUCCESS".equals(feature.getStatus())));
    }
}
