/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.client.snapshots.GetFeaturesRequest;
import org.elasticsearch.client.snapshots.GetFeaturesResponse;

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
}
