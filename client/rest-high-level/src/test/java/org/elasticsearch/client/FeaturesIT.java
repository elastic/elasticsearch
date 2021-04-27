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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
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

        // need superuser privileges to execute the reset
        RestHighLevelClient adminHighLevelClient = new RestHighLevelClient(
            adminClient(),
            (client) -> {},
            new SearchModule(Settings.EMPTY, true, Collections.emptyList()).getNamedXContents());
        ResetFeaturesResponse response = execute(request,
            adminHighLevelClient.features()::resetFeatures,
            adminHighLevelClient.features()::resetFeaturesAsync);

        assertThat(response, notNullValue());
        assertThat(response.getFeatureResetStatuses(), notNullValue());
        assertThat(response.getFeatureResetStatuses().size(), greaterThan(1));
        assertTrue(response.getFeatureResetStatuses().stream().anyMatch(
            feature -> "tasks".equals(feature.getFeatureName()) && "SUCCESS".equals(feature.getStatus())));

        Set<String> statuses = response.getFeatureResetStatuses().stream()
            .map(ResetFeaturesResponse.ResetFeatureStateStatus::getStatus)
            .collect(Collectors.toSet());

        assertThat(statuses, contains("SUCCESS"));
    }
}
