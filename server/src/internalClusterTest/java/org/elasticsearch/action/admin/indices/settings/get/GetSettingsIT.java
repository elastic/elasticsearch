/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.LambdaMatchers;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class GetSettingsIT extends ESIntegTestCase {
    public void testGetDefaultValueWhenDependentOnOtherSettings() {
        final String indexName = "test-index-1";

        Settings expectedSettings = Settings.builder()
            .put("index.mapping.source.mode", "SYNTHETIC")
            .put("index.mapping.total_fields.ignore_dynamic_beyond_limit", true)
            .put("index.recovery.use_synthetic_source", true)
            .put("index.use_time_series_doc_values_format", true)
            .put("index.mapping.ignore_above", 8191)
            .putList("index.sort.field", "@timestamp")
            .putList("index.sort.order", "desc")
            .putList("index.sort.mode", "max")
            .putList("index.sort.missing", "_last")
            .build();

        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("index.mode", "logsdb")).get());
        GetSettingsResponse unfilteredResponse = indicesAdmin().getSettings(
            new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(indexName).includeDefaults(true)
        ).actionGet();
        for (String key : expectedSettings.keySet()) {
            assertThat(unfilteredResponse.getSetting(indexName, key), equalTo(expectedSettings.get(key)));
            GetSettingsResponse filteredResponse = indicesAdmin().getSettings(
                new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(indexName).includeDefaults(true).names(key)
            ).actionGet();

            var expectedFilteredSettingsMap = Map.of(indexName, expectedSettings.filter(key::equals));
            assertThat(
                filteredResponse,
                LambdaMatchers.transformedMatch(GetSettingsResponse::getIndexToDefaultSettings, equalTo(expectedFilteredSettingsMap))
            );
        }

    }
}
