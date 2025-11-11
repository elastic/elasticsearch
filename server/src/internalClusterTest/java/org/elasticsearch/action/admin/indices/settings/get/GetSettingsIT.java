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

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class GetSettingsIT extends ESIntegTestCase {
    public void testGetDefaultValueWhenDependentOnOtherSettings() {
        final String indexName = "test-index-1";

        String[][] expectedSettings = {
            { "index.mapping.source.mode", "SYNTHETIC" },
            { "index.mapping.total_fields.ignore_dynamic_beyond_limit", "true" },
            { "index.recovery.use_synthetic_source", "true" },
            { "index.use_time_series_doc_values_format", "true" },
            { "index.mapping.ignore_above", "8191" },
            { "index.sort.field", "[@timestamp]" },
            { "index.sort.order", "[desc]" },
            { "index.sort.mode", "[max]" },
            { "index.sort.missing", "[_last]" } };

        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("index.mode", "logsdb")).get());
        for (String[] expectedSetting : expectedSettings) {
            GetSettingsResponse unfilteredResponse = indicesAdmin().getSettings(
                new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(indexName).includeDefaults(true)
            ).actionGet();
            assertThat(unfilteredResponse.getSetting(indexName, expectedSetting[0]), equalTo(expectedSetting[1]));
            GetSettingsResponse filteredResponse = indicesAdmin().getSettings(
                new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(indexName).includeDefaults(true).names(expectedSetting[0])
            ).actionGet();
            assertThat(filteredResponse.getSetting(indexName, expectedSetting[0]), equalTo(expectedSetting[1]));
        }

    }
}
