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

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

public class TransportGetSettingsActionIT extends ESIntegTestCase {
    public void testIncludeDefaults() {
        String indexName = "test_index";
        createIndex(indexName, Settings.EMPTY);
        GetSettingsRequest noDefaultsRequest = new GetSettingsRequest().indices(indexName);
        GetSettingsResponse noDefaultsResponse = client().admin().indices().getSettings(noDefaultsRequest).actionGet();
        assertNull("index.refresh_interval should be null as it was never set", noDefaultsResponse.getSetting(indexName,
            "index.refresh_interval"));

        GetSettingsRequest defaultsRequest = new GetSettingsRequest().indices(indexName).includeDefaults(true);
        GetSettingsResponse defaultsResponse = client().admin().indices().getSettings(defaultsRequest).actionGet();
        assertNotNull("index.refresh_interval should be set as we are including defaults", defaultsResponse.getSetting(indexName,
            "index.refresh_interval"));
    }
    public void testIncludeDefaultsWithFiltering() {
        String indexName = "test_index";
        createIndex(indexName, Settings.EMPTY);
        GetSettingsRequest defaultsRequest = new GetSettingsRequest().indices(indexName).includeDefaults(true)
            .names("index.refresh_interval");
        GetSettingsResponse defaultsResponse = client().admin().indices().getSettings(defaultsRequest).actionGet();
        assertNotNull("index.refresh_interval should be set as we are including defaults", defaultsResponse.getSetting(indexName,
            "index.refresh_interval"));
        assertNull("index.number_of_shards should be null as this query is filtered",
            defaultsResponse.getSetting(indexName, "index.number_of_shards"));
        assertNull("index.warmer.enabled should be null as this query is filtered",
            defaultsResponse.getSetting(indexName, "index.warmer.enabled"));
    }
}
