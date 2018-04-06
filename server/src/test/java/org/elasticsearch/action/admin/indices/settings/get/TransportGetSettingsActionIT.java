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
}
