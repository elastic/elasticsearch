/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.is;

public class UpgradeWithOldIndexSettingsIT extends AbstractRollingTestCase {

    @SuppressWarnings("unchecked")
    public void testOldIndexSettings() throws Exception {
        switch (CLUSTER_TYPE) {
            case OLD -> {
                // create index with settings no longer valid
                Request createTestIndex = new Request("PUT", "/test_index_old");
                createTestIndex.setJsonEntity("{\"settings\": {\"index.indexing.slowlog.level\": \"INFO\"}}");
                client().performRequest(createTestIndex);

                // add some data
                Request bulk = new Request("POST", "/_bulk");
                bulk.addParameter("refresh", "true");
                bulk.setJsonEntity("""
                    {"index": {"_index": "test_index_old"}}
                    {"f1": "v1", "f2": "v2"}
                    """);
                client().performRequest(bulk);
            }
            case MIXED -> {
                // add some more data
                Request bulk = new Request("POST", "/_bulk");
                bulk.addParameter("refresh", "true");
                bulk.setJsonEntity("""
                    {"index": {"_index": "test_index_old"}}
                    {"f1": "v3", "f2": "v4"}
                    """);
                client().performRequest(bulk);
            }
            case UPGRADED -> {
                Request indexSettingsRequest = new Request("GET", "/test_index_old/_settings");
                Map<String, Object> response = entityAsMap(client().performRequest(indexSettingsRequest));

                Map<?, ?> slowLog = (Map<?, ?>) ((List<?>) (XContentMapValues.extractValue("settings.index.indexing.slowlog", response)))
                    .get(0);

                // Make sure our non-system index is still non-system
                assertThat(slowLog.get("level"), is("INFO"));
                assertCount("test_index_old", 2);
            }
        }
    }

    private void assertCount(String index, int count) throws IOException {
        Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        searchTestIndexRequest.addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        assertEquals(
            "{\"hits\":{\"total\":" + count + "}}",
            EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8)
        );
    }
}
