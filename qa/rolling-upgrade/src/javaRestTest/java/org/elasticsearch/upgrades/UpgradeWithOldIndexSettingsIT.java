/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class UpgradeWithOldIndexSettingsIT extends AbstractRollingUpgradeTestCase {

    public UpgradeWithOldIndexSettingsIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    private static final String INDEX_NAME = "test_index_old_settings";
    private static final String EXPECTED_WARNING = "[index.indexing.slowlog.level] setting was deprecated in Elasticsearch and will "
        + "be removed in a future release! See the breaking changes documentation for the next major version.";

    public void testOldIndexSettings() throws Exception {
        if (isOldCluster()) {
            Request createTestIndex = new Request("PUT", "/" + INDEX_NAME);
            createTestIndex.setJsonEntity("{\"settings\": {\"index.indexing.slowlog.level\": \"WARN\"}}");
            createTestIndex.setOptions(expectWarnings(EXPECTED_WARNING));
            assertTrue(
                expectThrows(ResponseException.class, () -> client().performRequest(createTestIndex)).getMessage()
                    .contains("unknown setting [index.indexing.slowlog.level]")
            );

            Request createTestIndex1 = new Request("PUT", "/" + INDEX_NAME);
            client().performRequest(createTestIndex1);

            // add some data
            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity(Strings.format("""
                {"index": {"_index": "%s"}}
                {"f1": "v1", "f2": "v2"}
                """, INDEX_NAME));
            client().performRequest(bulk);
        } else if (isMixedCluster()) {
            // add some more data
            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity(Strings.format("""
                {"index": {"_index": "%s"}}
                {"f1": "v3", "f2": "v4"}
                """, INDEX_NAME));
            client().performRequest(bulk);
        } else {
            assertCount(INDEX_NAME, 2);
        }
    }

    public void testMapperDynamicIndexSetting() throws IOException {
        assumeTrue(
            "Setting deprecated in 6.x, but was disallowed/removed incorrectly in some 7.x versions and can only be set safely in 7.17.22. "
                + "Setting can't be used in 8.x ",
            getOldClusterTestVersion().before("8.0.0") && getOldClusterTestVersion().after("7.17.21")
        );
        String indexName = "my-index";
        if (isOldCluster()) {
            createIndex(indexName);
            Request request = new Request("PUT", "/" + indexName + "/_settings");
            request.setJsonEntity(org.elasticsearch.common.Strings.toString(Settings.builder().put("index.mapper.dynamic", true).build()));
            request.setOptions(
                expectWarnings(
                    "[index.mapper.dynamic] setting was deprecated in Elasticsearch and will be removed in a future release! "
                        + "See the breaking changes documentation for the next major version."
                )
            );
            assertOK(client().performRequest(request));
        } else {
            if (isUpgradedCluster()) {
                var indexSettings = getIndexSettings(indexName);
                assertThat(XContentMapValues.extractValue(indexName + ".settings.index.mapper.dynamic", indexSettings), equalTo("true"));
                ensureGreen(indexName);
                // New indices can never define the index.mapper.dynamic setting.
                Exception e = expectThrows(
                    ResponseException.class,
                    () -> createIndex("my-index2", Settings.builder().put("index.mapper.dynamic", true).build())
                );
                assertThat(e.getMessage(), containsString("unknown setting [index.mapper.dynamic]"));
            }
        }
    }

    private void assertCount(String index, int countAtLeast) throws IOException {
        Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        searchTestIndexRequest.addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        Map<String, Object> response = entityAsMap(searchTestIndexResponse);

        var hitsTotal = (Integer) (XContentMapValues.extractValue("hits.total", response));

        assertTrue(hitsTotal >= countAtLeast);
    }

    public static void updateIndexSettingsPermittingSlowlogDeprecationWarning(String index, Settings.Builder settings) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(org.elasticsearch.common.Strings.toString(settings.build()));
        client().performRequest(request);
    }
}
