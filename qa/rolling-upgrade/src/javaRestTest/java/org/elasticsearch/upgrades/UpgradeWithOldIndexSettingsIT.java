/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.rest.RestTestLegacyFeatures;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.is;

public class UpgradeWithOldIndexSettingsIT extends ParameterizedRollingUpgradeTestCase {

    public UpgradeWithOldIndexSettingsIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    private static final String INDEX_NAME = "test_index_old_settings";
    private static final String EXPECTED_WARNING = "[index.indexing.slowlog.level] setting was deprecated in Elasticsearch and will "
        + "be removed in a future release! See the breaking changes documentation for the next major version.";

    private static final String EXPECTED_V8_WARNING = "[index.indexing.slowlog.level] setting was deprecated in the previous Elasticsearch"
        + " release and is removed in this release.";

    public void testOldIndexSettings() throws Exception {
        if (isOldCluster()) {
            Request createTestIndex = new Request("PUT", "/" + INDEX_NAME);
            createTestIndex.setJsonEntity("{\"settings\": {\"index.indexing.slowlog.level\": \"WARN\"}}");
            createTestIndex.setOptions(expectWarnings(EXPECTED_WARNING));
            if (oldClusterHasFeature(RestTestLegacyFeatures.INDEXING_SLOWLOG_LEVEL_SETTING_REMOVED)) {
                assertTrue(
                    expectThrows(ResponseException.class, () -> client().performRequest(createTestIndex)).getMessage()
                        .contains("unknown setting [index.indexing.slowlog.level]")
                );

                Request createTestIndex1 = new Request("PUT", "/" + INDEX_NAME);
                client().performRequest(createTestIndex1);
            } else {
                // create index with settings no longer valid in 8.0
                client().performRequest(createTestIndex);
            }

            // add some data
            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            if (oldClusterHasFeature(RestTestLegacyFeatures.INDEXING_SLOWLOG_LEVEL_SETTING_REMOVED) == false) {
                bulk.setOptions(expectWarnings(EXPECTED_WARNING));
            }
            bulk.setJsonEntity(Strings.format("""
                {"index": {"_index": "%s"}}
                {"f1": "v1", "f2": "v2"}
                """, INDEX_NAME));
            client().performRequest(bulk);
        } else if (isMixedCluster()) {
            // add some more data
            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            if (oldClusterHasFeature(RestTestLegacyFeatures.INDEXING_SLOWLOG_LEVEL_SETTING_REMOVED) == false) {
                bulk.setOptions(expectWarnings(EXPECTED_WARNING));
            }
            bulk.setJsonEntity(Strings.format("""
                {"index": {"_index": "%s"}}
                {"f1": "v3", "f2": "v4"}
                """, INDEX_NAME));
            client().performRequest(bulk);
        } else {
            if (oldClusterHasFeature(RestTestLegacyFeatures.INDEXING_SLOWLOG_LEVEL_SETTING_REMOVED) == false) {
                Request createTestIndex = new Request("PUT", "/" + INDEX_NAME + "/_settings");
                // update index settings should work
                createTestIndex.setJsonEntity("{\"index.indexing.slowlog.level\": \"INFO\"}");
                createTestIndex.setOptions(expectWarnings(EXPECTED_V8_WARNING));
                client().performRequest(createTestIndex);

                // ensure we were able to change the setting, despite it having no effect
                Request indexSettingsRequest = new Request("GET", "/" + INDEX_NAME + "/_settings");
                Map<String, Object> response = entityAsMap(client().performRequest(indexSettingsRequest));

                var slowLogLevel = (String) (XContentMapValues.extractValue(
                    INDEX_NAME + ".settings.index.indexing.slowlog.level",
                    response
                ));

                // check that we can read our old index settings
                assertThat(slowLogLevel, is("INFO"));
            }
            assertCount(INDEX_NAME, 2);
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
        if (oldClusterHasFeature(RestTestLegacyFeatures.DEPRECATION_WARNINGS_LEAK_FIXED) == false) {
            // There is a bug (fixed in 7.17.9 and 8.7.0 where deprecation warnings could leak into ClusterApplierService#applyChanges)
            // Below warnings are set (and leaking) from an index in this test case
            request.setOptions(expectVersionSpecificWarnings(v -> {
                v.compatible(
                    "[index.indexing.slowlog.level] setting was deprecated in Elasticsearch and will be removed in a future release! "
                        + "See the breaking changes documentation for the next major version."
                );
            }));
        }
        client().performRequest(request);
    }
}
