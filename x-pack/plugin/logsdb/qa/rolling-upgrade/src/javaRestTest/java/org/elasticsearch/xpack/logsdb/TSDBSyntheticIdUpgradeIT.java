/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexFeatures;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

public class TSDBSyntheticIdUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {
    private static final int DOC_COUNT = 10;

    public void testRollingUpgrade() throws IOException {
        int numNodes = getCluster().getNumNodes();
        boolean isServerless = isServerless();

        if (oldClusterHasFeature(IndexFeatures.TIME_SERIES_SYNTHETIC_ID)) {
            // Should be able to create synthetic id index throughout the rolling upgrade
            for (int i = 0; i < numNodes; i++) {
                assertWriteIndex(indexName(i));
                for (int j = 0; j <= i; j++) {
                    assertIndexRead(indexName(j), isServerless);
                }
                upgradeNode(i);
            }
            assertWriteIndex(indexName(numNodes));
            for (int j = 0; j <= numNodes; j++) {
                assertIndexRead(indexName(j), isServerless);
            }
        } else {
            // Cluster supports synthetic id index after all nodes have been upgraded, not before
            for (int i = 0; i < numNodes; i++) {
                assertNoWriteIndex(indexName(i));
                upgradeNode(i);
            }
            assertWriteIndex(indexName(numNodes));
            assertIndexRead(indexName(numNodes), isServerless);
        }
    }

    private static void assertWriteIndex(String indexName) throws IOException {
        assertIndexCanBeCreated(indexName);
        assertCanAddDocuments(indexName);
    }

    private static void assertIndexRead(String indexName, boolean isServerless) throws IOException {
        assertTrue("Expected index [" + indexName + "] to exist, but did not", indexExists(indexName));
        Map<String, Object> indexSettingsAsMap = getIndexSettingsAsMap(indexName);
        assertThat(indexSettingsAsMap.get(IndexSettings.SYNTHETIC_ID.getKey()), Matchers.equalTo("true"));
        assertDocCount(client(), indexName, DOC_COUNT);
        if (!isServerless) {
            assertThat(invertedIndexSize(indexName), Matchers.equalTo(0));
        }
    }

    private static int invertedIndexSize(String indexName) throws IOException {
        var diskUsage = new Request("POST", "/" + indexName + "/_disk_usage?run_expensive_tasks=true");
        Response response = client().performRequest(diskUsage);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        return objectPath.evaluate(indexName + ".all_fields.inverted_index.total_in_bytes");
    }

    private static void assertIndexCanBeCreated(String indexName) throws IOException {
        CreateIndexResponse response = null;
        try {
            response = createSyntheticIdIndex(indexName);
            assertTrue("Expected index [" + indexName + "] to be created successfully, but was not", response.isAcknowledged());
            assertTrue(
                "Expected shards of index [" + indexName + "] to be created successfully, but was not",
                response.isShardsAcknowledged()
            );
        } finally {
            if (response != null) {
                response.decRef();
            }
        }
    }

    private static void assertCanAddDocuments(String indexName) throws IOException {
        StringJoiner joiner = new StringJoiner("\n", "", "\n");
        Instant now = Instant.now();
        for (int i = 0; i < DOC_COUNT; i++) {
            addDocument(joiner, now.plus(i, ChronoUnit.SECONDS));
        }
        var request = new Request("PUT", "/" + indexName + "/_bulk");
        request.setJsonEntity(joiner.toString());
        request.addParameter("refresh", "true");
        Response response = client().performRequest(request);
        assertOK(response);
    }

    private static void addDocument(StringJoiner joiner, Instant timestamp) {
        joiner.add("{\"create\": {}}");
        joiner.add(String.format(Locale.ROOT, """
            {"@timestamp": "%s", "hostname": "host", "metric": {"field": "cpu-load", "value": %d}}
            """, timestamp, randomByte()));
    }

    private static void assertNoWriteIndex(String indexName) throws IOException {
        String setting = IndexSettings.SYNTHETIC_ID.getKey();
        String unknownSetting = "unknown setting [" + setting + "]";
        String versionTooLow = String.format(
            Locale.ROOT,
            "The setting [%s] is only permitted for indexVersion [%s] or later. Current indexVersion:",
            setting,
            IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94
        );

        ResponseException e = assertThrows(ResponseException.class, () -> createSyntheticIdIndex(indexName));
        assertThat(e.getMessage(), Matchers.either(Matchers.containsString(unknownSetting)).or(Matchers.containsString(versionTooLow)));
        String reason = ObjectPath.createFromResponse(e.getResponse()).evaluate("error.reason");

        assertThat(reason, Matchers.either(Matchers.containsString(unknownSetting)).or(Matchers.containsString(versionTooLow)));
        assertThat(e.getMessage(), Matchers.containsString("illegal_argument_exception"));
    }

    private static CreateIndexResponse createSyntheticIdIndex(String indexName) throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.SYNTHETIC_ID.getKey(), true)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
            .build();
        final var mapping = """
            {
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "hostname": {
                        "type": "keyword",
                        "time_series_dimension": true
                    },
                    "metric": {
                        "properties": {
                            "field": {
                                "type": "keyword",
                                "time_series_dimension": true
                            },
                            "value": {
                                "type": "integer",
                                "time_series_metric": "counter"
                            }
                        }
                    }
                }
            }
            """;
        return createIndex(indexName, settings, mapping);
    }

    private static String indexName(int i) {
        return "index_" + i;
    }

    private static boolean isServerless() throws IOException {
        Map<String, Map<?, ?>> nodesInfo = getNodesInfo(adminClient());
        List<?> buildFlavors = nodesInfo.values().stream().map(nodeInfoMap -> nodeInfoMap.get("build_flavor")).distinct().toList();
        assertThat(buildFlavors.size(), Matchers.equalTo(1));
        String buildFlavor = ESRestTestCase.asInstanceOf(String.class, buildFlavors.getFirst());
        return "serverless".equals(buildFlavor);
    }
}
