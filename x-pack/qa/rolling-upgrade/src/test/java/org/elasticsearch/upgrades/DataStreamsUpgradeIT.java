/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.upgrades.IndexingIT.assertCount;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamsUpgradeIT extends AbstractUpgradeTestCase {

    public void testDataStreams() throws IOException {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            String requestBody = """
                {
                  "index_patterns": [ "logs-*" ],
                  "template": {
                    "mappings": {
                      "properties": {
                        "@timestamp": {
                          "type": "date"
                        }
                      }
                    }
                  },
                  "data_stream": {}
                }""";
            Request request = new Request("PUT", "/_index_template/1");
            request.setJsonEntity(requestBody);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(request);
            client().performRequest(request);

            StringBuilder b = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                b.append(Strings.format("""
                    {"create":{"_index":"logs-foobar"}}
                    {"@timestamp":"2020-12-12","test":"value%s"}
                    """, i));
            }
            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.addParameter("filter_path", "errors");
            bulk.setJsonEntity(b.toString());
            Response response = client().performRequest(bulk);
            assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            long nowMillis = System.currentTimeMillis();
            Request rolloverRequest = new Request("POST", "/logs-foobar/_rollover");
            client().performRequest(rolloverRequest);

            Request index = new Request("POST", "/logs-foobar/_doc");
            index.addParameter("refresh", "true");
            index.addParameter("filter_path", "_index");
            if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                // include legacy name and date-named indices with today +/-1 in case of clock skew
                var expectedIndices = List.of(
                    "{\"_index\":\"" + DataStreamTestHelper.getLegacyDefaultBackingIndexName("logs-foobar", 2) + "\"}",
                    "{\"_index\":\"" + DataStream.getDefaultBackingIndexName("logs-foobar", 2, nowMillis) + "\"}",
                    "{\"_index\":\"" + DataStream.getDefaultBackingIndexName("logs-foobar", 2, nowMillis + 86400000) + "\"}",
                    "{\"_index\":\"" + DataStream.getDefaultBackingIndexName("logs-foobar", 2, nowMillis - 86400000) + "\"}"
                );
                index.setJsonEntity("{\"@timestamp\":\"2020-12-12\",\"test\":\"value1000\"}");
                Response response = client().performRequest(index);
                assertThat(expectedIndices, Matchers.hasItem(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)));
            } else {
                // include legacy name and date-named indices with today +/-1 in case of clock skew
                var expectedIndices = List.of(
                    "{\"_index\":\"" + DataStreamTestHelper.getLegacyDefaultBackingIndexName("logs-foobar", 3) + "\"}",
                    "{\"_index\":\"" + DataStream.getDefaultBackingIndexName("logs-foobar", 3, nowMillis) + "\"}",
                    "{\"_index\":\"" + DataStream.getDefaultBackingIndexName("logs-foobar", 3, nowMillis + 86400000) + "\"}",
                    "{\"_index\":\"" + DataStream.getDefaultBackingIndexName("logs-foobar", 3, nowMillis - 86400000) + "\"}"
                );
                index.setJsonEntity("{\"@timestamp\":\"2020-12-12\",\"test\":\"value1001\"}");
                Response response = client().performRequest(index);
                assertThat(expectedIndices, Matchers.hasItem(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)));
            }
        }

        final int expectedCount;
        if (CLUSTER_TYPE.equals(ClusterType.OLD)) {
            expectedCount = 1000;
        } else if (CLUSTER_TYPE.equals(ClusterType.MIXED)) {
            if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                expectedCount = 1001;
            } else {
                expectedCount = 1002;
            }
        } else if (CLUSTER_TYPE.equals(ClusterType.UPGRADED)) {
            expectedCount = 1002;
        } else {
            throw new AssertionError("unexpected cluster type");
        }
        assertCount("logs-foobar", expectedCount);
    }

    public void testDataStreamValidationDoesNotBreakUpgrade() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            String requestBody = """
                {
                  "index_patterns": [ "logs-*" ],
                  "template": {
                    "mappings": {
                      "properties": {
                        "@timestamp": {
                          "type": "date"
                        }
                      }
                    }
                  },
                  "data_stream": {}
                }""";
            Request request = new Request("PUT", "/_index_template/1");
            request.setJsonEntity(requestBody);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(request);
            client().performRequest(request);

            String b = """
                {"create":{"_index":"logs-barbaz"}}
                {"@timestamp":"2020-12-12","test":"value0"}
                {"create":{"_index":"logs-barbaz-2021.01.13"}}
                {"@timestamp":"2020-12-12","test":"value0"}
                """;

            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.addParameter("filter_path", "errors");
            bulk.setJsonEntity(b);
            Response response = client().performRequest(bulk);
            assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));

            Request rolloverRequest = new Request("POST", "/logs-barbaz-2021.01.13/_rollover");
            client().performRequest(rolloverRequest);
        } else {
            if (CLUSTER_TYPE == ClusterType.MIXED) {
                ensureHealth((request -> {
                    request.addParameter("timeout", "70s");
                    request.addParameter("wait_for_nodes", "3");
                    request.addParameter("wait_for_status", "yellow");
                }));
            } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
                ensureHealth("logs-barbaz", (request -> {
                    request.addParameter("wait_for_nodes", "3");
                    request.addParameter("wait_for_status", "green");
                    request.addParameter("timeout", "70s");
                    request.addParameter("level", "shards");
                }));
            }
            assertCount("logs-barbaz", 1);
            assertCount("logs-barbaz-2021.01.13", 1);
        }
    }

    public void testUpgradeDataStream() throws Exception {
        /*
         * This test tests upgrading a "normal" data stream (dataStreamName), and upgrading a data stream that was originally just an
         * ordinary index that was converted to a data stream (dataStreamFromNonDataStreamIndices).
         */
        String dataStreamName = "reindex_test_data_stream";
        String dataStreamFromNonDataStreamIndices = "index_first_reindex_test_data_stream";
        int numRollovers = randomIntBetween(0, 5);
        boolean hasILMPolicy = randomBoolean();
        boolean ilmEnabled = hasILMPolicy && randomBoolean();

        if (ilmEnabled) {
            startILM();
        } else {
            stopILM();
        }

        if (CLUSTER_TYPE == ClusterType.OLD) {
            createAndRolloverDataStream(dataStreamName, numRollovers, hasILMPolicy, ilmEnabled);
            createDataStreamFromNonDataStreamIndices(dataStreamFromNonDataStreamIndices);
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            Map<String, Map<String, Object>> oldIndicesMetadata = getIndicesMetadata(dataStreamName);
            upgradeDataStream(dataStreamName, numRollovers, numRollovers + 1, 0, ilmEnabled);
            cancelReindexTask(dataStreamName);
            upgradeDataStream(dataStreamFromNonDataStreamIndices, 0, 1, 0, ilmEnabled);
            cancelReindexTask(dataStreamFromNonDataStreamIndices);
            Map<String, Map<String, Object>> upgradedIndicesMetadata = getIndicesMetadata(dataStreamName);

            if (ilmEnabled) {
                checkILMPhase(dataStreamName, upgradedIndicesMetadata);
            } else {
                compareIndexMetadata(oldIndicesMetadata, upgradedIndicesMetadata);
            }
        }
    }

    public void testMigrateDoesNotRestartOnUpgrade() throws Exception {
        /*
         * This test makes sure that if reindex is run and completed, then when the cluster is upgraded the task
         * does not begin running again.
         */
        String dataStreamName = "reindex_test_data_stream_ugprade_test";
        int numRollovers = randomIntBetween(0, 5);
        boolean hasILMPolicy = randomBoolean();
        boolean ilmEnabled = hasILMPolicy && randomBoolean();
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createAndRolloverDataStream(dataStreamName, numRollovers, hasILMPolicy, ilmEnabled);
            upgradeDataStream(dataStreamName, numRollovers, numRollovers + 1, 0, ilmEnabled);
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            makeSureNoUpgrade(dataStreamName);
            cancelReindexTask(dataStreamName);
        } else {
            makeSureNoUpgrade(dataStreamName);
        }
    }

    private void cancelReindexTask(String dataStreamName) throws IOException {
        Request cancelRequest = new Request("POST", "_migration/reindex/" + dataStreamName + "/_cancel");
        String upgradeUser = "upgrade_user";
        String upgradeUserPassword = "x-pack-test-password";
        createRole("upgrade_role", dataStreamName);
        createUser(upgradeUser, upgradeUserPassword, "upgrade_role");
        try (RestClient upgradeUserClient = getClient(upgradeUser, upgradeUserPassword)) {
            Response cancelResponse = upgradeUserClient.performRequest(cancelRequest);
            assertOK(cancelResponse);
        }
    }

    private void compareIndexMetadata(
        Map<String, Map<String, Object>> oldIndicesMetadata,
        Map<String, Map<String, Object>> upgradedIndicesMetadata
    ) {
        String oldWriteIndex = getWriteIndexFromDataStreamIndexMetadata(oldIndicesMetadata);
        for (Map.Entry<String, Map<String, Object>> upgradedIndexEntry : upgradedIndicesMetadata.entrySet()) {
            String upgradedIndexName = upgradedIndexEntry.getKey();
            if (upgradedIndexName.startsWith(".migrated-")) {
                String oldIndexName = "." + upgradedIndexName.substring(".migrated-".length());
                Map<String, Object> oldIndexMetadata = oldIndicesMetadata.get(oldIndexName);
                Map<String, Object> upgradedIndexMetadata = upgradedIndexEntry.getValue();
                compareSettings(oldIndexMetadata, upgradedIndexMetadata);
                compareMappings((Map<?, ?>) oldIndexMetadata.get("mappings"), (Map<?, ?>) upgradedIndexMetadata.get("mappings"));
                assertThat("ILM states did not match", upgradedIndexMetadata.get("ilm"), equalTo(oldIndexMetadata.get("ilm")));
                if (oldIndexName.equals(oldWriteIndex) == false) { // the old write index will have been rolled over by upgrade
                    assertThat(
                        "Rollover info did not match",
                        upgradedIndexMetadata.get("rollover_info"),
                        equalTo(oldIndexMetadata.get("rollover_info"))
                    );
                }
                assertThat(upgradedIndexMetadata.get("system"), equalTo(oldIndexMetadata.get("system")));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void checkILMPhase(String dataStreamName, Map<String, Map<String, Object>> upgradedIndicesMetadata) throws Exception {
        var writeIndex = getWriteIndexFromDataStreamIndexMetadata(upgradedIndicesMetadata);
        assertBusy(() -> {

            Request request = new Request("GET", dataStreamName + "/_ilm/explain");
            Response response = client().performRequest(request);
            Map<String, Object> responseMap = XContentHelper.convertToMap(
                JsonXContent.jsonXContent,
                response.getEntity().getContent(),
                false
            );
            Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
            for (var index : indices.keySet()) {
                if (index.equals(writeIndex) == false) {
                    Map<String, Object> ilmInfo = (Map<String, Object>) indices.get(index);
                    assertThat("Index has not moved to cold ILM phase", ilmInfo.get("phase"), equalTo("cold"));
                }
            }
        }, 30, TimeUnit.SECONDS);
    }

    private String getWriteIndexFromDataStreamIndexMetadata(Map<String, Map<String, Object>> indexMetadataForDataStream) {
        return indexMetadataForDataStream.entrySet()
            .stream()
            .sorted((o1, o2) -> Long.compare(getCreationDate(o2.getValue()), getCreationDate(o1.getValue())))
            .map(Map.Entry::getKey)
            .findFirst()
            .get();
    }

    private void startILM() throws IOException {
        setILMInterval();
        var request = new Request("POST", "/_ilm/start");
        assertOK(client().performRequest(request));
    }

    private void stopILM() throws IOException {
        var request = new Request("POST", "/_ilm/stop");
        assertOK(client().performRequest(request));
    }

    private void setILMInterval() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("""
            { "persistent": {"indices.lifecycle.poll_interval": "1s"} }
            """);
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    long getCreationDate(Map<String, Object> indexMetadata) {
        return Long.parseLong(
            (String) ((Map<String, Map<String, Object>>) indexMetadata.get("settings")).get("index").get("creation_date")
        );
    }

    private void compareSettings(Map<String, Object> oldIndexMetadata, Map<String, Object> upgradedIndexMetadata) {
        Map<String, Object> oldIndexSettings = getIndexSettingsFromIndexMetadata(oldIndexMetadata);
        Map<String, Object> upgradedIndexSettings = getIndexSettingsFromIndexMetadata(upgradedIndexMetadata);
        final Set<String> SETTINGS_TO_CHECK = Set.of(
            "lifecycle",
            "mode",
            "routing",
            "hidden",
            "number_of_shards",
            "creation_date",
            "number_of_replicas"
        );
        for (String setting : SETTINGS_TO_CHECK) {
            assertThat(
                "Unexpected value for setting " + setting,
                upgradedIndexSettings.get(setting),
                equalTo(oldIndexSettings.get(setting))
            );
        }
    }

    private void compareMappings(Map<?, ?> oldMappings, Map<?, ?> upgradedMappings) {
        boolean ignoreSource = Version.fromString(UPGRADE_FROM_VERSION).before(Version.V_9_0_0);
        if (ignoreSource) {
            Map<?, ?> doc = (Map<?, ?>) oldMappings.get("_doc");
            if (doc != null) {
                Map<?, ?> sourceEntry = (Map<?, ?>) doc.get("_source");
                if (sourceEntry != null && sourceEntry.isEmpty()) {
                    doc.remove("_source");
                }
                assert doc.containsKey("_source") == false;
            }
        }
        assertThat("Mappings did not match", upgradedMappings, equalTo(oldMappings));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getIndexSettingsFromIndexMetadata(Map<String, Object> indexMetadata) {
        return (Map<String, Object>) ((Map<String, Object>) indexMetadata.get("settings")).get("index");
    }

    private void createAndRolloverDataStream(String dataStreamName, int numRollovers, boolean hasILMPolicy, boolean ilmEnabled)
        throws IOException {
        if (hasILMPolicy) {
            createIlmPolicy();
        }
        // We want to create a data stream and roll it over several times so that we have several indices to upgrade
        String template = """
            {
                "settings":{
                    "index": {
                        $ILM_SETTING
                        "mode": "standard"
                    }
                },
                $DSL_TEMPLATE
                "mappings":{
                    "dynamic_templates": [
                        {
                            "labels": {
                                "path_match": "pod.labels.*",
                                "mapping": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    ],
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "metricset": {
                            "type": "keyword"
                        },
                        "k8s": {
                            "properties": {
                                "pod": {
                                    "properties": {
                                        "name": {
                                            "type": "keyword"
                                        },
                                        "network": {
                                            "properties": {
                                                "tx": {
                                                    "type": "long"
                                                },
                                                "rx": {
                                                    "type": "long"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """;
        if (hasILMPolicy) {
            template = template.replace("$ILM_SETTING", """
                "lifecycle.name": "test-lifecycle-policy",
                """);
            template = template.replace("$DSL_TEMPLATE", "");
        } else {
            template = template.replace("$ILM_SETTING", "");
            template = template.replace("$DSL_TEMPLATE", """
                    "lifecycle": {
                      "data_retention": "7d"
                    },
                """);
        }
        final String indexTemplate = """
            {
                "index_patterns": ["$PATTERN"],
                "template": $TEMPLATE,
                "data_stream": {
                }
            }""";
        var putIndexTemplateRequest = new Request(
            "POST",
            "/_index_template/reindex_test_data_stream_template" + randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT)
        );
        putIndexTemplateRequest.setJsonEntity(indexTemplate.replace("$TEMPLATE", template).replace("$PATTERN", dataStreamName));
        assertOK(client().performRequest(putIndexTemplateRequest));
        bulkLoadData(dataStreamName);
        for (int i = 0; i < numRollovers; i++) {
            String oldIndexName = rollover(dataStreamName);
            if (ilmEnabled == false && randomBoolean()) {
                closeIndex(oldIndexName);
            }
            bulkLoadData(dataStreamName);
        }
    }

    private static void createIlmPolicy() throws IOException {
        String ilmPolicy = """
            {
              "policy": {
                "phases": {
                  "warm": {
                    "min_age": "1s",
                    "actions": {
                      "forcemerge": {
                        "max_num_segments": 1
                      }
                    }
                  },
                  "cold": {
                    "actions": {
                      "set_priority" : {
                        "priority": 50
                      }
                    }
                  }
                }
              }
            }""";
        Request putIlmPolicyRequest = new Request("PUT", "_ilm/policy/test-lifecycle-policy");
        putIlmPolicyRequest.setJsonEntity(ilmPolicy);
        assertOK(client().performRequest(putIlmPolicyRequest));
    }

    /*
     * This returns a Map of index metadata for each index in the data stream, as retrieved from the cluster state.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> getIndicesMetadata(String dataStreamName) throws IOException {
        Request getClusterStateRequest = new Request("GET", "/_cluster/state/metadata/" + dataStreamName);
        Response clusterStateResponse = client().performRequest(getClusterStateRequest);
        Map<String, Object> clusterState = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            clusterStateResponse.getEntity().getContent(),
            false
        );
        return ((Map<String, Map<String, Map<String, Object>>>) clusterState.get("metadata")).get("indices");
    }

    private void createDataStreamFromNonDataStreamIndices(String dataStreamFromNonDataStreamIndices) throws IOException {
        /*
         * This method creates an index, creates an alias to that index, and then converts the aliased index into a data stream. This is
         * similar to the path that many indices (including system indices) took in versions 7/8.
         */
        // First, we create an ordinary index with no @timestamp mapping:
        final String templateWithNoTimestamp = """
            {
                "mappings":{
                    "properties": {
                        "message": {
                            "type": "text"
                        }
                    }
                }
            }
            """;
        // Note that this is not a data stream template:
        final String indexTemplate = """
            {
                "index_patterns": ["$PATTERN"],
                "template": $TEMPLATE
            }""";
        var putIndexTemplateRequest = new Request("POST", "/_index_template/reindex_test_data_stream_index_template");
        putIndexTemplateRequest.setJsonEntity(
            indexTemplate.replace("$TEMPLATE", templateWithNoTimestamp).replace("$PATTERN", dataStreamFromNonDataStreamIndices + "-*")
        );
        String indexName = dataStreamFromNonDataStreamIndices + "-01";
        if (minimumTransportVersion().before(TransportVersions.V_8_0_0)) {
            /*
             * It is not possible to create a 7.x index template with a type. And you can't create an empty index with a type. But you can
             * create the index with a type by posting a document to an index with a type. We do that here so that we test that the type is
             * removed when we reindex into 8.x.
             */
            String typeName = "test-type";
            Request createIndexRequest = new Request("POST", indexName + "/" + typeName);
            createIndexRequest.setJsonEntity("""
                {
                  "@timestamp": "2099-11-15T13:12:00",
                  "message": "GET /search HTTP/1.1 200 1070000",
                  "user": {
                    "id": "kimchy"
                  }
                }""");
            createIndexRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
            assertOK(client().performRequest(createIndexRequest));
        }
        assertOK(client().performRequest(putIndexTemplateRequest));
        bulkLoadDataMissingTimestamp(indexName);
        /*
         * Next, we will change the index's mapping to include a @timestamp field since we are going to convert it to a data stream. But
         * first we have to flush the translog to disk because adding a @timestamp field will cause errors if it is done before the translog
         * is flushed:
         */
        assertOK(client().performRequest(new Request("POST", indexName + "/_flush")));
        ensureHealth(indexName, (request -> {
            request.addParameter("wait_for_nodes", "3");
            request.addParameter("wait_for_status", "green");
            request.addParameter("timeout", "70s");
            request.addParameter("level", "shards");
        }));

        // Updating the mapping to include @timestamp:
        Request updateIndexMappingRequest = new Request("PUT", indexName + "/_mapping");
        updateIndexMappingRequest.setJsonEntity("""
            {
                "properties": {
                    "@timestamp" : {
                        "type": "date"
                    },
                    "message": {
                        "type": "text"
                    }
                }
            }""");
        assertOK(client().performRequest(updateIndexMappingRequest));

        // Creating an alias with the same name that the data stream will have:
        Request createAliasRequest = new Request("POST", "/_aliases");
        String aliasRequestBody = """
            {
              "actions": [
                {
                  "add": {
                    "index": "$index",
                    "alias": "$alias"
                  }
                }
              ]
            }""";
        createAliasRequest.setJsonEntity(
            aliasRequestBody.replace("$index", indexName).replace("$alias", dataStreamFromNonDataStreamIndices)
        );
        assertOK(client().performRequest(createAliasRequest));

        // This is now just an aliased index. We'll convert it into a data stream
        final String templateWithTimestamp = """
            {
                "mappings":{
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "message": {
                            "type": "text"
                        }
                    }
                }
            }
            """;
        final String dataStreamTemplate = """
            {
                "index_patterns": ["$PATTERN"],
                "template": $TEMPLATE,
                "data_stream": {
                }
            }""";
        var putDataStreamTemplateRequest = new Request("POST", "/_index_template/reindex_test_data_stream_data_stream_template");
        putDataStreamTemplateRequest.setJsonEntity(
            dataStreamTemplate.replace("$TEMPLATE", templateWithTimestamp).replace("$PATTERN", dataStreamFromNonDataStreamIndices)
        );
        assertOK(client().performRequest(putDataStreamTemplateRequest));
        Request migrateToDataStreamRequest = new Request("POST", "/_data_stream/_migrate/" + dataStreamFromNonDataStreamIndices);
        assertOK(client().performRequest(migrateToDataStreamRequest));
    }

    @SuppressWarnings("unchecked")
    private void upgradeDataStream(
        String dataStreamName,
        int numRolloversOnOldCluster,
        int expectedSuccessesCount,
        int expectedErrorCount,
        boolean ilmEnabled
    ) throws Exception {
        Set<String> indicesNeedingUpgrade = getDataStreamIndices(dataStreamName);
        final int explicitRolloverOnNewClusterCount = randomIntBetween(0, 2);
        for (int i = 0; i < explicitRolloverOnNewClusterCount; i++) {
            String oldIndexName = rollover(dataStreamName);
            if (ilmEnabled == false && randomBoolean()) {
                closeIndex(oldIndexName);
            }
        }
        Request reindexRequest = new Request("POST", "/_migration/reindex");
        reindexRequest.setJsonEntity(Strings.format("""
            {
              "mode": "upgrade",
              "source": {
                "index": "%s"
              }
            }""", dataStreamName));

        String upgradeUser = "upgrade_user";
        String upgradeUserPassword = "x-pack-test-password";
        createRole("upgrade_role", dataStreamName);
        createUser(upgradeUser, upgradeUserPassword, "upgrade_role");
        try (RestClient upgradeUserClient = getClient(upgradeUser, upgradeUserPassword)) {
            Response reindexResponse = upgradeUserClient.performRequest(reindexRequest);
            assertOK(reindexResponse);
            assertBusy(() -> {
                Request statusRequest = new Request("GET", "_migration/reindex/" + dataStreamName + "/_status");
                Response statusResponse = upgradeUserClient.performRequest(statusRequest);
                Map<String, Object> statusResponseMap = XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    statusResponse.getEntity().getContent(),
                    false
                );
                String statusResponseString = statusResponseMap.keySet()
                    .stream()
                    .map(key -> key + "=" + statusResponseMap.get(key))
                    .collect(Collectors.joining(", ", "{", "}"));
                assertOK(statusResponse);
                assertThat(statusResponseString, statusResponseMap.get("complete"), equalTo(true));
                final int originalWriteIndex = 1;
                if (isOriginalClusterSameMajorVersionAsCurrent() || CLUSTER_TYPE == ClusterType.OLD) {
                    assertThat(
                        statusResponseString,
                        statusResponseMap.get("total_indices_in_data_stream"),
                        equalTo(originalWriteIndex + numRolloversOnOldCluster + explicitRolloverOnNewClusterCount)
                    );
                    // If the original cluster was the same as this one, we don't want any indices reindexed:
                    assertThat(statusResponseString, statusResponseMap.get("total_indices_requiring_upgrade"), equalTo(0));
                    assertThat(statusResponseString, statusResponseMap.get("successes"), equalTo(0));
                } else {
                    // The number of rollovers that will have happened when we call reindex:
                    final int rolloversPerformedByReindex = explicitRolloverOnNewClusterCount == 0 ? 1 : 0;
                    final int expectedTotalIndicesInDataStream = originalWriteIndex + numRolloversOnOldCluster
                        + explicitRolloverOnNewClusterCount + rolloversPerformedByReindex;
                    assertThat(
                        statusResponseString,
                        statusResponseMap.get("total_indices_in_data_stream"),
                        equalTo(expectedTotalIndicesInDataStream)
                    );
                    /*
                     * total_indices_requiring_upgrade is made up of: (the original write index) + numRolloversOnOldCluster. The number of
                     * rollovers on the upgraded cluster is irrelevant since those will not be reindexed.
                     */
                    assertThat(
                        statusResponseString,
                        statusResponseMap.get("total_indices_requiring_upgrade"),
                        equalTo(originalWriteIndex + numRolloversOnOldCluster)
                    );
                    assertThat(statusResponseString, statusResponseMap.get("successes"), equalTo(expectedSuccessesCount));
                    // We expect all the original indices to have been deleted
                    if (expectedErrorCount == 0) {
                        for (String oldIndex : indicesNeedingUpgrade) {
                            assertThat(statusResponseString, indexExists(oldIndex), equalTo(false));
                        }
                    }
                    assertThat(
                        statusResponseString,
                        getDataStreamIndices(dataStreamName).size(),
                        equalTo(expectedTotalIndicesInDataStream)
                    );
                    assertThat(statusResponseString, ((List<Object>) statusResponseMap.get("errors")).size(), equalTo(expectedErrorCount));
                }
            }, 60, TimeUnit.SECONDS);

            // Verify it's possible to reindex again after a successful reindex
            reindexResponse = upgradeUserClient.performRequest(reindexRequest);
            assertOK(reindexResponse);
        }
    }

    private void makeSureNoUpgrade(String dataStreamName) throws Exception {
        String upgradeUser = "upgrade_user";
        String upgradeUserPassword = "x-pack-test-password";
        createRole("upgrade_role", dataStreamName);
        createUser(upgradeUser, upgradeUserPassword, "upgrade_role");
        try (RestClient upgradeUserClient = getClient(upgradeUser, upgradeUserPassword)) {
            assertBusy(() -> {
                try {
                    Request statusRequest = new Request("GET", "_migration/reindex/" + dataStreamName + "/_status");
                    Response statusResponse = upgradeUserClient.performRequest(statusRequest);
                    Map<String, Object> statusResponseMap = XContentHelper.convertToMap(
                        JsonXContent.jsonXContent,
                        statusResponse.getEntity().getContent(),
                        false
                    );
                    String statusResponseString = statusResponseMap.keySet()
                        .stream()
                        .map(key -> key + "=" + statusResponseMap.get(key))
                        .collect(Collectors.joining(", ", "{", "}"));
                    assertOK(statusResponse);
                    assertThat(statusResponseString, statusResponseMap.get("complete"), equalTo(true));
                    assertThat(statusResponseString, statusResponseMap.get("successes"), equalTo(0));
                } catch (Exception e) {
                    fail(e);
                }
            }, 60, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> getDataStreamIndices(String dataStreamName) throws IOException {
        Response response = client().performRequest(new Request("GET", "_data_stream/" + dataStreamName));
        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
        List<Map<String, Object>> dataStreams = (List<Map<String, Object>>) responseMap.get("data_streams");
        Map<String, Object> dataStream = dataStreams.get(0);
        List<Map<String, Object>> indices = (List<Map<String, Object>>) dataStream.get("indices");
        return indices.stream().map(index -> index.get("index_name").toString()).collect(Collectors.toSet());
    }

    /*
     * Similar to isOriginalClusterCurrent, but returns true if the major versions of the clusters are the same. So true
     * for 8.6 and 8.17, but false for 7.17 and 8.18.
     */
    private boolean isOriginalClusterSameMajorVersionAsCurrent() {
        /*
         * Since data stream reindex is specifically about upgrading a data stream from one major version to the next, it's ok to use the
         * deprecated Version.fromString here
         */
        return Version.fromString(UPGRADE_FROM_VERSION).major == Version.fromString(Build.current().version()).major;
    }

    private static void bulkLoadData(String dataStreamName) throws IOException {
        final String bulk = """
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cat", "network": {"tx": 2001818691, "rx": 802133794}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "hamster", "network": {"tx": 2005177954, "rx": 801479970}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cow", "network": {"tx": 2006223737, "rx": 802337279}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "rat", "network": {"tx": 2012916202, "rx": 803685721}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "dog", "network": {"tx": 1434521831, "rx": 530575198}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "tiger", "network": {"tx": 1434577921, "rx": 530600088}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "lion", "network": {"tx": 1434587694, "rx": 530604797}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "elephant", "network": {"tx": 1434595272, "rx": 530605511}}}}
            """;
        var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
        bulkRequest.setJsonEntity(bulk.replace("$now", formatInstant(Instant.now())));
        var response = client().performRequest(bulkRequest);
        assertOK(response);
    }

    /*
     * This bulkloads data, where some documents have no @timestamp field and some do.
     */
    private static void bulkLoadDataMissingTimestamp(String dataStreamName) throws IOException {
        final String bulk = """
            {"create": {}}
            {"metricset": "pod", "k8s": {"pod": {"name": "cat", "network": {"tx": 2001818691, "rx": 802133794}}}}
            {"create": {}}
            {"metricset": "pod", "k8s": {"pod": {"name": "hamster", "network": {"tx": 2005177954, "rx": 801479970}}}}
            {"create": {}}
            {"metricset": "pod", "k8s": {"pod": {"name": "cow", "network": {"tx": 2006223737, "rx": 802337279}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "rat", "network": {"tx": 2012916202, "rx": 803685721}}}}
            """;
        var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
        bulkRequest.setJsonEntity(bulk.replace("$now", formatInstant(Instant.now())));
        var response = client().performRequest(bulkRequest);
        assertOK(response);
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    private static String rollover(String dataStreamName) throws IOException {
        Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
        Response rolloverResponse = client().performRequest(rolloverRequest);
        assertOK(rolloverResponse);
        String oldIndexName = (String) entityAsMap(rolloverResponse).get("old_index");
        return oldIndexName;
    }

    private void createUser(String name, String password, String role) throws IOException {
        Request request = new Request("PUT", "/_security/user/" + name);
        request.setJsonEntity("{ \"password\": \"" + password + "\", \"roles\": [ \"" + role + "\"] }");
        assertOK(adminClient().performRequest(request));
    }

    private void createRole(String name, String dataStream) throws IOException {
        Request request = new Request("PUT", "/_security/role/" + name);
        request.setJsonEntity("{ \"indices\": [ { \"names\" : [ \"" + dataStream + "\"], \"privileges\": [ \"manage\" ] } ] }");
        assertOK(adminClient().performRequest(request));
    }

    private RestClient getClient(String user, String passwd) throws IOException {
        RestClientBuilder builder = RestClient.builder(adminClient().getNodes().toArray(new Node[0]));
        String token = basicAuthHeaderValue(user, new SecureString(passwd.toCharArray()));
        configureClient(builder, Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build());
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }
}
