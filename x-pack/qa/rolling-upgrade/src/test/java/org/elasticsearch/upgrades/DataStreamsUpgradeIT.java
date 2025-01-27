/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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
import java.util.HashSet;
import java.util.List;
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
        String dataStreamName = "reindex_test_data_stream";
        int numRollovers = randomIntBetween(0, 5);
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createAndRolloverDataStream(dataStreamName, numRollovers);
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            upgradeDataStream(dataStreamName, numRollovers);
        }
    }

    private static void createAndRolloverDataStream(String dataStreamName, int numRollovers) throws IOException {
        // We want to create a data stream and roll it over several times so that we have several indices to upgrade
        final String template = """
            {
                "settings":{
                    "index": {
                        "mode": "time_series"
                    }
                },
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
                            "type": "keyword",
                            "time_series_dimension": true
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
        final String indexTemplate = """
            {
                "index_patterns": ["$PATTERN"],
                "template": $TEMPLATE,
                "data_stream": {
                }
            }""";
        var putIndexTemplateRequest = new Request("POST", "/_index_template/reindex_test_data_stream_template");
        putIndexTemplateRequest.setJsonEntity(indexTemplate.replace("$TEMPLATE", template).replace("$PATTERN", dataStreamName));
        assertOK(client().performRequest(putIndexTemplateRequest));
        bulkLoadData(dataStreamName);
        for (int i = 0; i < numRollovers; i++) {
            String oldIndexName = rollover(dataStreamName);
            if (randomBoolean()) {
                closeIndex(oldIndexName);
            }
            bulkLoadData(dataStreamName);
        }
    }

    private void upgradeDataStream(String dataStreamName, int numRolloversOnOldCluster) throws Exception {
        Set<String> indicesNeedingUpgrade = getDataStreamIndices(dataStreamName);
        Set<String> closedOldIndices = getClosedIndices(dataStreamName);
        final int explicitRolloverOnNewClusterCount = randomIntBetween(0, 2);
        for (int i = 0; i < explicitRolloverOnNewClusterCount; i++) {
            String oldIndexName = rollover(dataStreamName);
            if (randomBoolean()) {
                if (i == 0) {
                    // Since this is the first rollover on the new cluster, the old index came from the old cluster
                    closedOldIndices.add(oldIndexName);
                }
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
                assertOK(statusResponse);
                assertThat(statusResponseMap.get("complete"), equalTo(true));
                final int originalWriteIndex = 1;
                if (isOriginalClusterSameMajorVersionAsCurrent()) {
                    assertThat(
                        statusResponseMap.get("total_indices_in_data_stream"),
                        equalTo(originalWriteIndex + numRolloversOnOldCluster + explicitRolloverOnNewClusterCount)
                    );
                    // If the original cluster was the same as this one, we don't want any indices reindexed:
                    assertThat(statusResponseMap.get("total_indices_requiring_upgrade"), equalTo(0));
                    assertThat(statusResponseMap.get("successes"), equalTo(0));
                } else {
                    // The number of rollovers that will have happened when we call reindex:
                    final int rolloversPerformedByReindex = explicitRolloverOnNewClusterCount == 0 ? 1 : 0;
                    final int expectedTotalIndicesInDataStream = originalWriteIndex + numRolloversOnOldCluster
                        + explicitRolloverOnNewClusterCount + rolloversPerformedByReindex;
                    assertThat(statusResponseMap.get("total_indices_in_data_stream"), equalTo(expectedTotalIndicesInDataStream));
                    /*
                     * total_indices_requiring_upgrade is made up of: (the original write index) + numRolloversOnOldCluster. The number of
                     * rollovers on the upgraded cluster is irrelevant since those will not be reindexed.
                     */
                    assertThat(
                        statusResponseMap.get("total_indices_requiring_upgrade"),
                        equalTo(originalWriteIndex + numRolloversOnOldCluster - closedOldIndices.size())
                    );
                    assertThat(statusResponseMap.get("successes"), equalTo(numRolloversOnOldCluster + 1 - closedOldIndices.size()));
                    // We expect all the original indices to have been deleted
                    for (String oldIndex : indicesNeedingUpgrade) {
                        if (closedOldIndices.contains(oldIndex) == false) {
                            assertThat(indexExists(oldIndex), equalTo(false));
                        }
                    }
                    assertThat(getDataStreamIndices(dataStreamName).size(), equalTo(expectedTotalIndicesInDataStream));
                }
            }, 60, TimeUnit.SECONDS);
            Request cancelRequest = new Request("POST", "_migration/reindex/" + dataStreamName + "/_cancel");
            Response cancelResponse = upgradeUserClient.performRequest(cancelRequest);
            assertOK(cancelResponse);
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

    @SuppressWarnings("unchecked")
    private Set<String> getClosedIndices(String dataStreamName) throws IOException {
        Set<String> allIndices = getDataStreamIndices(dataStreamName);
        Set<String> closedIndices = new HashSet<>();
        Response response = client().performRequest(new Request("GET", "_cluster/state/blocks/indices"));
        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
        Map<String, Object> blocks = (Map<String, Object>) responseMap.get("blocks");
        Map<String, Object> indices = (Map<String, Object>) blocks.get("indices");
        for (Map.Entry<String, Object> indexEntry : indices.entrySet()) {
            String indexName = indexEntry.getKey();
            if (allIndices.contains(indexName)) {
                Map<String, Object> blocksForIndex = (Map<String, Object>) indexEntry.getValue();
                for (Map.Entry<String, Object> blockEntry : blocksForIndex.entrySet()) {
                    Map<String, String> block = (Map<String, String>) blockEntry.getValue();
                    if ("index closed".equals(block.get("description"))) {
                        closedIndices.add(indexName);
                    }
                }
            }
        }
        return closedIndices;
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
