/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.upgrades.IndexingIT.assertCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataStreamsUpgradeIT extends AbstractUpgradeTestCase {

    static final String TEMPLATE = """
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
                                    "uid": {
                                        "type": "keyword",
                                        "time_series_dimension": true
                                    },
                                    "name": {
                                        "type": "keyword"
                                    },
                                    "ip": {
                                        "type": "ip"
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

    private static final String BULK =
        """
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507", "ip": "10.10.55.1", "network": {"tx": 2001818691, "rx": 802133794}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "hamster", "uid":"947e4ced-1786-4e53-9e0c-5c447e959508", "ip": "10.10.55.1", "network": {"tx": 2005177954, "rx": 801479970}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cow", "uid":"947e4ced-1786-4e53-9e0c-5c447e959509", "ip": "10.10.55.1", "network": {"tx": 2006223737, "rx": 802337279}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "rat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959510", "ip": "10.10.55.2", "network": {"tx": 2012916202, "rx": 803685721}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "dog", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9", "ip": "10.10.55.3", "network": {"tx": 1434521831, "rx": 530575198}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "tiger", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea10", "ip": "10.10.55.3", "network": {"tx": 1434577921, "rx": 530600088}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "lion", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876e11", "ip": "10.10.55.3", "network": {"tx": 1434587694, "rx": 530604797}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "elephant", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876eb4", "ip": "10.10.55.3", "network": {"tx": 1434595272, "rx": 530605511}}}}
            """;

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
        String dataStreamName = "k8s";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            final String INDEX_TEMPLATE = """
                {
                    "index_patterns": ["$PATTERN"],
                    "template": $TEMPLATE,
                    "data_stream": {
                    }
                }""";
            // Add composable index template
            String templateName = "1";
            var putIndexTemplateRequest = new Request("POST", "/_index_template/" + templateName);
            putIndexTemplateRequest.setJsonEntity(INDEX_TEMPLATE.replace("$TEMPLATE", TEMPLATE).replace("$PATTERN", dataStreamName));
            assertOK(client().performRequest(putIndexTemplateRequest));

            performOldClustertOperations(templateName, dataStreamName);
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            // nothing
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            performUpgradedClusterOperations(dataStreamName);
        }
    }

    private void performUpgradedClusterOperations(String dataStreamName) throws IOException {
        Request reindexRequest = new Request("POST", "/_reindex_data_stream?source=" + dataStreamName);
        Response reindexResponse = client().performRequest(reindexRequest);
        assertOK(reindexResponse);
        Request statusRequest = new Request("GET", "/_reindex_data_stream_status/reindex-data-stream-" + dataStreamName + "?pretty");
        Response statusResponse = client().performRequest(statusRequest);
        Map<String, Object> statusResponseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            statusResponse.getEntity().getContent(),
            false
        );
        assertOK(statusResponse);
        assertThat(statusResponseMap.get("successes"), equalTo(11));
    }

    private static void performOldClustertOperations(String templateName, String dataStreamName) throws IOException {
        bulkLoadData(dataStreamName);

        var dataStreams = getDataStream(dataStreamName);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo(templateName));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
        String firstBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
        assertThat(firstBackingIndex, backingIndexEqualTo(dataStreamName, 1));
        assertSearch(dataStreamName, 8);

        for (int i = 0; i < 10; i++) {
            rollover(dataStreamName);
            bulkLoadData(dataStreamName);
        }

    }

    private static void bulkLoadData(String dataStreamName) throws IOException {
        var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
        bulkRequest.setJsonEntity(BULK.replace("$now", formatInstant(Instant.now())));
        bulkRequest.addParameter("refresh", "true");
        var response = client().performRequest(bulkRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
    }

    private static void rollover(String dataStreamName) throws IOException {
        Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
        Response rolloverResponse = client().performRequest(rolloverRequest);
        assertOK(rolloverResponse);
    }

    private static Map<String, Object> getDataStream(String dataStreamName) throws IOException {
        var getDataStreamsRequest = new Request("GET", "/_data_stream/" + dataStreamName);
        var response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        return entityAsMap(response);
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    private static void assertSearch(String dataStreamName, int expectedHitCount) throws IOException {
        var searchRequest = new Request("GET", dataStreamName + "/_search");
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat(ObjectPath.evaluate(responseBody, "hits.total.value"), equalTo(expectedHitCount));
    }

}
