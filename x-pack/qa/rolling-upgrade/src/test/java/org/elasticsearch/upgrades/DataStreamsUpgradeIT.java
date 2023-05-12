/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Strings;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.upgrades.IndexingIT.assertCount;

public class DataStreamsUpgradeIT extends AbstractUpgradeTestCase {

    public void testDataStreams() throws IOException {
        assumeTrue("no data streams in versions before " + Version.V_7_9_0, UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_9_0));
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
        assumeTrue("Bug started to occur from version: " + Version.V_7_10_2, UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_10_2));
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

}
