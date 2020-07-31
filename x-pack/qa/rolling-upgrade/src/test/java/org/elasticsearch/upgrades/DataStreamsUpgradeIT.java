/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Booleans;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.upgrades.IndexingIT.assertCount;

public class DataStreamsUpgradeIT extends AbstractUpgradeTestCase {

    public void testDataStreams() throws IOException {
        assumeTrue("no data streams in versions before " + Version.V_7_9_0, UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_9_0));
        if (CLUSTER_TYPE == ClusterType.OLD) {
            String requestBody = "{\n" +
                "      \"index_patterns\":[\"logs-*\"],\n" +
                "      \"template\": {\n" +
                "        \"mappings\": {\n" +
                "          \"properties\": {\n" +
                "            \"@timestamp\": {\n" +
                "              \"type\": \"date\"\n" +
                "             }\n" +
                "          }\n" +
                "        }\n" +
                "      },\n" +
                "      \"data_stream\":{\n" +
                "      }\n" +
                "    }";
            Request request = new Request("PUT", "/_index_template/1");
            request.setJsonEntity(requestBody);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(request);
            client().performRequest(request);

            StringBuilder b = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                b.append("{\"create\":{\"_index\":\"").append("logs-foobar").append("\"}}\n");
                b.append("{\"@timestamp\":\"2020-12-12\",\"test\":\"value").append(i).append("\"}\n");
            }
            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.addParameter("filter_path", "errors");
            bulk.setJsonEntity(b.toString());
            Response response = client().performRequest(bulk);
            assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            Request rolloverRequest = new Request("POST", "/logs-foobar/_rollover");
            client().performRequest(rolloverRequest);

            Request index = new Request("POST", "/logs-foobar/_doc");
            index.addParameter("refresh", "true");
            index.addParameter("filter_path", "_index");
            if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                index.setJsonEntity("{\"@timestamp\":\"2020-12-12\",\"test\":\"value1000\"}");
                Response response = client().performRequest(index);
                assertEquals("{\"_index\":\".ds-logs-foobar-000002\"}",
                    EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
            } else {
                index.setJsonEntity("{\"@timestamp\":\"2020-12-12\",\"test\":\"value1001\"}");
                Response response = client().performRequest(index);
                assertEquals("{\"_index\":\".ds-logs-foobar-000003\"}",
                    EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
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

}
