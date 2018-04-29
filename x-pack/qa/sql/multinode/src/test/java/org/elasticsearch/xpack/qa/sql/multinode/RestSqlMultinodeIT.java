/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.multinode;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.JDBCType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.qa.sql.rest.RestSqlTestCase.columnInfo;
import static org.elasticsearch.xpack.qa.sql.rest.RestSqlTestCase.randomMode;

/**
 * Tests specific to multiple nodes.
 */
public class RestSqlMultinodeIT extends ESRestTestCase {
    /**
     * Tests count of index run across multiple nodes.
     */
    public void testIndexSpread() throws IOException {
        int documents = between(10, 100);
        createTestData(documents);
        assertCount(client(), documents);
    }

    /**
     * Tests count against index on a node that doesn't have any shards of the index.
     */
    public void testIndexOnWrongNode() throws IOException {
        HttpHost firstHost = getClusterHosts().get(0);
        String firstHostName = null;

        String match = firstHost.getHostName() + ":" + firstHost.getPort();
        Map<String, Object> nodesInfo = responseToMap(client().performRequest("GET", "/_nodes"));
        @SuppressWarnings("unchecked")
        Map<String, Object> nodes = (Map<String, Object>) nodesInfo.get("nodes");
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            String name = node.getKey();
            Map<?, ?> nodeEntries = (Map<?, ?>) node.getValue();
            Map<?, ?> http = (Map<?, ?>) nodeEntries.get("http");
            List<?> boundAddress = (List<?>) http.get("bound_address");
            if (boundAddress.contains(match)) {
                firstHostName = name;
                break;
            }
        }
        assertNotNull("Didn't find first host among published addresses", firstHostName);

        XContentBuilder index = JsonXContent.contentBuilder().prettyPrint().startObject();
        index.startObject("settings"); {
            index.field("routing.allocation.exclude._name", firstHostName);
        }
        index.endObject();
        index.endObject();
        client().performRequest("PUT", "/test", emptyMap(), new StringEntity(Strings.toString(index), ContentType.APPLICATION_JSON));
        int documents = between(10, 100);
        createTestData(documents);

        try (RestClient firstNodeClient = buildClient(restClientSettings(), new HttpHost[] {firstHost})) {
            assertCount(firstNodeClient, documents);
        }
    }

    private void createTestData(int documents) throws UnsupportedCharsetException, IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < documents; i++) {
            int a = 3 * i;
            int b = a + 1;
            int c = b + 1;
            bulk.append("{\"index\":{\"_id\":\"" + i + "\"}\n");
            bulk.append("{\"a\": " + a + ", \"b\": " + b + ", \"c\": " + c + "}\n");
        }
        client().performRequest("PUT", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }

    private Map<String, Object> responseToMap(Response response) throws IOException {
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    private void assertCount(RestClient client, int count) throws IOException {
        Map<String, Object> expected = new HashMap<>();
        String mode = randomMode();
        expected.put("columns", singletonList(columnInfo(mode, "COUNT(1)", "long", JDBCType.BIGINT, 20)));
        expected.put("rows", singletonList(singletonList(count)));

        Map<String, String> params = new TreeMap<>();
        params.put("format", "json");        // JSON is easier to parse then a table
        if (Strings.hasText(mode)) {
            params.put("mode", mode);        // JDBC or PLAIN mode
        }

        Map<String, Object> actual = responseToMap(client.performRequest("POST", "/_xpack/sql", params,
                new StringEntity("{\"query\": \"SELECT COUNT(*) FROM test\"}", ContentType.APPLICATION_JSON)));

        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }
}
