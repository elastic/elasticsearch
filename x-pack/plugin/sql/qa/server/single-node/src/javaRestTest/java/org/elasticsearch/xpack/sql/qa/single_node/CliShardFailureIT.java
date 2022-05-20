/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.client.Request;
import org.elasticsearch.xpack.sql.qa.cli.CliIntegrationTestCase;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CliShardFailureIT extends CliIntegrationTestCase {
    private void createTestIndex() throws IOException {
        Request createTest1 = new Request("PUT", "/test1");
        String body1 = """
            {"aliases":{"test":{}}, "mappings": {"properties": {"test_field":{"type":"integer"}}}}""";
        createTest1.setJsonEntity(body1);
        client().performRequest(createTest1);

        Request createTest2 = new Request("PUT", "/test2");
        String body2 = """
            {
              "aliases": {
                "test": {}
              },
              "mappings": {
                "properties": {
                  "test_field": {
                    "type": "integer"
                  }
                }
              },
              "settings": {
                "index.routing.allocation.include.node": "nowhere"
              }
            }""";
        createTest2.setJsonEntity(body2);
        createTest2.addParameter("timeout", "100ms");
        client().performRequest(createTest2);

        Request request = new Request("PUT", "/test1/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("""
                {"index":{}}
                {"test_field":%s}
                """.formatted(i));
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
    }

    public void testPartialResponseHandling() throws Exception {
        createTestIndex();

        String result = command("SELECT * FROM test ORDER BY test_field ASC");
        assertThat(result, containsString("Server error"));
        consumeStackTrace();

        result = command("allow_partial_search_results = false");
        assertThat(result, containsString("allow_partial_search_results set to [90mfalse"));
        result = command("SELECT * FROM test ORDER BY test_field ASC");
        assertThat(result, containsString("Server error"));
        consumeStackTrace();
    }

    private void consumeStackTrace() throws IOException {
        String line;
        do {
            line = readLine();
        } while (line.startsWith("][") == false);
    }

    public void testAllowPartialSearchResults() throws Exception {
        final String mappingTemplate = """
            {
              "aliases": {
                "test": {}
              },
              "mappings": {
                "properties": {
                  "bool": {
                    "type": "boolean",
                      "index": %s,
                      "doc_values": %s
                  }
                }
              }
            }""";

        // must match org.elasticsearch.xpack.sql.execution.search.Querier.BaseActionListener.MAX_WARNING_HEADERS
        final int maxWarningHeaders = 20;
        final int extraBadShards = randomIntBetween(1, 5);
        final int okShards = randomIntBetween(1, 5);

        for (int i = 0; i < maxWarningHeaders - 1 + okShards + extraBadShards; i++) {
            String indexName = "/test" + i;
            Request request = new Request("PUT", indexName);
            boolean indexWithDocVals = i < okShards;
            request.setJsonEntity(String.format(Locale.ROOT, mappingTemplate, indexWithDocVals, indexWithDocVals));
            assertOK(client().performRequest(request));

            request = new Request("POST", indexName + "/_doc");
            request.addParameter("refresh", "true");
            request.setJsonEntity("{\"bool\": " + (indexWithDocVals || randomBoolean()) + "}");
            assertOK(client().performRequest(request));
        }

        String query = "SELECT * FROM test WHERE bool=true";
        String result;
        result = command(query);
        assertThat(result, containsString("Partial shards failure"));
        consumeStackTrace();

        result = command("allow_partial_search_results=true");
        assertThat(result, containsString("allow_partial_search_results set to [90mtrue"));

        result = command(query);
        assertThat(result, containsString("bool"));
        result = readLine();
        assertEquals("---------------", result);
        int rowCount = 0;
        result = readLine();
        while (result.isBlank() == false) {
            result = readLine();
            rowCount++;
        }
        assertThat(rowCount, greaterThanOrEqualTo(okShards));
    }
}
