/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.cli;

import org.elasticsearch.client.Request;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public abstract class PartialResultsTestCase extends CliIntegrationTestCase {

    private void createTestIndex(int okShards, int badShards) throws IOException {
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

        for (int i = 0; i < maxWarningHeaders - 1 + okShards + badShards; i++) {
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
    }

    public void testSetAllowPartialResults() throws Exception {
        createTestIndex(1, 1);

        // default value is false
        String result = command("SELECT * FROM test WHERE bool = true");
        assertThat(
            result,
            startsWith(
                "[?1l>[?1000l[?2004l[31;1mServer error [[3;33;22mServer sent bad type [search_phase_execution_exception]. "
                    + "Original type was [Partial shards failure]. [Failed to execute phase [query], Partial shards failure;"
            )
        );
        consumeStackTrace();

        result = command("allow_partial_search_results = false");
        assertEquals("[?1l>[?1000l[?2004lallow_partial_search_results set to [90mfalse[0m", result);
        result = command("SELECT * FROM test WHERE bool = true");
        assertThat(
            result,
            startsWith(
                "[?1l>[?1000l[?2004l[31;1mServer error [[3;33;22mServer sent bad type [search_phase_execution_exception]. "
                    + "Original type was [Partial shards failure]. [Failed to execute phase [query], Partial shards failure;"
            )
        );
        consumeStackTrace();

        result = command("allow_partial_search_results = true");
        assertEquals("[?1l>[?1000l[?2004lallow_partial_search_results set to [90mtrue[0m", result);
        result = command("SELECT * FROM test WHERE bool = true");
        assertEquals("[?1l>[?1000l[?2004l     bool      ", result);
        while (readLine().length() > 0)
            ;

        result = command("allow_partial_search_results = false");
        assertEquals("[?1l>[?1000l[?2004lallow_partial_search_results set to [90mfalse[0m", result);
        result = command("SELECT * FROM test WHERE bool = true");
        assertThat(
            result,
            startsWith(
                "[?1l>[?1000l[?2004l[31;1mServer error [[3;33;22mServer sent bad type [search_phase_execution_exception]. "
                    + "Original type was [Partial shards failure]. [Failed to execute phase [query], Partial shards failure;"
            )
        );
        consumeStackTrace();
    }

    public void testPartialResultHandling() throws Exception {
        final int okShards = randomIntBetween(1, 5);
        final int extraBadShards = randomIntBetween(1, 5);
        createTestIndex(okShards, extraBadShards);

        String query = "SELECT * FROM test WHERE bool=true";
        String result = command("allow_partial_search_results=true");
        assertEquals("[?1l>[?1000l[?2004lallow_partial_search_results set to [90mtrue[0m", result);

        result = command(query);
        assertEquals("[?1l>[?1000l[?2004l     bool      ", result);
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

    private void consumeStackTrace() throws IOException {
        String line;
        do {
            line = readLine();
        } while (line.startsWith("][") == false);
    }
}
