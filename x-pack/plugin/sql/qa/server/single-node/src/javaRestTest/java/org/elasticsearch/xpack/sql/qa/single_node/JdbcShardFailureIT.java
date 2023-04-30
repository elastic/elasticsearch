/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.client.Request;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class JdbcShardFailureIT extends JdbcIntegrationTestCase {
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
            bulk.append(Strings.format("""
                {"index":{}}
                {"test_field":%s}
                """, i));
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
    }

    public void testPartialResponseHandling() throws Exception {
        createTestIndex();
        try (Connection c = esJdbc(); Statement s = c.createStatement()) {
            SQLException exception = expectThrows(SQLException.class, () -> s.executeQuery("SELECT * FROM test ORDER BY test_field ASC"));
            assertThat(exception.getMessage(), containsString("Search rejected due to missing shards"));
        }
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

        final String suppressMessage = " remaining shard failure" + (extraBadShards > 1 ? "s" : "") + " suppressed";
        final String reason = "Cannot search on field [bool] since it is not indexed nor has doc values";
        final String warnMessage = "org.elasticsearch.index.query.QueryShardException: failed to create query: " + reason;

        for (int i = 0; i < maxWarningHeaders - 1 + okShards + extraBadShards; i++) {
            String indexName = "/test" + i;
            Request request = new Request("PUT", indexName);
            boolean indexWithDocVals = i < okShards;
            request.setJsonEntity(Strings.format(mappingTemplate, indexWithDocVals, indexWithDocVals));
            assertOK(provisioningClient().performRequest(request));

            request = new Request("POST", indexName + "/_doc");
            request.addParameter("refresh", "true");
            request.setJsonEntity("{\"bool\": " + (indexWithDocVals || randomBoolean()) + "}");
            assertOK(provisioningClient().performRequest(request));
        }

        String query = "SELECT * FROM test WHERE bool=true";
        try (Connection c = esJdbc(); Statement s = c.createStatement()) {
            SQLException exception = expectThrows(SQLException.class, () -> s.executeQuery(query));
            assertThat(exception.getMessage(), containsString(reason));
        }
        Properties properties = connectionProperties();
        properties.setProperty("allow.partial.search.results", "true"); // org.elasticsearch.xpack.sql.client package not available here
        try (Connection c = esJdbc(properties); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(query)) {
            int failedShards = 0;
            boolean hasSupressMessage = false;

            SQLWarning warns = rs.getWarnings();
            do {
                if (warns.getMessage().contains(warnMessage)) {
                    failedShards++;
                } else if (warns.getMessage().contains(suppressMessage)) {
                    hasSupressMessage = true;
                }
            } while ((warns = warns.getNextWarning()) != null);

            assertEquals(maxWarningHeaders - 1, failedShards);
            assertTrue(hasSupressMessage);

            int rows = 0;
            while (rs.next()) {
                rows++;
            }
            assertThat(rows, greaterThanOrEqualTo(okShards));
        }
    }
}
