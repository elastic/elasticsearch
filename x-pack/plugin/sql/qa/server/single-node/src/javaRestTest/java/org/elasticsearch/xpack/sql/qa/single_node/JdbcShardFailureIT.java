/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.client.Request;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class JdbcShardFailureIT extends JdbcIntegrationTestCase {
    private void createTestIndex() throws IOException {
        client().performRequest(new Request("PUT", "/test1").setJsonEntity("""
            {"aliases":{"test":{}}, "mappings": {"properties": {"test_field":{"type":"integer"}}}}"""));
        client().performRequest(new Request("PUT", "/test2").setJsonEntity("""
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
            }""").addParameter("timeout", "100ms"));

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append(formatted("""
                {"index":{}}
                {"test_field":%s}
                """, i));
        }
        client().performRequest(new Request("PUT", "/test1/_bulk").addParameter("refresh", "true").setJsonEntity(bulk.toString()));
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
            boolean indexWithDocVals = i < okShards;
            assertOK(
                provisioningClient().performRequest(
                    new Request("PUT", indexName).setJsonEntity(
                        String.format(Locale.ROOT, mappingTemplate, indexWithDocVals, indexWithDocVals)
                    )
                )
            );
            assertOK(
                provisioningClient().performRequest(
                    new Request("POST", indexName + "/_doc").addParameter("refresh", "true")
                        .setJsonEntity("{\"bool\": " + (indexWithDocVals || randomBoolean()) + "}")
                )
            );
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
