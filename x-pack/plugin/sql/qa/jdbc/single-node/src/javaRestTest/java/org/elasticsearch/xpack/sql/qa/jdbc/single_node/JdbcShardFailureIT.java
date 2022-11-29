/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc.single_node;

import org.elasticsearch.client.Request;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;
import org.junit.Before;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.Matchers.containsString;

public class JdbcShardFailureIT extends JdbcIntegrationTestCase {

    @Before
    public void createTestIndex() throws IOException {
        client().performRequest(new Request("PUT", "/test1").setJsonEntity("""
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
               }
             }"""));

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

    public void testPartialResponseHandling() throws SQLException {
        try (Connection c = esJdbc(); Statement s = c.createStatement()) {
            SQLException exception = expectThrows(SQLException.class, () -> s.executeQuery("SELECT * FROM test ORDER BY test_field ASC"));
            assertThat(exception.getMessage(), containsString("Search rejected due to missing shards"));
        }
    }
}
