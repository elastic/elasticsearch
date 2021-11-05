/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

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
        Request createTest1 = new Request("PUT", "/test1");
        String body1 = "{\"aliases\":{\"test\":{}}, \"mappings\": {\"properties\": {\"test_field\":{\"type\":\"integer\"}}}}";
        createTest1.setJsonEntity(body1);
        client().performRequest(createTest1);

        Request createTest2 = new Request("PUT", "/test2");
        String body2 = "{\"aliases\":{\"test\":{}}, \"mappings\": {\"properties\": {\"test_field\":{\"type\":\"integer\"}}},"
            + "\"settings\": {\"index.routing.allocation.include.node\": \"nowhere\"}}";
        createTest2.setJsonEntity(body2);
        createTest2.addParameter("timeout", "100ms");
        client().performRequest(createTest2);

        Request request = new Request("PUT", "/test1/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"test_field\":" + i + "}\n");
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
    }

    public void testPartialResponseHandling() throws SQLException {
        try (Connection c = esJdbc(); Statement s = c.createStatement()) {
            SQLException exception = expectThrows(SQLException.class, () -> s.executeQuery("SELECT * FROM test ORDER BY test_field ASC"));
            assertThat(exception.getMessage(), containsString("Search rejected due to missing shards"));
        }
    }
}
