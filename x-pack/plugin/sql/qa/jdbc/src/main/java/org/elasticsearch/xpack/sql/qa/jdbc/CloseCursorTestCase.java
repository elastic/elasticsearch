/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.core.CheckedConsumer;
import org.junit.Before;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class CloseCursorTestCase extends JdbcIntegrationTestCase {

    @Before
    public void initIndex() throws IOException {
        index("library", "1", builder -> { builder.field("name", "foo"); });
        index("library", "2", builder -> { builder.field("name", "bar"); });
        index("library", "3", builder -> { builder.field("name", "baz"); });
    }

    public void testCloseCursor() throws SQLException {
        doWithQuery("SELECT name FROM library", results -> {
            assertTrue(results.next());
            results.close(); // force sending a cursor close since more pages are available
            assertTrue(results.isClosed());
        });
    }

    public void testCloseConsumedCursor() throws SQLException {
        doWithQuery("SELECT name FROM library", results -> {
            for (int i = 0; i < 3; i++) {
                assertTrue(results.next());
            }
            assertFalse(results.next());
            results.close();
            assertTrue(results.isClosed());
        });
    }

    public void testCloseNoCursor() throws SQLException {
        doWithQuery("SELECT name FROM library WHERE name = 'zzz'", results -> {
            results.close();
            assertTrue(results.isClosed());
        });
    }

    private void doWithQuery(String query, CheckedConsumer<ResultSet, SQLException> consumer) throws SQLException {
        try (Connection connection = createConnection(connectionProperties()); Statement statement = connection.createStatement()) {
            statement.setFetchSize(1);
            ResultSet results = statement.executeQuery(query);
            consumer.accept(results);
        }
    }
}
