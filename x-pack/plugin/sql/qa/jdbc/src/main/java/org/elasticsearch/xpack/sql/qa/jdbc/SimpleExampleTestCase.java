/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.Matchers.containsString;

public abstract class SimpleExampleTestCase extends JdbcIntegrationTestCase {

    public void testSimpleExample() throws SQLException, IOException {
        index("library", builder -> {
            builder.field("name", "Don Quixote");
            builder.field("page_count", 1072);
        });
        try (Connection connection = esJdbc()) {
            // tag::simple_example
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery(
                          " SELECT name, page_count"
                        + "    FROM library"
                        + " ORDER BY page_count DESC"
                        + " LIMIT 1")) {
                assertTrue(results.next());
                assertEquals("Don Quixote", results.getString(1));
                assertEquals(1072, results.getInt(2));
                SQLException e = expectThrows(SQLException.class, () ->
                    results.getInt(1));
                assertThat(e.getMessage(), containsString("Unable to convert "
                        + "value [Don Quixote] of type [TEXT] to [Integer]"));
                assertFalse(results.next());
            }
            // end::simple_example
        }
    }
}
