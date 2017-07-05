/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.jdbc.framework.JdbcIntegrationTestCase;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Tests for error messages.
 */
public class ErrorsIT extends JdbcIntegrationTestCase {
    public void testSelectFromMissingTable() throws Exception {
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * from test.doc").executeQuery());
            assertEquals("line 1:15: Cannot resolve index test", e.getMessage());
        }
    }

    public void testSelectFromMissingType() throws Exception {
        index("test", builder -> builder.field("name", "bob"));

        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * from test.notdoc").executeQuery());
            assertEquals("line 1:15: Cannot resolve type notdoc in index test", e.getMessage());
        }
    }
}
