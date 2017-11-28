/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Tests for exceptions and their messages.
 */
public class ErrorsTestCase extends JdbcIntegrationTestCase implements org.elasticsearch.xpack.qa.sql.ErrorsTestCase {
    @Override
    public void testSelectInvalidSql() throws Exception {
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * FRO").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:8: Cannot determine columns for *", e.getMessage());
        }
    }

    @Override
    public void testSelectFromMissingIndex() throws SQLException {
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * FROM test").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:15: Unknown index [test]", e.getMessage());
        }
    }

    @Override
    public void testSelectMissingField() throws Exception {
        index("test", body -> body.field("test", "test"));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT missing FROM test").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:8: Unknown column [missing]", e.getMessage());
        }
    }

    @Override
    public void testSelectMissingFunction() throws Exception {
        index("test", body -> body.field("foo", 1));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT missing(foo) FROM test").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:8: Unknown function [missing]", e.getMessage());
        }
    }
}
