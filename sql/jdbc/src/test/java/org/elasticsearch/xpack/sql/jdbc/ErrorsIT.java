/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.jdbc.framework.JdbcIntegrationTestCase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Tests for error messages.
 */
public class ErrorsIT extends JdbcIntegrationTestCase {

    String message(String index) {
        return String.format(Locale.ROOT, "line 1:15: Cannot resolve index %s (does not exist or has multiple types)", index);
    }
    public void testSelectFromMissingTable() throws Exception {
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * from test").executeQuery());
            assertEquals(message("test"), e.getMessage());
        }
    }

    public void testMultiTypeIndex() throws Exception {

        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * from multi_type").executeQuery());
            assertEquals(message("multi_type"), e.getMessage());
        }
    }
}
