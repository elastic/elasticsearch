/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import static org.hamcrest.Matchers.startsWith;

public abstract class JdbcNoSqlTestCase extends JdbcIntegrationTestCase {

    public void testJdbcExceptionMessage() throws SQLException {
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * FROM bla").executeQuery());
            assertThat(e.getMessage(), startsWith("Found 1 problem\nline 1:15: Unknown index [bla]"));
        }
    }
}
