/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.Version;
import org.elasticsearch.xpack.sql.jdbc.framework.JdbcIntegrationTestCase;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConnection;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Test the jdbc {@link Connection} implementation.
 */
public class ConnectionIT extends JdbcIntegrationTestCase {
    public void testConnectionProperties() throws SQLException {
        try (Connection c = esJdbc()) {
            assertFalse(c.isClosed());
            assertTrue(c.isReadOnly());
            assertEquals(Version.CURRENT.major, ((JdbcConnection) c).esInfoMajorVersion());
            assertEquals(Version.CURRENT.minor, ((JdbcConnection) c).esInfoMinorVersion());
        }
    }

    public void testIsValid() throws SQLException {
        try (Connection c = esJdbc()) {
            assertTrue(c.isValid(10));
        }
    }

    /**
     * Tests that we throw report no transaction isolation and throw sensible errors if you ask for any.
     */
    public void testTransactionIsolation() throws Exception {
        try (Connection c = esJdbc()) {
            assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation());
            SQLException e = expectThrows(SQLException.class, () -> c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
            assertEquals("Transactions not supported", e.getMessage());
            assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation());
        }
    }
}