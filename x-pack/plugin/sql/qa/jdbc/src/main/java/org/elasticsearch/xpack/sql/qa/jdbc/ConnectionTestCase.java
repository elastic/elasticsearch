/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.Version;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Test the jdbc {@link Connection} implementation.
 */
public abstract class ConnectionTestCase extends JdbcIntegrationTestCase {

    public void testConnectionProperties() throws SQLException {
        try (Connection c = esJdbc()) {
            assertFalse(c.isClosed());
            assertTrue(c.isReadOnly());
            DatabaseMetaData md = c.getMetaData();
            assertEquals(Version.CURRENT.major, md.getDatabaseMajorVersion());
            assertEquals(Version.CURRENT.minor, md.getDatabaseMinorVersion());
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
    public void testTransactionIsolation() throws SQLException {
        try (Connection c = esJdbc()) {
            assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation());
            SQLException e = expectThrows(SQLException.class, () -> c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
            assertEquals("Transactions not supported", e.getMessage());
            assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation());
        }
    }
}
