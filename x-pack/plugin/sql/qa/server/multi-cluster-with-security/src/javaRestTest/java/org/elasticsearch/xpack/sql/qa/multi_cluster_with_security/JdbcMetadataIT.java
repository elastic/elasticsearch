/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcMetadataIT extends JdbcIntegrationTestCase {

    // gradle defines
    public static final String LOCAL_CLUSTER_NAME = "javaRestTest";
    public static final String REMOTE_CLUSTER_NAME = "my_remote_cluster";

    public void testJdbcGetClusters() throws SQLException {
        try (Connection es = esJdbc()) {
            ResultSet rs = es.getMetaData().getCatalogs();
            assertEquals(1, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals(LOCAL_CLUSTER_NAME, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(REMOTE_CLUSTER_NAME, rs.getString(1));
            assertFalse(rs.next());
        }
    }
}
