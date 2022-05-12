/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import org.elasticsearch.client.Request;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

public class JdbcCatalogIT extends JdbcIntegrationTestCase {

    // gradle defines
    public static final String LOCAL_CLUSTER_NAME = "javaRestTest";
    public static final String REMOTE_CLUSTER_NAME = "my_remote_cluster";

    private static final String INDEX_NAME = "test";

    @BeforeClass
    static void setupIndex() throws IOException {
        index(INDEX_NAME, body -> body.field("zero", 0));
    }

    @AfterClass
    static void cleanupIndex() throws IOException {
        provisioningClient().performRequest(new Request("DELETE", "/" + INDEX_NAME));
    }

    public void testJdbcSetCatalog() throws Exception {
        try (Connection es = esJdbc()) {
            PreparedStatement ps = es.prepareStatement("SELECT count(*) FROM " + INDEX_NAME);
            SQLException ex = expectThrows(SQLException.class, ps::executeQuery);
            assertTrue(ex.getMessage().contains("Unknown index [" + INDEX_NAME + "]"));

            String catalog = REMOTE_CLUSTER_NAME.substring(0, randomIntBetween(0, REMOTE_CLUSTER_NAME.length())) + "*";
            es.setCatalog(catalog);
            assertEquals(catalog, es.getCatalog());

            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            es.setCatalog(LOCAL_CLUSTER_NAME);
            ex = expectThrows(SQLException.class, ps::executeQuery);
            assertTrue(ex.getMessage().contains("Unknown index [" + INDEX_NAME + "]"));
        }
    }

    public void testQueryCatalogPrecedence() throws Exception {
        try (Connection es = esJdbc()) {
            PreparedStatement ps = es.prepareStatement("SELECT count(*) FROM " + buildRemoteIndexName(REMOTE_CLUSTER_NAME, INDEX_NAME));
            es.setCatalog(LOCAL_CLUSTER_NAME);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }

    public void testCatalogDependentCommands() throws Exception {
        for (String query : Arrays.asList(
            "SHOW TABLES \"" + INDEX_NAME + "\"",
            "SHOW COLUMNS FROM \"" + INDEX_NAME + "\"",
            "DESCRIBE \"" + INDEX_NAME + "\""
        )) {
            try (Connection es = esJdbc()) {
                PreparedStatement ps = es.prepareStatement(query);
                ResultSet rs = ps.executeQuery();
                assertFalse(rs.next());

                es.setCatalog(REMOTE_CLUSTER_NAME);
                rs = ps.executeQuery();
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
        }
    }
}
