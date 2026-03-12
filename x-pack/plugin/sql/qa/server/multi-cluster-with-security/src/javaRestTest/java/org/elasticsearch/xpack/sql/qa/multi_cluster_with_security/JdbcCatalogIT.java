/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;
import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.LOCAL_CLUSTER_NAME;
import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.PASSWORD;
import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.REMOTE_CLUSTER_ALIAS;
import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.USER_NAME;

public class JdbcCatalogIT extends JdbcIntegrationTestCase {
    public static SqlTestClusterWithRemote clusterAndRemote = new SqlTestClusterWithRemote();
    public static TestRule setupIndex = new TestRule() {
        @Override
        public Statement apply(Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    try {
                        index(INDEX_NAME, body -> body.field("zero", 0), clusterAndRemote.getRemoteClient());
                        base.evaluate();
                    } finally {
                        clusterAndRemote.getRemoteClient().performRequest(new Request("DELETE", "/" + INDEX_NAME));
                    }
                }
            };
        }
    };

    @ClassRule
    public static RuleChain testSetup = RuleChain.outerRule(clusterAndRemote).around(setupIndex);

    @Override
    protected String getTestRestCluster() {
        return clusterAndRemote.getCluster().getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return clusterAndRemote.clusterAuthSettings();
    }

    @Override
    protected RestClient provisioningClient() {
        return clusterAndRemote.getRemoteClient();
    }

    @Override
    protected Properties connectionProperties() {
        Properties connectionProperties = super.connectionProperties();
        connectionProperties.put("user", USER_NAME);
        connectionProperties.put("password", PASSWORD);
        return connectionProperties;
    }

    private static final String INDEX_NAME = "test";

    public void testJdbcSetCatalog() throws Exception {
        try (Connection es = esJdbc()) {
            PreparedStatement ps = es.prepareStatement("SELECT count(*) FROM " + INDEX_NAME);
            SQLException ex = expectThrows(SQLException.class, ps::executeQuery);
            assertTrue(ex.getMessage().contains("Unknown index [" + INDEX_NAME + "]"));

            String catalog = REMOTE_CLUSTER_ALIAS.substring(0, randomIntBetween(0, REMOTE_CLUSTER_ALIAS.length())) + "*";
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
            PreparedStatement ps = es.prepareStatement("SELECT count(*) FROM " + buildRemoteIndexName(REMOTE_CLUSTER_ALIAS, INDEX_NAME));
            es.setCatalog(LOCAL_CLUSTER_NAME);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }

    public void testQueryWithQualifierAndSetCatalog() throws Exception {
        try (Connection es = esJdbc()) {
            PreparedStatement ps = es.prepareStatement("SELECT " + INDEX_NAME + ".zero FROM " + INDEX_NAME);
            es.setCatalog(REMOTE_CLUSTER_ALIAS);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    public void testQueryWithQualifiedFieldAndIndex() throws Exception {
        try (Connection es = esJdbc()) {
            PreparedStatement ps = es.prepareStatement(
                "SELECT " + INDEX_NAME + ".zero FROM " + buildRemoteIndexName(REMOTE_CLUSTER_ALIAS, INDEX_NAME)
            );
            es.setCatalog(LOCAL_CLUSTER_NAME); // set, but should be ignored
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    public void testCatalogDependentCommands() throws Exception {
        for (String query : List.of(
            "SHOW TABLES \"" + INDEX_NAME + "\"",
            "SHOW COLUMNS FROM \"" + INDEX_NAME + "\"",
            "DESCRIBE \"" + INDEX_NAME + "\""
        )) {
            try (Connection es = esJdbc()) {
                PreparedStatement ps = es.prepareStatement(query);
                ResultSet rs = ps.executeQuery();
                assertFalse(rs.next());

                es.setCatalog(REMOTE_CLUSTER_ALIAS);
                rs = ps.executeQuery();
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
        }
    }
}
