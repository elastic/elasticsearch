/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;
import org.junit.ClassRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.LOCAL_CLUSTER_NAME;
import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.PASSWORD;
import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.REMOTE_CLUSTER_ALIAS;
import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.USER_NAME;

public class JdbcMetadataIT extends JdbcIntegrationTestCase {
    @ClassRule
    public static SqlTestClusterWithRemote clusterAndRemote = new SqlTestClusterWithRemote();

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

    public void testJdbcGetClusters() throws SQLException {
        try (Connection es = esJdbc()) {
            ResultSet rs = es.getMetaData().getCatalogs();
            assertEquals(1, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals(LOCAL_CLUSTER_NAME, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(REMOTE_CLUSTER_ALIAS, rs.getString(1));
            assertFalse(rs.next());
        }
    }
}
