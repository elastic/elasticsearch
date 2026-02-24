/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.client.Request;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.elasticsearch.xpack.sql.qa.security.RestSqlIT.SSL_ENABLED;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for JDBC connections using API key authentication.
 */
public class JdbcApiKeyIT extends SqlApiKeyTestCase {

    public void testJdbcConnectionWithApiKey() throws Exception {
        String encodedApiKey = createApiKey("""
            {
                "name": "jdbc_test_key",
                "role_descriptors": {
                    "role": {
                        "cluster": ["monitor"],
                        "indices": [
                            {
                                "names": ["*"],
                                "privileges": ["all"]
                            }
                        ]
                    }
                }
            }
            """);

        Request createIndex = new Request("PUT", "/test_api_key");
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                        "value": { "type": "integer" }
                    }
                }
            }
            """);
        client().performRequest(createIndex);

        Request indexDoc = new Request("PUT", "/test_api_key/_doc/1");
        indexDoc.addParameter("refresh", "true");
        indexDoc.setJsonEntity("""
            {
                "value": 42
            }
            """);
        client().performRequest(indexDoc);

        Properties props = createJdbcPropertiesWithApiKey(encodedApiKey);
        String jdbcUrl = jdbcUrl();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT value FROM test_api_key")) {
                    assertTrue("Expected at least one result", rs.next());
                    assertEquals(42, rs.getInt("value"));
                    assertFalse("Expected only one result", rs.next());
                }
            }
        }
    }

    public void testJdbcConnectionWithInvalidApiKey() throws Exception {
        Properties props = createJdbcPropertiesWithApiKey("invalid_api_key_value");
        String jdbcUrl = jdbcUrl();

        SQLException e = expectThrows(SQLException.class, () -> {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
                connection.createStatement().executeQuery("SELECT 1");
            }
        });
        assertThat(e.getMessage(), containsString("security_exception"));
    }

    public void testJdbcConnectionWithLimitedApiKey() throws Exception {
        Request createRestrictedIndex = new Request("PUT", "/restricted_index");
        createRestrictedIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                        "secret": { "type": "keyword" }
                    }
                }
            }
            """);
        client().performRequest(createRestrictedIndex);

        Request indexRestrictedDoc = new Request("PUT", "/restricted_index/_doc/1");
        indexRestrictedDoc.addParameter("refresh", "true");
        indexRestrictedDoc.setJsonEntity("""
            {
                "secret": "confidential"
            }
            """);
        client().performRequest(indexRestrictedDoc);

        String encodedApiKey = createApiKey("""
            {
                "name": "limited_key",
                "role_descriptors": {
                    "role": {
                        "cluster": ["monitor"],
                        "indices": [
                            {
                                "names": ["allowed_*"],
                                "privileges": ["read"]
                            }
                        ]
                    }
                }
            }
            """);

        Properties props = createJdbcPropertiesWithApiKey(encodedApiKey);
        String jdbcUrl = jdbcUrl();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
            try (Statement statement = connection.createStatement()) {
                SQLException e = expectThrows(SQLException.class, () -> statement.executeQuery("SELECT * FROM restricted_index"));
                assertThat(e.getMessage(), containsString("Unknown index [restricted_index]"));
            }
        }
    }

    private String jdbcUrl() {
        return "jdbc:es://" + getProtocol() + "://" + elasticsearchAddress();
    }

    private Properties createJdbcPropertiesWithApiKey(String apiKey) {
        Properties props = new Properties();
        props.setProperty("apiKey", apiKey);
        props.setProperty("timezone", "UTC");
        addSslPropertiesIfNeeded(props);
        return props;
    }

    private void addSslPropertiesIfNeeded(Properties properties) {
        if (SSL_ENABLED == false) {
            return;
        }
        String keystorePath = SqlSecurityTestCluster.getKeystorePath();
        String keystorePass = SqlSecurityTestCluster.KEYSTORE_PASSWORD;

        properties.put("ssl", "true");
        properties.put("ssl.keystore.location", keystorePath);
        properties.put("ssl.keystore.pass", keystorePass);
        properties.put("ssl.truststore.location", keystorePath);
        properties.put("ssl.truststore.pass", keystorePass);
    }
}
