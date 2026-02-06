/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.xpack.sql.qa.security.RestSqlIT.SSL_ENABLED;

/**
 * Integration tests for JDBC connections using API key authentication.
 */
public class JdbcApiKeyIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = SqlSecurityTestCluster.getCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }

    @Override
    protected String getProtocol() {
        return SSL_ENABLED ? "https" : "http";
    }

    /**
     * Test that a JDBC connection can be established using an API key.
     */
    public void testJdbcConnectionWithApiKey() throws Exception {
        // Create an API key with full access
        String encodedApiKey = createApiKey("jdbc_test_key", """
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

        // Create a test index
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

        // Index a document
        Request indexDoc = new Request("PUT", "/test_api_key/_doc/1");
        indexDoc.addParameter("refresh", "true");
        indexDoc.setJsonEntity("""
            {
                "value": 42
            }
            """);
        client().performRequest(indexDoc);

        // Connect via JDBC using the API key
        Properties props = new Properties();
        props.setProperty("apiKey", encodedApiKey);
        props.setProperty("timezone", "UTC");
        addSslPropertiesIfNeeded(props);

        String jdbcUrl = "jdbc:es://" + getProtocol() + "://" + getTestRestCluster().split(",")[0];

        try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
            // Execute a simple query
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT value FROM test_api_key")) {
                    assertTrue("Expected at least one result", rs.next());
                    assertEquals(42, rs.getInt("value"));
                    assertFalse("Expected only one result", rs.next());
                }
            }
        }
    }

    /**
     * Test that an invalid API key results in an authentication error.
     */
    public void testJdbcConnectionWithInvalidApiKey() throws Exception {
        Properties props = new Properties();
        props.setProperty("apiKey", "invalid_api_key_value");
        props.setProperty("timezone", "UTC");
        addSslPropertiesIfNeeded(props);

        String jdbcUrl = "jdbc:es://" + getProtocol() + "://" + getTestRestCluster().split(",")[0];

        SQLException e = expectThrows(SQLException.class, () -> {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
                connection.createStatement().executeQuery("SELECT 1");
            }
        });
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("security_exception"));
    }

    /**
     * Test that API key authentication respects role restrictions.
     * When an API key doesn't have access to an index, SQL reports it as "Unknown index"
     * because the index is effectively hidden from the API key's view.
     */
    public void testJdbcConnectionWithLimitedApiKey() throws Exception {
        // Create a restricted index that the API key cannot access
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

        // Create an API key that only has access to a specific index pattern
        String encodedApiKey = createApiKey("limited_key", """
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

        Properties props = new Properties();
        props.setProperty("apiKey", encodedApiKey);
        props.setProperty("timezone", "UTC");
        addSslPropertiesIfNeeded(props);

        String jdbcUrl = "jdbc:es://" + getProtocol() + "://" + getTestRestCluster().split(",")[0];

        try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
            // Query to restricted index should fail - the index appears as "Unknown" because
            // the API key doesn't have permission to see it
            try (Statement statement = connection.createStatement()) {
                SQLException e = expectThrows(SQLException.class, () -> statement.executeQuery("SELECT * FROM restricted_index"));
                assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("Unknown index [restricted_index]"));
            }
        }
    }

    private String createApiKey(String name, String body) throws IOException {
        Request createApiKey = new Request("POST", "/_security/api_key");
        createApiKey.setJsonEntity(body);
        Response response = client().performRequest(createApiKey);

        try (InputStream content = response.getEntity().getContent()) {
            Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
            return (String) responseMap.get("encoded");
        }
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
