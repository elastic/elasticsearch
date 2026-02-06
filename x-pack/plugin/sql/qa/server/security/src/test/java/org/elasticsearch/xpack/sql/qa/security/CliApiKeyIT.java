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
import org.elasticsearch.xpack.sql.qa.cli.EmbeddedCli;
import org.elasticsearch.xpack.sql.qa.cli.EmbeddedCli.ApiKeySecurityConfig;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.elasticsearch.xpack.sql.qa.security.RestSqlIT.SSL_ENABLED;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for CLI connections using API key authentication.
 */
public class CliApiKeyIT extends ESRestTestCase {

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
     * Test that a CLI connection can be established using an API key.
     */
    public void testCliConnectionWithApiKey() throws Exception {
        // Create an API key with full access
        String encodedApiKey = createApiKey("cli_test_key", """
            {
                "name": "cli_test_key",
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
        Request createIndex = new Request("PUT", "/test_cli_api_key");
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
        Request indexDoc = new Request("PUT", "/test_cli_api_key/_doc/1");
        indexDoc.addParameter("refresh", "true");
        indexDoc.setJsonEntity("""
            {
                "value": 123
            }
            """);
        client().performRequest(indexDoc);

        // Connect via CLI using the API key
        ApiKeySecurityConfig apiKeyConfig = createApiKeySecurityConfig(encodedApiKey);

        try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, apiKeyConfig)) {
            // Execute a simple query
            String result = cli.command("SELECT value FROM test_cli_api_key");
            assertThat(result, containsString("value"));
            String dataLine = cli.readLine();
            // Skip separator line
            String valueLine = cli.readLine();
            assertThat(valueLine, containsString("123"));
        }
    }

    /**
     * Test that an invalid API key results in an authentication error.
     */
    public void testCliConnectionWithInvalidApiKey() throws Exception {
        ApiKeySecurityConfig apiKeyConfig = new ApiKeySecurityConfig(
            SSL_ENABLED,
            "invalid_api_key_value",
            SSL_ENABLED ? SqlSecurityTestCluster.getKeystorePath() : null,
            SSL_ENABLED ? SqlSecurityTestCluster.KEYSTORE_PASSWORD : null
        );

        try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), false, apiKeyConfig)) {
            String result = cli.command("SELECT 1");
            // The CLI shows a communication error when authentication fails
            // Read subsequent lines to get the full error message
            StringBuilder fullError = new StringBuilder(result);
            String line;
            while ((line = cli.readLine()) != null && !line.isEmpty()) {
                fullError.append(line);
            }
            String errorMessage = fullError.toString();
            // Invalid API key causes authentication failure which manifests as communication error
            assertTrue(
                "Expected authentication error but got: " + errorMessage,
                errorMessage.contains("security_exception") || errorMessage.contains("Communication error")
            );
        }
    }

    /**
     * Test that API key authentication respects role restrictions.
     */
    public void testCliConnectionWithLimitedApiKey() throws Exception {
        // Create a restricted index that the API key cannot access
        Request createRestrictedIndex = new Request("PUT", "/cli_restricted_index");
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

        Request indexRestrictedDoc = new Request("PUT", "/cli_restricted_index/_doc/1");
        indexRestrictedDoc.addParameter("refresh", "true");
        indexRestrictedDoc.setJsonEntity("""
            {
                "secret": "confidential"
            }
            """);
        client().performRequest(indexRestrictedDoc);

        // Create an API key that only has access to a specific index pattern
        String encodedApiKey = createApiKey("cli_limited_key", """
            {
                "name": "cli_limited_key",
                "role_descriptors": {
                    "role": {
                        "cluster": ["monitor"],
                        "indices": [
                            {
                                "names": ["cli_allowed_*"],
                                "privileges": ["read"]
                            }
                        ]
                    }
                }
            }
            """);

        ApiKeySecurityConfig apiKeyConfig = createApiKeySecurityConfig(encodedApiKey);

        try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, apiKeyConfig)) {
            // Query to restricted index should fail - the index appears as "Unknown" because
            // the API key doesn't have permission to see it
            String result = cli.command("SELECT * FROM cli_restricted_index");
            // The first line contains "Found 1 problem", the actual error is on the next line
            String errorLine = cli.readLine();
            assertThat(errorLine, containsString("Unknown index [cli_restricted_index]"));
        }
    }

    private String elasticsearchAddress() {
        String cluster = getTestRestCluster();
        return cluster.split(",")[0];
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

    private ApiKeySecurityConfig createApiKeySecurityConfig(String apiKey) {
        return new ApiKeySecurityConfig(
            SSL_ENABLED,
            apiKey,
            SSL_ENABLED ? SqlSecurityTestCluster.getKeystorePath() : null,
            SSL_ENABLED ? SqlSecurityTestCluster.KEYSTORE_PASSWORD : null
        );
    }
}
