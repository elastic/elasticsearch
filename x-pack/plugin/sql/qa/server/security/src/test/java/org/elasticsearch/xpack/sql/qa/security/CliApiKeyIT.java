/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.xpack.sql.qa.cli.EmbeddedCli;
import org.elasticsearch.xpack.sql.qa.cli.EmbeddedCli.ApiKeySecurityConfig;

import static org.elasticsearch.xpack.sql.qa.security.RestSqlIT.SSL_ENABLED;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for CLI connections using API key authentication.
 */
public class CliApiKeyIT extends SqlApiKeyTestCase {

    public void testCliConnectionWithApiKey() throws Exception {
        String encodedApiKey = createApiKey("""
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

        Request indexDoc = new Request("PUT", "/test_cli_api_key/_doc/1");
        indexDoc.addParameter("refresh", "true");
        indexDoc.setJsonEntity("""
            {
                "value": 123
            }
            """);
        client().performRequest(indexDoc);

        ApiKeySecurityConfig apiKeyConfig = createApiKeySecurityConfig(encodedApiKey);

        try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, apiKeyConfig)) {
            String result = cli.command("SELECT value FROM test_cli_api_key");
            assertThat(result, containsString("value"));
            cli.readLine(); // separator line
            String valueLine = cli.readLine();
            assertThat(valueLine, containsString("123"));
        }
    }

    public void testCliConnectionWithInvalidApiKey() throws Exception {
        ApiKeySecurityConfig apiKeyConfig = new ApiKeySecurityConfig(
            SSL_ENABLED,
            "invalid_api_key_value",
            SSL_ENABLED ? SqlSecurityTestCluster.getKeystorePath() : null,
            SSL_ENABLED ? SqlSecurityTestCluster.KEYSTORE_PASSWORD : null
        );

        try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), false, apiKeyConfig)) {
            String result = cli.command("SELECT 1");
            StringBuilder fullError = new StringBuilder(result);
            String line;
            while ((line = cli.readLine()) != null && !line.isEmpty()) {
                fullError.append(line);
            }
            String errorMessage = fullError.toString();
            assertTrue(
                "Expected authentication error but got: " + errorMessage,
                errorMessage.contains("security_exception") || errorMessage.contains("Communication error")
            );
        }
    }

    public void testCliConnectionWithLimitedApiKey() throws Exception {
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

        String encodedApiKey = createApiKey("""
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
            String result = cli.command("SELECT * FROM cli_restricted_index");
            String errorLine = cli.readLine();
            assertThat(errorLine, containsString("Unknown index [cli_restricted_index]"));
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
