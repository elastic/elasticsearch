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
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.sql.qa.security.RestSqlIT.SSL_ENABLED;

/**
 * Base class for integration tests that use API key authentication.
 */
public abstract class SqlApiKeyTestCase extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = SqlSecurityTestCluster.getCluster();

    private final List<String> createdApiKeyIds = new ArrayList<>();

    @After
    public void cleanupApiKeys() throws IOException {
        for (String apiKeyId : createdApiKeyIds) {
            try {
                Request deleteApiKey = new Request("DELETE", "/_security/api_key");
                deleteApiKey.setJsonEntity("{\"ids\": [\"" + apiKeyId + "\"]}");
                client().performRequest(deleteApiKey);
            } catch (Exception e) {
                logger.warn("Failed to delete API key [{}]: {}", apiKeyId, e.getMessage());
            }
        }
        createdApiKeyIds.clear();
    }

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

    protected String elasticsearchAddress() {
        return getTestRestCluster().split(",")[0];
    }

    protected String createApiKey(String body) throws IOException {
        Request createApiKey = new Request("POST", "/_security/api_key");
        createApiKey.setJsonEntity(body);
        Response response = client().performRequest(createApiKey);

        try (InputStream content = response.getEntity().getContent()) {
            Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
            String apiKeyId = (String) responseMap.get("id");
            createdApiKeyIds.add(apiKeyId);
            return (String) responseMap.get("encoded");
        }
    }
}
