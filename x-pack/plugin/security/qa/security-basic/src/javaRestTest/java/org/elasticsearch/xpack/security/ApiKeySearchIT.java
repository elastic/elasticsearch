/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public class ApiKeySearchIT extends SecurityInBasicRestTestCase {

    private static final String API_KEY_ADMIN_AUTH_HEADER = "Basic YXBpX2tleV9hZG1pbjpzZWN1cml0eS10ZXN0LXBhc3N3b3Jk";
    private static final String API_KEY_USER_AUTH_HEADER = "Basic YXBpX2tleV91c2VyOnNlY3VyaXR5LXRlc3QtcGFzc3dvcmQ=";

    public void testSearch() throws IOException {
        createApiKeys();
        createUser("someone");

        // Admin with manage_api_key can search for all keys
        assertSearch(API_KEY_ADMIN_AUTH_HEADER,
            "{ \"query\": { \"wildcard\": {\"name\": \"*alert*\"} } }",
            apiKeys -> {
                assertThat(apiKeys.size(), equalTo(2));
                assertThat(apiKeys.get(0).get("name"), oneOf("my-org/alert-key-1", "my-alert-key-2"));
                assertThat(apiKeys.get(1).get("name"), oneOf("my-org/alert-key-1", "my-alert-key-2"));
            });

        assertSearch(API_KEY_ADMIN_AUTH_HEADER,
            "{\"query\":{\"bool\":{\"must\":[" +
                "{\"prefix\":{\"metadata.application\":\"fleet\"}},{\"term\":{\"metadata.environment.os\":\"Cat\"}}]}}}",
            apiKeys -> {
                assertThat(apiKeys.size(), equalTo(2));
                assertThat(apiKeys.get(0).get("name"), oneOf("my-org/ingest-key-1", "my-org/management-key-1"));
                assertThat(apiKeys.get(1).get("name"), oneOf("my-org/ingest-key-1", "my-org/management-key-1"));
            }
        );

        assertSearch(API_KEY_ADMIN_AUTH_HEADER,
            "{\"query\":{\"terms\":{\"metadata.tags\":[\"prod\",\"east\"]}}}",
            apiKeys -> {
                assertThat(apiKeys.size(), equalTo(5));
            });

        assertSearch(API_KEY_ADMIN_AUTH_HEADER,
            "{\"query\":{\"range\":{\"creation_time\":{\"lt\":\"now\"}}}}",
            apiKeys -> {
                assertThat(apiKeys.size(), equalTo(6));
            });

        // Search for fields outside of the allowlist fails
        assertSearchError(API_KEY_ADMIN_AUTH_HEADER, 400,
            "{ \"query\": { \"prefix\": {\"api_key_hash\": \"{PBKDF2}10000$\"} } }");

        // Search for api keys won't return other entities
        assertSearch(API_KEY_ADMIN_AUTH_HEADER,
            "{ \"query\": { \"term\": {\"name\": \"someone\"} } }",
            apiKeys -> {
                assertThat(apiKeys, empty());
            });

        // User with manage_own_api_key cannot search without specifying either owner or username/realm_name
        assertSearchError(API_KEY_USER_AUTH_HEADER, 403,
            "{ \"query\": { \"wildcard\": {\"name\": \"*alert*\"} } }");

        assertSearch(API_KEY_USER_AUTH_HEADER,
            "{ \"owner\":true, \"query\": { \"wildcard\": {\"name\": \"*alert*\"} } }",
            apiKeys -> {
                assertThat(apiKeys.size(), equalTo(1));
                assertThat(apiKeys.get(0).get("name"), equalTo("my-alert-key-2"));
            });

        assertSearch(API_KEY_USER_AUTH_HEADER,
            "{ \"username\":\"api_key_user\",\"realm_name\":\"default_file\", \"query\": { \"wildcard\": {\"name\": \"*alert*\"} } }",
            apiKeys -> {
                assertThat(apiKeys.size(), equalTo(1));
                assertThat(apiKeys.get(0).get("name"), equalTo("my-alert-key-2"));
            });
    }

    private void assertSearchError(String authHeader, int statusCode, String body) throws IOException {
        final Request request = new Request("GET", "/_security/api_key");
        request.setJsonEntity(body);
        request.setOptions(
            request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
    }

    private void assertSearch(String authHeader, String body,
                        Consumer<List<Map<String, Object>>> apiKeysVerifier) throws IOException {
        final Request request = new Request("GET", "/_security/api_key");
        request.setJsonEntity(body);
        request.setOptions(
            request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> api_keys = (List<Map<String, Object>>) responseMap.get("api_keys");
        apiKeysVerifier.accept(api_keys);
    }

    private void createApiKeys() throws IOException {
        createApiKey(
            "my-org/ingest-key-1",
            Map.of(
                "application", "fleet-agent",
                "tags", List.of("prod", "east"),
                "environment", Map.of(
                    "os", "Cat", "level", 42, "system", false, "hostname", "my-org-host-1")
            ),
            API_KEY_ADMIN_AUTH_HEADER);

        createApiKey(
            "my-org/ingest-key-2",
            Map.of(
                "application", "fleet-server",
                "tags", List.of("staging", "east"),
                "environment", Map.of(
                    "os", "Dog", "level", 11, "system", true, "hostname", "my-org-host-2")
            ),
            API_KEY_ADMIN_AUTH_HEADER);

        createApiKey(
            "my-org/management-key-1",
            Map.of(
                "application", "fleet-agent",
                "tags", List.of("prod", "west"),
                "environment", Map.of(
                    "os", "Cat", "level", 11, "system", false, "hostname", "my-org-host-3")
            ),
            API_KEY_ADMIN_AUTH_HEADER);

        createApiKey(
            "my-org/alert-key-1",
            Map.of(
                "application", "siem",
                "tags", List.of("prod", "north", "upper"),
                "environment", Map.of(
                    "os", "Dog", "level", 3, "system", true, "hostname", "my-org-host-4")
            ),
            API_KEY_ADMIN_AUTH_HEADER);

        createApiKey(
            "my-ingest-key-1",
            Map.of(
                "application", "cli",
                "tags", List.of("user", "test"),
                "notes", Map.of(
                    "sun", "hot", "earth", "blue")
            ),
            API_KEY_USER_AUTH_HEADER);

        createApiKey(
            "my-alert-key-2",
            Map.of(
                "application", "web",
                "tags", List.of("app", "prod"),
                "notes", Map.of(
                    "shared", false, "weather", "sunny")
            ),
            API_KEY_USER_AUTH_HEADER);
    }

    private void createApiKey(String name, Map<String, Object> metadata, String authHeader) throws IOException {
        final Request request = new Request("POST", "/_security/api_key");
        final String metadataString = XContentTestUtils.convertToXContent(metadata, XContentType.JSON).utf8ToString();
        request.setJsonEntity("{\"name\":\"" + name + "\", \"metadata\":" + metadataString + "}");
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        assertOK(client().performRequest(request));
    }

    private void createUser(String name) throws IOException {
        final Request request = new Request("POST", "/_security/user/" + name);
        request.setJsonEntity("{\"password\":\"super-strong-password\",\"roles\":[]}");
        assertOK(adminClient().performRequest(request));
    }
}
