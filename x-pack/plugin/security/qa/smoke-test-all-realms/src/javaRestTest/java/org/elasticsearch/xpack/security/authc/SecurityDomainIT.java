/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.action.ApiKey;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class SecurityDomainIT extends SecurityRealmSmokeTestCase {

    private static final String COMMON_USERNAME = "pki-auth";

    @Before
    public void prepare() throws IOException {
        final Request changePasswordRequest = new Request("PUT", "_security/user/elastic/_password");
        changePasswordRequest.setJsonEntity("{\"password\":\"elastic-user-password\"}");
        assertOK(adminClient().performRequest(changePasswordRequest));

        // Create a native realm user
        createUser("native_user", new SecureString("native-user-password".toCharArray()), List.of("rac_role"));
        // Create a native realm user that has the same username as the one from file realm
        createUser(COMMON_USERNAME, new SecureString("pki-auth-password".toCharArray()), List.of("rac_role"));

        // Role mapping for pki realm
        final Request putRoleMappingRequest = new Request("PUT", "_security/role_mapping/pki4");
        // Use realm domain for role mapping
        putRoleMappingRequest.setJsonEntity("{\"enabled\":true,\"roles\":[\"rac_role\"],\"rules\":{\"field\":{\"realm.domain\":\"rac\"}}}");
        assertOK(adminClient().performRequest(putRoleMappingRequest));

        // ingest pipeline for rac
        final Request putPipelineRequest = new Request("PUT", "_ingest/pipeline/rac-pipeline");
        putPipelineRequest.setJsonEntity(
            "{\"description\":\"rac pipeline\",\"processors\":[{\"set_security_user\":{\"field\":\"user\"}}]}"
        );
        assertOK(adminClient().performRequest(putPipelineRequest));
    }

    @Override
    protected Settings restClientSettings() {
        Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(CLIENT_CERT_PATH, getDataPath("/ssl/pki-auth.crt"))
            .put(CLIENT_KEY_PATH, getDataPath("/ssl/pki-auth.key"))
            .put(CLIENT_KEY_PASSWORD, "http-password");
        builder.remove(ThreadContext.PREFIX + ".Authorization");
        return builder.build();
    }

    public void testDomainNotConfigured() throws IOException {
        // domain not shown if not configured or not configurable
        var responseMap1 = authenticate(optionsBuilderWithAuthorization("elastic", "elastic-user-password"));
        assertRealm(responseMap1, "reserved", "reserved");
        assertUsername(responseMap1, "elastic");

        var responseMap2 = authenticate(optionsBuilderWithAuthorization("admin_user", "admin-password"));
        assertRealm(responseMap2, "file", "file0");
        assertUsername(responseMap2, "admin_user");
    }

    public void testConfiguredDomain() throws IOException {
        // Native realm user
        var responseMap1 = authenticate(optionsBuilderWithAuthorization("native_user", "native-user-password"));
        assertRealm(responseMap1, "native", "native1", "rac");
        assertUsername(responseMap1, "native_user");
        assertRoles(responseMap1, "rac_role");
    }

    public void testAuthenticateDifferentRealmsSameDomain() throws IOException {
        // native realm user
        var responseMap1 = authenticate(optionsBuilderWithAuthorization(COMMON_USERNAME, "pki-auth-password"));
        assertRealm(responseMap1, "native", "native1", "rac");
        assertUsername(responseMap1, "pki-auth");
        assertRoles(responseMap1, "rac_role");

        final Map<String, Object> responseMap2 = authenticate(RequestOptions.DEFAULT.toBuilder());
        assertRealm(responseMap2, "pki", "pki4", "rac");
        assertUsername(responseMap2, "pki-auth");
        assertRoles(responseMap2, "rac_role");
    }

    public void testApiKeyWithNoDomain() throws IOException {
        var keyTuple = createApiKey(optionsBuilderWithAuthorization("admin_user", "admin-password"), "key-with-no-domain");

        var apiKeyAuth = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + keyTuple.v2());
        var authenticateMap = authenticate(apiKeyAuth);
        assertRealm(authenticateMap, "_es_api_key", "_es_api_key");
        assertUsername(authenticateMap, "admin_user");

        // Ingest pipeline should Not record domain since it is not configured
        final String docId = ingest(apiKeyAuth);
        final Map<String, Object> doc = getDocById(docId);
        final Map<String, Object> user = getFieldAsMap(doc, "user");
        assertThat(user, hasEntry("realm", Map.of("name", "file0", "type", "file")));
    }

    public void testApiKeyWithDomain() throws IOException {
        // create a key that does not belong to the rac domain
        createApiKey(optionsBuilderWithAuthorization("admin_user", "admin-password"), "admin-key");

        // create a key with the rac user from the native realm
        var keyTuple = createApiKey(optionsBuilderWithAuthorization(COMMON_USERNAME, "pki-auth-password"), "key-with-domain");
        var apiKeyAuth = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + keyTuple.v2());
        var authenticateMap = authenticate(apiKeyAuth);
        assertRealm(authenticateMap, "_es_api_key", "_es_api_key");
        assertUsername(authenticateMap, COMMON_USERNAME);

        // Ingest pipeline should record domain
        final String docId = ingest(apiKeyAuth);
        final Map<String, Object> doc = getDocById(docId);
        final Map<String, Object> user = getFieldAsMap(doc, "user");
        assertThat(user, hasEntry("realm", Map.of("name", "native1", "type", "native", "domain", "rac")));

        // query with the pki realm user of the same domain
        List<ApiKey> apiKeys = queryApiKey(null);
        assertThat(apiKeys, hasSize(1));
        assertThat(apiKeys.get(0).getId(), equalTo(keyTuple.v1()));
        assertThat(apiKeys.get(0).getDomain(), equalTo("rac"));

        // get with the pki realm user of the same domain
        apiKeys = getApiKey(null);
        assertThat(apiKeys, hasSize(1));
        assertThat(apiKeys.get(0).getId(), equalTo(keyTuple.v1()));
        assertThat(apiKeys.get(0).getDomain(), equalTo("rac"));

        // Invalidate the API Key
        final Request invalidateApiKeyRequest = new Request("DELETE", "_security/api_key");
        invalidateApiKeyRequest.setJsonEntity("{\"username\":\"pki-auth\",\"realm_domain\":\"rac\"}");

        final Map<String, Object> invalidateApiKeyMap = responseAsMap(client().performRequest(invalidateApiKeyRequest));
        assertThat(invalidateApiKeyMap.get("invalidated_api_keys"), equalTo(List.of(keyTuple.v1())));
    }

    // Async search result can be shared between the same users from different realms but the same domain
    public void testAsyncSearchWithDomain() throws IOException {
        ingest(optionsBuilderWithAuthorization("admin_user", "admin-password"));

        // Submit the initial search with the native realm user
        final Request submitAsyncSearchRequest = new Request("POST", "rac_index/_async_search");
        submitAsyncSearchRequest.addParameters(Map.of("size", "0", "wait_for_completion_timeout", "1nanos"));
        submitAsyncSearchRequest.setOptions(optionsBuilderWithAuthorization(COMMON_USERNAME, "pki-auth-password"));
        var searchMap = responseAsMap(adminClient().performRequest(submitAsyncSearchRequest));
        var asyncSearchId = (String) searchMap.get("id");

        // Retrieve the search result with the PKI realm user
        final Request getAsyncSearchRequest = new Request("GET", "_async_search/" + asyncSearchId);
        final Response getAsyncSearchResponse = client().performRequest(getAsyncSearchRequest);
        assertOK(getAsyncSearchResponse);
        final Map<String, Object> getAsyncSearchMap = responseAsMap(getAsyncSearchResponse);
        assertThat(getAsyncSearchMap.get("response"), notNullValue());

        // Even admin cannot access the search result
        final ResponseException e =
            expectThrows(ResponseException.class, () -> adminClient().performRequest(getAsyncSearchRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        // Delete the async search task
        final Request deleteAsyncSearchRequest = new Request("DELETE", "_async_search/" + asyncSearchId);
        assertOK(client().performRequest(deleteAsyncSearchRequest));
    }

    private Tuple<String, String> createApiKey(RequestOptions.Builder builder, String name) throws IOException {
        final Request request = new Request("POST", "_security/api_key");
        request.setJsonEntity("{\"name\":\"" + name + "\"}\n");
        request.setOptions(builder);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        return new Tuple<>((String) responseMap.get("id"), (String) responseMap.get("encoded"));
    }

    private List<ApiKey> queryApiKey(RequestOptions.Builder builder) throws IOException {
        final Request request = new Request("GET", "_security/_query/api_key");
        final Response response;
        if (builder != null) {
            request.setOptions(builder);
            response = adminClient().performRequest(request);
        } else {
            // run request with pki settings
            response = client().performRequest(request);
        }
        assertOK(response);
        var responseMap = responseAsMap(response);
        return convertToApiKeys(responseMap);
    }

    private List<ApiKey> getApiKey(RequestOptions.Builder builder) throws IOException {
        final Request request = new Request("GET", "_security/api_key");
        request.addParameter("owner", "true");
        final Response response;
        if (builder != null) {
            request.setOptions(builder);
            response = adminClient().performRequest(request);
        } else {
            // run request with pki settings
            response = client().performRequest(request);
        }
        assertOK(response);
        var responseMap = responseAsMap(response);
        return convertToApiKeys(responseMap);
    }

    @SuppressWarnings("unchecked")
    private List<ApiKey> convertToApiKeys(Map<String, Object> responseMap) {
        final List<ApiKey> apiKeys = new ArrayList<>();
        for (Map<String, Object> apiKey : (List<Map<String, Object>>) responseMap.get("api_keys")) {
            apiKeys.add(new ApiKey(
                (String) apiKey.get("name"),
                (String) apiKey.get("id"),
                Instant.ofEpochMilli((long) apiKey.get("creation")),
                null,
                (boolean) apiKey.get("invalidated"),
                (String) apiKey.get("username"),
                (String) apiKey.get("realm"),
                null,
                (String) apiKey.get("domain")));
        }
        return List.copyOf(apiKeys);
    }

    private String ingest(RequestOptions.Builder builder) throws IOException {
        final Request request = new Request("POST", "rac_index/_doc");
        request.addParameters(Map.of("pipeline", "rac-pipeline", "refresh", "wait_for"));
        request.setJsonEntity("{\"value\":42}");
        request.setOptions(builder);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        return (String) responseAsMap(response).get("_id");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getDocById(String docId) throws IOException {
        final Request request = new Request("GET", "rac_index/_doc/" + docId);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        return (Map<String, Object>) responseAsMap(response).get("_source");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getFieldAsMap(Map<String, Object> doc, String fieldName) {
        return (Map<String, Object>) doc.get(fieldName);
    }

}
