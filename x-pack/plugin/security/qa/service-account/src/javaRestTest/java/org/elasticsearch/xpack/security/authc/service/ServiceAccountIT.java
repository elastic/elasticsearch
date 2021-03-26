/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class ServiceAccountIT extends ESRestTestCase {

    private static final String VALID_SERVICE_TOKEN = "AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOnI1d2RiZGJvUVNlOXZHT0t3YUpHQXc";
    private static final String INVALID_SERVICE_TOKEN = "AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOjNhSkNMYVFXUk4yc1hsT2R0eEEwU1E";
    private static Path caPath;

    private static final String AUTHENTICATE_RESPONSE = ""
        + "{\n"
        + "  \"username\": \"elastic/fleet\",\n"
        + "  \"roles\": [],\n"
        + "  \"full_name\": \"Service account - elastic/fleet\",\n"
        + "  \"email\": null,\n"
        + "  \"metadata\": {\n"
        + "    \"_elastic_service_account\": true\n"
        + "  },\n" + "  \"enabled\": true,\n"
        + "  \"authentication_realm\": {\n"
        + "    \"name\": \"service_account\",\n"
        + "    \"type\": \"service_account\"\n"
        + "  },\n"
        + "  \"lookup_realm\": {\n"
        + "    \"name\": \"service_account\",\n"
        + "    \"type\": \"service_account\"\n"
        + "  },\n"
        + "  \"authentication_type\": \"token\"\n"
        + "}\n";

    @BeforeClass
    public static void init() throws URISyntaxException, FileNotFoundException {
        URL resource = ServiceAccountIT.class.getResource("/ssl/ca.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/ca.crt");
        }
        caPath = PathUtils.get(resource.toURI());
    }

    @Override
    protected String getProtocol() {
        // Because http.ssl.enabled = true
        return "https";
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CERTIFICATE_AUTHORITIES, caPath)
            .build();
    }

    public void testAuthenticate() throws IOException {
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + VALID_SERVICE_TOKEN));
        final Response response = client().performRequest(request);
        assertOK(response);
        assertThat(responseAsMap(response),
            equalTo(XContentHelper.convertToMap(new BytesArray(AUTHENTICATE_RESPONSE), false, XContentType.JSON).v2()));
    }

    public void testAuthenticateShouldNotFallThroughInCaseOfFailure() throws IOException {
        final boolean securityIndexExists = randomBoolean();
        if (securityIndexExists) {
            final Request createRoleRequest = new Request("POST", "_security/role/dummy_role");
            createRoleRequest.setJsonEntity("{\"cluster\":[]}");
            assertOK(client().performRequest(createRoleRequest));
        }
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + INVALID_SERVICE_TOKEN));
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(401));
        if (securityIndexExists) {
            assertThat(e.getMessage(), containsString("failed to authenticate service account [elastic/fleet] with token name [token1]"));
        } else {
            assertThat(e.getMessage(), containsString("no such index [.security]"));
        }
    }

    public void testAuthenticateShouldWorkWithOAuthBearerToken() throws IOException {
        final Request oauthTokenRequest = new Request("POST", "_security/oauth2/token");
        oauthTokenRequest.setJsonEntity("{\"grant_type\":\"password\",\"username\":\"test_admin\",\"password\":\"x-pack-test-password\"}");
        final Response oauthTokenResponse = client().performRequest(oauthTokenRequest);
        assertOK(oauthTokenResponse);
        final Map<String, Object> oauthTokenResponseMap = responseAsMap(oauthTokenResponse);
        final String accessToken = (String) oauthTokenResponseMap.get("access_token");

        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + accessToken));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.get("username"), equalTo("test_admin"));
        assertThat(responseMap.get("authentication_type"), equalTo("token"));

        final String refreshToken = (String) oauthTokenResponseMap.get("refresh_token");
        final Request refreshTokenRequest = new Request("POST", "_security/oauth2/token");
        refreshTokenRequest.setJsonEntity("{\"grant_type\":\"refresh_token\",\"refresh_token\":\"" + refreshToken + "\"}");
        final Response refreshTokenResponse = client().performRequest(refreshTokenRequest);
        assertOK(refreshTokenResponse);
    }

    public void testAuthenticateShouldDifferentiateBetweenNormalUserAndServiceAccount() throws IOException {
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(
            "Authorization", basicAuthHeaderValue("elastic/fleet", new SecureString("x-pack-test-password".toCharArray()))
        ));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);

        assertThat(responseMap.get("username"), equalTo("elastic/fleet"));
        assertThat(responseMap.get("authentication_type"), equalTo("realm"));
        assertThat(responseMap.get("roles"), equalTo(List.of("superuser")));
        Map<?, ?> authRealm = (Map<?, ?>) responseMap.get("authentication_realm");
        assertThat(authRealm, hasEntry("type", "file"));
    }

    public void testCreateApiServiceAccountTokenAndAuthenticateWithIt() throws IOException {
        final Request createTokenRequest = new Request("POST", "_security/service/elastic/fleet/credential/token/api-token-1");
        final Response createTokenResponse = client().performRequest(createTokenRequest);
        assertOK(createTokenResponse);
        final Map<String, Object> createTokenResponseMap = responseAsMap(createTokenResponse);
        assertThat(createTokenResponseMap.get("created"), is(true));
        @SuppressWarnings("unchecked")
        final Map<String, String> tokenMap = (Map<String, String>) createTokenResponseMap.get("token");
        assertThat(tokenMap.get("name"), equalTo("api-token-1"));

        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + tokenMap.get("value")));
        final Response response = client().performRequest(request);
        assertOK(response);
        assertThat(responseAsMap(response),
            equalTo(XContentHelper.convertToMap(new BytesArray(AUTHENTICATE_RESPONSE), false, XContentType.JSON).v2()));
    }

    public void testFileTokenAndApiTokenCanShareTheSameNameAndBothWorks() throws IOException {
        final Request createTokenRequest = new Request("POST", "_security/service/elastic/fleet/credential/token/token1");
        final Response createTokenResponse = client().performRequest(createTokenRequest);
        assertOK(createTokenResponse);
        final Map<String, Object> createTokenResponseMap = responseAsMap(createTokenResponse);
        assertThat(createTokenResponseMap.get("created"), is(true));
        @SuppressWarnings("unchecked")
        final Map<String, String> tokenMap = (Map<String, String>) createTokenResponseMap.get("token");
        assertThat(tokenMap.get("name"), equalTo("token1"));

        // The API token works
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + tokenMap.get("value")));
        assertOK(client().performRequest(request));

        // And the file token also works
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + VALID_SERVICE_TOKEN));
        assertOK(client().performRequest(request));
    }

    public void testNoDuplicateApiServiceAccountToken() throws IOException {
        final String tokeName = randomAlphaOfLengthBetween(3, 8);
        final Request createTokenRequest = new Request("POST", "_security/service/elastic/fleet/credential/token/" + tokeName);
        final Response createTokenResponse = client().performRequest(createTokenRequest);
        assertOK(createTokenResponse);

        final ResponseException e =
            expectThrows(ResponseException.class, () -> client().performRequest(createTokenRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(409));
        assertThat(e.getMessage(), containsString("document already exists"));
    }

    public void testGetServiceAccountTokens() throws IOException {
        final Request getTokensRequest = new Request("GET", "_security/service/elastic/fleet/credential");
        final Response getTokensResponse1 = client().performRequest(getTokensRequest);
        assertOK(getTokensResponse1);
        final Map<String, Object> getTokensResponseMap1 = responseAsMap(getTokensResponse1);
        assertThat(getTokensResponseMap1.get("service_account"), equalTo("elastic/fleet"));
        assertThat(getTokensResponseMap1.get("count"), equalTo(1));
        assertThat(getTokensResponseMap1.get("tokens"), equalTo(Map.of()));
        assertThat(getTokensResponseMap1.get("file_tokens"), equalTo(Map.of("token1", Map.of())));

        final Request createTokenRequest1 = new Request("POST", "_security/service/elastic/fleet/credential/token/api-token-1");
        final Response createTokenResponse1 = client().performRequest(createTokenRequest1);
        assertOK(createTokenResponse1);

        final Request createTokenRequest2 = new Request("POST", "_security/service/elastic/fleet/credential/token/api-token-2");
        final Response createTokenResponse2 = client().performRequest(createTokenRequest2);
        assertOK(createTokenResponse2);

        final Response getTokensResponse2 = client().performRequest(getTokensRequest);
        assertOK(getTokensResponse2);
        final Map<String, Object> getTokensResponseMap2 = responseAsMap(getTokensResponse2);
        assertThat(getTokensResponseMap2.get("service_account"), equalTo("elastic/fleet"));
        assertThat(getTokensResponseMap2.get("count"), equalTo(3));
        assertThat(getTokensResponseMap2.get("file_tokens"), equalTo(Map.of("token1", Map.of())));
        assertThat(getTokensResponseMap2.get("tokens"), equalTo(Map.of(
            "api-token-1", Map.of(),
            "api-token-2", Map.of()
        )));
    }

    public void testManageOwnApiKey() throws IOException {
        final String token;
        if (randomBoolean()) {
            token = VALID_SERVICE_TOKEN;
        } else {
            final Request createTokenRequest = new Request("POST", "_security/service/elastic/fleet/credential/token/api-token-42");
            final Response createTokenResponse = client().performRequest(createTokenRequest);
            assertOK(createTokenResponse);
            final Map<String, Object> createTokenResponseMap = responseAsMap(createTokenResponse);
            assertThat(createTokenResponseMap.get("created"), is(true));
            @SuppressWarnings("unchecked")
            final Map<String, String> tokenMap = (Map<String, String>) createTokenResponseMap.get("token");
            assertThat(tokenMap.get("name"), equalTo("api-token-42"));
            token = tokenMap.get("value");
        }
        final RequestOptions.Builder requestOptions = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + token);

        final Request createApiKeyRequest1 = new Request("PUT", "_security/api_key");
        if (randomBoolean()) {
            createApiKeyRequest1.setJsonEntity("{\"name\":\"key-1\"}");
        } else {
            createApiKeyRequest1.setJsonEntity("{\"name\":\"key-1\",\"role_descriptors\":{\"a\":{\"cluster\":[\"all\"]}}}");
        }
        createApiKeyRequest1.setOptions(requestOptions);
        final Response createApiKeyResponse1 = client().performRequest(createApiKeyRequest1);
        assertOK(createApiKeyResponse1);
        final String apiKeyId1 = (String) responseAsMap(createApiKeyResponse1).get("id");

        assertApiKeys(apiKeyId1, "key-1", false, requestOptions);

        final Request invalidateApiKeysRequest = new Request("DELETE", "_security/api_key");
        invalidateApiKeysRequest.setJsonEntity("{\"ids\":[\"" + apiKeyId1 + "\"],\"owner\":true}");
        invalidateApiKeysRequest.setOptions(requestOptions);
        final Response invalidateApiKeysResponse = client().performRequest(invalidateApiKeysRequest);
        assertOK(invalidateApiKeysResponse);
        final Map<String, Object> invalidateApiKeysResponseMap = responseAsMap(invalidateApiKeysResponse);
        assertThat(invalidateApiKeysResponseMap.get("invalidated_api_keys"), equalTo(List.of(apiKeyId1)));

        assertApiKeys(apiKeyId1, "key-1", true, requestOptions);
    }

    private void assertApiKeys(String apiKeyId, String name, boolean invalidated,
                               RequestOptions.Builder requestOptions) throws IOException {
        final Request getApiKeysRequest = new Request("GET", "_security/api_key?owner=true");
        getApiKeysRequest.setOptions(requestOptions);
        final Response getApiKeysResponse = client().performRequest(getApiKeysRequest);
        assertOK(getApiKeysResponse);
        final Map<String, Object> getApiKeysResponseMap = responseAsMap(getApiKeysResponse);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> apiKeys = (List<Map<String, Object>>) getApiKeysResponseMap.get("api_keys");
        assertThat(apiKeys.size(), equalTo(1));

        final Map<String, Object> apiKey = apiKeys.get(0);
        assertThat(apiKey.get("id"), equalTo(apiKeyId));
        assertThat(apiKey.get("name"), equalTo(name));
        assertThat(apiKey.get("username"), equalTo("elastic/fleet"));
        assertThat(apiKey.get("realm"), equalTo("service_account"));
        assertThat(apiKey.get("invalidated"), is(invalidated));
    }
}
