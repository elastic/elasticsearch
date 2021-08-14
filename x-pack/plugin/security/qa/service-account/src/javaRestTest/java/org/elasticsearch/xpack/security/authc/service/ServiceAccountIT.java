/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ServiceAccountIT extends ESRestTestCase {

    private static final String VALID_SERVICE_TOKEN = "AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpyNXdkYmRib1FTZTl2R09Ld2FKR0F3";
    private static final String INVALID_SERVICE_TOKEN = "AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTozYUpDTGFRV1JOMnNYbE9kdHhBMFNR";
    private static Path caPath;

    private static final String AUTHENTICATE_RESPONSE = ""
        + "{\n"
        + "  \"username\": \"elastic/fleet-server\",\n"
        + "  \"roles\": [],\n"
        + "  \"full_name\": \"Service account - elastic/fleet-server\",\n"
        + "  \"email\": null,\n"
        + "  \"token\": {\n"
        + "    \"name\": \"%s\",\n"
        + "    \"type\": \"_service_account_%s\"\n"
        + "  },\n"
        + "  \"metadata\": {\n"
        + "    \"_elastic_service_account\": true\n"
        + "  },\n"
        + "  \"enabled\": true,\n"
        + "  \"authentication_realm\": {\n"
        + "    \"name\": \"_service_account\",\n"
        + "    \"type\": \"_service_account\"\n"
        + "  },\n"
        + "  \"lookup_realm\": {\n"
        + "    \"name\": \"_service_account\",\n"
        + "    \"type\": \"_service_account\"\n"
        + "  },\n"
        + "  \"authentication_type\": \"token\"\n"
        + "}\n";

    private static final String ELASTIC_FLEET_SERVER_ROLE_DESCRIPTOR = ""
        + "{\n"
        + "      \"cluster\": [\n"
        + "        \"monitor\",\n"
        + "        \"manage_own_api_key\"\n"
        + "      ],\n"
        + "      \"indices\": [\n"
        + "        {\n"
        + "          \"names\": [\n"
        + "            \"logs-*\",\n"
        + "            \"metrics-*\",\n"
        + "            \"traces-*\",\n"
        + "            \"synthetics-*\",\n"
        + "            \".logs-endpoint.diagnostic.collection-*\"\n"
        + "          ],\n"
        + "          \"privileges\": [\n"
        + "            \"write\",\n"
        + "            \"create_index\",\n"
        + "            \"auto_configure\"\n"
        + "          ],\n"
        + "          \"allow_restricted_indices\": false\n"
        + "        },\n"
        + "        {\n"
        + "          \"names\": [\n"
        + "            \".fleet-*\"\n"
        + "          ],\n"
        + "          \"privileges\": [\n"
        + "            \"read\",\n"
        + "            \"write\",\n"
        + "            \"monitor\",\n"
        + "            \"create_index\",\n"
        + "            \"auto_configure\"\n"
        + "          ],\n"
        + "          \"allow_restricted_indices\": false\n"
        + "        }\n"
        + "      ],\n"
        + "      \"applications\": [],\n"
        + "      \"run_as\": [],\n"
        + "      \"metadata\": {},\n"
        + "      \"transient_metadata\": {\n"
        + "        \"enabled\": true\n"
        + "      }\n"
        + "    }\n"
        + "  }";

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
    protected Settings restAdminSettings() {
        final String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CERTIFICATE_AUTHORITIES, caPath)
            .build();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("service_account_manager", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CERTIFICATE_AUTHORITIES, caPath)
            .build();
    }

    public void testGetServiceAccount() throws IOException {
        final Request getServiceAccountRequest1 = new Request("GET", "_security/service");
        final Response getServiceAccountResponse1 = client().performRequest(getServiceAccountRequest1);
        assertOK(getServiceAccountResponse1);
        assertServiceAccountRoleDescriptor(getServiceAccountResponse1,
            "elastic/fleet-server", ELASTIC_FLEET_SERVER_ROLE_DESCRIPTOR);

        final Request getServiceAccountRequest2 = new Request("GET", "_security/service/elastic");
        final Response getServiceAccountResponse2 = client().performRequest(getServiceAccountRequest2);
        assertOK(getServiceAccountResponse2);
        assertServiceAccountRoleDescriptor(getServiceAccountResponse2,
            "elastic/fleet-server", ELASTIC_FLEET_SERVER_ROLE_DESCRIPTOR);

        final Request getServiceAccountRequest3 = new Request("GET", "_security/service/elastic/fleet-server");
        final Response getServiceAccountResponse3 = client().performRequest(getServiceAccountRequest3);
        assertOK(getServiceAccountResponse3);
        assertServiceAccountRoleDescriptor(getServiceAccountResponse3,
            "elastic/fleet-server", ELASTIC_FLEET_SERVER_ROLE_DESCRIPTOR);

        final Request getServiceAccountRequestKibana = new Request("GET", "_security/service/elastic/kibana");
        final Response getServiceAccountResponseKibana = client().performRequest(getServiceAccountRequestKibana);
        assertOK(getServiceAccountResponseKibana);
        assertServiceAccountRoleDescriptor(
            getServiceAccountResponseKibana,
            "elastic/kibana",
            Strings.toString(
                ReservedRolesStore.kibanaSystemRoleDescriptor(KibanaSystemUser.ROLE_NAME)
                    .toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
            )
        );

        final String requestPath = "_security/service/" + randomFrom("foo", "elastic/foo", "foo/bar");
        final Request getServiceAccountRequest4 = new Request("GET", requestPath);
        final Response getServiceAccountResponse4 = client().performRequest(getServiceAccountRequest4);
        assertOK(getServiceAccountResponse4);
        assertThat(responseAsMap(getServiceAccountResponse4), anEmptyMap());
    }

    public void testAuthenticate() throws IOException {
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + VALID_SERVICE_TOKEN));
        final Response response = client().performRequest(request);
        assertOK(response);
        assertThat(responseAsMap(response),
            equalTo(XContentHelper.convertToMap(
                new BytesArray(String.format(Locale.ROOT, AUTHENTICATE_RESPONSE, "token1", "file")),
                false, XContentType.JSON).v2()));
    }

    public void testAuthenticateShouldNotFallThroughInCaseOfFailure() throws IOException {
        final boolean securityIndexExists = randomBoolean();
        if (securityIndexExists) {
            final Request createRoleRequest = new Request("POST", "_security/role/dummy_role");
            createRoleRequest.setJsonEntity("{\"cluster\":[]}");
            assertOK(adminClient().performRequest(createRoleRequest));
        }
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + INVALID_SERVICE_TOKEN));
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(401));
        if (securityIndexExists) {
            assertThat(e.getMessage(), containsString(
                "failed to authenticate service account [elastic/fleet-server] with token name [token1]"));
        } else {
            assertThat(e.getMessage(), containsString("no such index [.security]"));
        }
    }

    public void testAuthenticateShouldWorkWithOAuthBearerToken() throws IOException {
        final Request oauthTokenRequest = new Request("POST", "_security/oauth2/token");
        oauthTokenRequest.setJsonEntity("{\"grant_type\":\"password\",\"username\":\"test_admin\",\"password\":\"x-pack-test-password\"}");
        final Response oauthTokenResponse = adminClient().performRequest(oauthTokenRequest);
        assertOK(oauthTokenResponse);
        final Map<String, Object> oauthTokenResponseMap = responseAsMap(oauthTokenResponse);
        final String accessToken = (String) oauthTokenResponseMap.get("access_token");

        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + accessToken));
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.get("username"), equalTo("test_admin"));
        assertThat(responseMap.get("authentication_type"), equalTo("token"));

        final String refreshToken = (String) oauthTokenResponseMap.get("refresh_token");
        final Request refreshTokenRequest = new Request("POST", "_security/oauth2/token");
        refreshTokenRequest.setJsonEntity("{\"grant_type\":\"refresh_token\",\"refresh_token\":\"" + refreshToken + "\"}");
        final Response refreshTokenResponse = adminClient().performRequest(refreshTokenRequest);
        assertOK(refreshTokenResponse);
    }

    public void testAuthenticateShouldDifferentiateBetweenNormalUserAndServiceAccount() throws IOException {
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(
            "Authorization", basicAuthHeaderValue("elastic/fleet-server", new SecureString("x-pack-test-password".toCharArray()))
        ));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);

        assertThat(responseMap.get("username"), equalTo("elastic/fleet-server"));
        assertThat(responseMap.get("authentication_type"), equalTo("realm"));
        assertThat(responseMap.get("roles"), equalTo(org.elasticsearch.core.List.of("superuser")));
        Map<?, ?> authRealm = (Map<?, ?>) responseMap.get("authentication_realm");
        assertThat(authRealm, hasEntry("type", "file"));
    }

    public void testCreateApiServiceAccountTokenAndAuthenticateWithIt() throws IOException {
        final Request createTokenRequest = new Request("POST", "_security/service/elastic/fleet-server/credential/token/api-token-1");
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
            equalTo(XContentHelper.convertToMap(
                new BytesArray(String.format(Locale.ROOT, AUTHENTICATE_RESPONSE, "api-token-1", "index")),
                false, XContentType.JSON).v2()));
    }

    public void testFileTokenAndApiTokenCanShareTheSameNameAndBothWorks() throws IOException {
        final Request createTokenRequest = new Request("POST", "_security/service/elastic/fleet-server/credential/token/token1");
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
        final Request createTokenRequest = new Request("POST", "_security/service/elastic/fleet-server/credential/token/" + tokeName);
        final Response createTokenResponse = client().performRequest(createTokenRequest);
        assertOK(createTokenResponse);

        final ResponseException e =
            expectThrows(ResponseException.class, () -> client().performRequest(createTokenRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(409));
        assertThat(e.getMessage(), containsString("document already exists"));
    }

    public void testGetServiceAccountCredentials() throws IOException {
        final Request getTokensRequest = new Request("GET", "_security/service/elastic/fleet-server/credential");
        final Response getTokensResponse1 = client().performRequest(getTokensRequest);
        assertOK(getTokensResponse1);
        final Map<String, Object> getTokensResponseMap1 = responseAsMap(getTokensResponse1);
        assertThat(getTokensResponseMap1.get("service_account"), equalTo("elastic/fleet-server"));
        assertThat(getTokensResponseMap1.get("count"), equalTo(1));
        assertThat(getTokensResponseMap1.get("tokens"), equalTo(org.elasticsearch.core.Map.of()));
        assertNodesCredentials(getTokensResponseMap1);

        final Request createTokenRequest1 = new Request("POST", "_security/service/elastic/fleet-server/credential/token/api-token-1");
        final Response createTokenResponse1 = client().performRequest(createTokenRequest1);
        assertOK(createTokenResponse1);

        final Request createTokenRequest2 = new Request("POST", "_security/service/elastic/fleet-server/credential/token/api-token-2");
        final Response createTokenResponse2 = client().performRequest(createTokenRequest2);
        assertOK(createTokenResponse2);

        final Response getTokensResponse2 = client().performRequest(getTokensRequest);
        assertOK(getTokensResponse2);
        final Map<String, Object> getTokensResponseMap2 = responseAsMap(getTokensResponse2);
        assertThat(getTokensResponseMap2.get("service_account"), equalTo("elastic/fleet-server"));
        assertThat(getTokensResponseMap2.get("count"), equalTo(3));
        assertThat(getTokensResponseMap2.get("tokens"), equalTo(org.elasticsearch.core.Map.of(
            "api-token-1",org.elasticsearch.core. Map.of(),
            "api-token-2", org.elasticsearch.core.Map.of()
        )));
        assertNodesCredentials(getTokensResponseMap2);

        final Request deleteTokenRequest1 = new Request("DELETE", "_security/service/elastic/fleet-server/credential/token/api-token-2");
        final Response deleteTokenResponse1 = client().performRequest(deleteTokenRequest1);
        assertOK(deleteTokenResponse1);
        assertThat(responseAsMap(deleteTokenResponse1).get("found"), is(true));

        final Response getTokensResponse3 = client().performRequest(getTokensRequest);
        assertOK(getTokensResponse3);
        final Map<String, Object> getTokensResponseMap3 = responseAsMap(getTokensResponse3);
        assertThat(getTokensResponseMap3.get("service_account"), equalTo("elastic/fleet-server"));
        assertThat(getTokensResponseMap3.get("count"), equalTo(2));
        assertThat(getTokensResponseMap3.get("tokens"), equalTo(org.elasticsearch.core.Map.of(
            "api-token-1", org.elasticsearch.core.Map.of()
        )));
        assertNodesCredentials(getTokensResponseMap3);

        final Request deleteTokenRequest2 = new Request("DELETE", "_security/service/elastic/fleet-server/credential/token/non-such-thing");
        final ResponseException e2 = expectThrows(ResponseException.class, () -> client().performRequest(deleteTokenRequest2));
        assertThat(e2.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(EntityUtils.toString(e2.getResponse().getEntity()), equalTo("{\"found\":false}"));
    }

    public void testClearCache() throws IOException {
        final Request clearCacheRequest = new Request("POST", "_security/service/elastic/fleet-server/credential/token/"
            + randomFrom("", "*", "api-token-1", "api-token-1,api-token2") + "/_clear_cache");
        final Response clearCacheResponse = adminClient().performRequest(clearCacheRequest);
        assertOK(clearCacheResponse);
        final Map<String, Object> clearCacheResponseMap = responseAsMap(clearCacheResponse);
        @SuppressWarnings("unchecked")
        final Map<String, Object> nodesMap = (Map<String, Object>) clearCacheResponseMap.get("_nodes");
        assertThat(nodesMap.get("failed"), equalTo(0));
    }

    public void testManageOwnApiKey() throws IOException {
        final String token;
        if (randomBoolean()) {
            token = VALID_SERVICE_TOKEN;
        } else {
            final Request createTokenRequest = new Request("POST", "_security/service/elastic/fleet-server/credential/token/api-token-42");
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
        assertThat(invalidateApiKeysResponseMap.get("invalidated_api_keys"), equalTo(org.elasticsearch.core.List.of(apiKeyId1)));

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
        assertThat(apiKey.get("username"), equalTo("elastic/fleet-server"));
        assertThat(apiKey.get("realm"), equalTo("_service_account"));
        assertThat(apiKey.get("invalidated"), is(invalidated));
    }

    private void assertServiceAccountRoleDescriptor(Response response,
                                                    String serviceAccountPrincipal,
                                                    String roleDescriptorString) throws IOException {
        final Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap, hasEntry(serviceAccountPrincipal, org.elasticsearch.core.Map.of("role_descriptor",
            XContentHelper.convertToMap(new BytesArray(roleDescriptorString), false, XContentType.JSON).v2())));
    }

    @SuppressWarnings("unchecked")
    private void assertNodesCredentials(Map<String, Object> responseMap) {
        final Map<String, Object> nodes = (Map<String, Object>) responseMap.get("nodes_credentials");
        assertThat(nodes, hasKey("_nodes"));
        final Map<String, Object> header = (Map<String, Object>) nodes.get("_nodes");
        assertThat(header.get("total"), equalTo(2));
        assertThat(header.get("successful"), equalTo(2));
        assertThat(header.get("failed"), equalTo(0));
        assertThat(header.get("failures"), nullValue());
        final Map<String, Object> fileTokens = (Map<String, Object>) nodes.get("file_tokens");
        assertThat(fileTokens, hasKey("token1"));
        final Map<String, Object> token1 = (Map<String, Object>) fileTokens.get("token1");
        assertThat((List<String>) token1.get("nodes"), equalTo(org.elasticsearch.core.List.of("javaRestTest-0", "javaRestTest-1")));
    }
}
