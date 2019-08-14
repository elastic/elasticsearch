/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.security.authc.InternalRealms;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SecurityWithBasicLicenseIT extends ESRestTestCase {

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("security_test_user", new SecureString("security-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    public void testWithBasicLicense() throws Exception {
        checkLicenseType("basic");
        checkSecurityEnabled(false);
        checkAuthentication();
        checkHasPrivileges();
        checkIndexWrite();

        final Tuple<String, String> keyAndId = getApiKeyAndId();
        assertAuthenticateWithApiKey(keyAndId, true);

        assertFailToGetToken();
        assertAddRoleWithDLS(false);
        assertAddRoleWithFLS(false);
    }

    public void testWithTrialLicense() throws Exception {
        startTrial();
        String accessToken = null;
        Tuple<String, String> keyAndId = null;
        try {
            checkLicenseType("trial");
            checkSecurityEnabled(true);
            checkAuthentication();
            checkHasPrivileges();
            checkIndexWrite();
            accessToken = getAccessToken();
            keyAndId = getApiKeyAndId();
            assertAuthenticateWithToken(accessToken, true);
            assertAuthenticateWithApiKey(keyAndId, true);
            assertAddRoleWithDLS(true);
            assertAddRoleWithFLS(true);
        } finally {
            revertTrial();
            assertAuthenticateWithToken(accessToken, false);
            assertAuthenticateWithApiKey(keyAndId, true);
            assertFailToGetToken();
            assertAddRoleWithDLS(false);
            assertAddRoleWithFLS(false);
        }
    }

    private void startTrial() throws IOException {
        Response response = client().performRequest(new Request("POST", "/_license/start_trial?acknowledge=true"));
        assertOK(response);
    }

    private void revertTrial() throws IOException {
        client().performRequest(new Request("POST", "/_license/start_basic?acknowledge=true"));
    }

    private void checkLicenseType(String type) throws IOException {
        Map<String, Object> license = getAsMap("/_license");
        assertThat(license, notNullValue());
        assertThat(ObjectPath.evaluate(license, "license.type"), equalTo(type));
    }

    private void checkSecurityEnabled(boolean allowAllRealms) throws IOException {
        Map<String, Object> usage = getAsMap("/_xpack/usage");
        assertThat(usage, notNullValue());
        assertThat(ObjectPath.evaluate(usage, "security.available"), equalTo(true));
        assertThat(ObjectPath.evaluate(usage, "security.enabled"), equalTo(true));
        for (String realm : Arrays.asList("file", "native")) {
            assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".available"), equalTo(true));
            assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".enabled"), equalTo(true));
        }
        for (String realm : InternalRealms.getConfigurableRealmsTypes()) {
            if (realm.equals("file") == false && realm.equals("native") == false) {
                assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".available"), equalTo(allowAllRealms));
                assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".enabled"), equalTo(false));
            }
        }
    }

    private void checkAuthentication() throws IOException {
        final Map<String, Object> auth = getAsMap("/_security/_authenticate");
        // From file realm, configured in build.gradle
        assertThat(ObjectPath.evaluate(auth, "username"), equalTo("security_test_user"));
        assertThat(ObjectPath.evaluate(auth, "roles"), contains("security_test_role"));
    }

    private void checkHasPrivileges() throws IOException {
        final Request request = new Request("GET", "/_security/user/_has_privileges");
        request.setJsonEntity("{" +
            "\"cluster\": [ \"manage\", \"monitor\" ]," +
            "\"index\": [{ \"names\": [ \"index_allowed\", \"index_denied\" ], \"privileges\": [ \"read\", \"all\" ] }]" +
            "}");
        Response response = client().performRequest(request);
        final Map<String, Object> auth = entityAsMap(response);
        assertThat(ObjectPath.evaluate(auth, "username"), equalTo("security_test_user"));
        assertThat(ObjectPath.evaluate(auth, "has_all_requested"), equalTo(false));
        assertThat(ObjectPath.evaluate(auth, "cluster.manage"), equalTo(false));
        assertThat(ObjectPath.evaluate(auth, "cluster.monitor"), equalTo(true));
        assertThat(ObjectPath.evaluate(auth, "index.index_allowed.read"), equalTo(true));
        assertThat(ObjectPath.evaluate(auth, "index.index_allowed.all"), equalTo(false));
        assertThat(ObjectPath.evaluate(auth, "index.index_denied.read"), equalTo(false));
        assertThat(ObjectPath.evaluate(auth, "index.index_denied.all"), equalTo(false));
    }

    private void checkIndexWrite() throws IOException {
        final Request request1 = new Request("POST", "/index_allowed/_doc");
        request1.setJsonEntity("{ \"key\" : \"value\" }");
        Response response1 = client().performRequest(request1);
        final Map<String, Object> result1 = entityAsMap(response1);
        assertThat(ObjectPath.evaluate(result1, "_index"), equalTo("index_allowed"));
        assertThat(ObjectPath.evaluate(result1, "result"), equalTo("created"));

        final Request request2 = new Request("POST", "/index_denied/_doc");
        request2.setJsonEntity("{ \"key\" : \"value\" }");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request2));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(e.getMessage(), containsString("unauthorized for user [security_test_user]"));
    }

    private Request buildGetTokenRequest() {
        final Request getToken = new Request("POST", "/_security/oauth2/token");
        getToken.setJsonEntity("{\"grant_type\" : \"password\",\n" +
            "  \"username\" : \"security_test_user\",\n" +
            "  \"password\" : \"security-test-password\"\n" +
            "}");
        return getToken;
    }

    private Request buildGetApiKeyRequest() {
        final Request getApiKey = new Request("POST", "/_security/api_key");
        getApiKey.setJsonEntity("{\"name\" : \"my-api-key\",\n" +
            "  \"expiration\" : \"2d\",\n" +
            "  \"role_descriptors\" : {} \n" +
            "}");
        return getApiKey;
    }

    private String getAccessToken() throws IOException {
        Response getTokenResponse = adminClient().performRequest(buildGetTokenRequest());
        assertThat(getTokenResponse.getStatusLine().getStatusCode(), equalTo(200));
        final Map<String, Object> tokens = entityAsMap(getTokenResponse);
        return ObjectPath.evaluate(tokens, "access_token").toString();
    }

    private Tuple<String, String> getApiKeyAndId() throws IOException {
        Response getApiKeyResponse = adminClient().performRequest(buildGetApiKeyRequest());
        assertThat(getApiKeyResponse.getStatusLine().getStatusCode(), equalTo(200));
        final Map<String, Object> apiKeyResponseMap = entityAsMap(getApiKeyResponse);
        assertOK(getApiKeyResponse);
        return new Tuple<>(ObjectPath.evaluate(apiKeyResponseMap, "api_key").toString(),
            ObjectPath.evaluate(apiKeyResponseMap, "id").toString());
    }

    private void assertFailToGetToken() {
        ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(buildGetTokenRequest()));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(e.getMessage(), containsString("current license is non-compliant for [security tokens]"));
    }

    private void assertAuthenticateWithToken(String accessToken, boolean shouldSucceed) throws IOException {
        assertNotNull("access token cannot be null", accessToken);
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
        request.setOptions(options);
        if (shouldSucceed) {
            Response authenticateResponse = client().performRequest(request);
            assertOK(authenticateResponse);
            assertEquals("security_test_user", entityAsMap(authenticateResponse).get("username"));
        } else {
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(401));
            assertThat(e.getMessage(), containsString("missing authentication credentials for REST request"));
        }
    }

    private void assertAuthenticateWithApiKey(Tuple<String, String> keyAndId, boolean shouldSucceed) throws IOException {
        assertNotNull("API Key and Id cannot be null", keyAndId);
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        String headerValue = Base64.getEncoder().encodeToString((keyAndId.v2() + ":" + keyAndId.v1()).getBytes(StandardCharsets.UTF_8));
        options.addHeader(HttpHeaders.AUTHORIZATION, "ApiKey " + headerValue);
        request.setOptions(options);
        if (shouldSucceed) {
            Response authenticateResponse = client().performRequest(request);
            assertOK(authenticateResponse);
            assertEquals("admin_user", entityAsMap(authenticateResponse).get("username"));
        } else {
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(401));
            assertThat(e.getMessage(), containsString("missing authentication credentials for REST request"));
        }
    }

    private void assertAddRoleWithDLS(boolean shouldSucceed) throws IOException {
        final Request addRole = new Request("POST", "/_security/role/dlsrole");
        addRole.setJsonEntity("{\n" +
            "  \"cluster\": [\"all\"],\n" +
            "  \"indices\": [\n" +
            "    {\n" +
            "      \"names\": [ \"index1\", \"index2\" ],\n" +
            "      \"privileges\": [\"all\"],\n" +
            "      \"query\": \"{\\\"match\\\": {\\\"title\\\": \\\"foo\\\"}}\" \n" +
            "    }\n" +
            "  ],\n" +
            "  \"run_as\": [ \"other_user\" ],\n" +
            "  \"metadata\" : { // optional\n" +
            "    \"version\" : 1\n" +
            "  }\n" +
            "}");
        if (shouldSucceed) {
            Response addRoleResponse = adminClient().performRequest(addRole);
            assertThat(addRoleResponse.getStatusLine().getStatusCode(), equalTo(200));
        } else {
            ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(addRole));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(e.getMessage(), containsString("current license is non-compliant for [field and document level security]"));
        }
    }

    private void assertAddRoleWithFLS(boolean shouldSucceed) throws IOException {
        final Request addRole = new Request("POST", "/_security/role/dlsrole");
        addRole.setJsonEntity("{\n" +
            "  \"cluster\": [\"all\"],\n" +
            "  \"indices\": [\n" +
            "    {\n" +
            "      \"names\": [ \"index1\", \"index2\" ],\n" +
            "      \"privileges\": [\"all\"],\n" +
            "      \"field_security\" : { // optional\n" +
            "        \"grant\" : [ \"title\", \"body\" ]\n" +
            "      }\n" +
            "    }\n" +
            "  ],\n" +
            "  \"run_as\": [ \"other_user\" ],\n" +
            "  \"metadata\" : { // optional\n" +
            "    \"version\" : 1\n" +
            "  }\n" +
            "}");
        if (shouldSucceed) {
            Response addRoleResponse = adminClient().performRequest(addRole);
            assertThat(addRoleResponse.getStatusLine().getStatusCode(), equalTo(200));
        } else {
            ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(addRole));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(e.getMessage(), containsString("current license is non-compliant for [field and document level security]"));
        }
    }
}
