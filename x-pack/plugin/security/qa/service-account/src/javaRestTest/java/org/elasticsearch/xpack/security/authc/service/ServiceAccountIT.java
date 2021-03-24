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
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + INVALID_SERVICE_TOKEN));
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(401));
        assertThat(e.getMessage(), containsString("failed to authenticate service account [elastic/fleet] with token name [token1]"));
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
}
