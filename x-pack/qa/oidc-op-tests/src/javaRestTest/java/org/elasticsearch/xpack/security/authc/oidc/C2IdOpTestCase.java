/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.oidc;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public abstract class C2IdOpTestCase extends ESRestTestCase {
    protected static final String TEST_SUBJECT_ID = "alice";

    // URLs for accessing the C2id OP
    private static final String C2OP_PORT = getEphemeralTcpPortFromProperty("oidc-provider", "8080");
    private static final String C2ID_LOGIN_API = "http://127.0.0.1:" + C2OP_PORT + "/c2id-login/api/";
    private static final String C2ID_REGISTRATION_URL = "http://127.0.0.1:" + C2OP_PORT + "/c2id/clients";
    protected static final String C2ID_AUTH_ENDPOINT = "http://127.0.0.1:" + C2OP_PORT + "/c2id-login";

    private static final String ES_PORT = getEphemeralTcpPortFromProperty("elasticsearch-node", "9200");
    // SHA256 of this is defined in x-pack/test/idp-fixture/oidc/override.properties
    private static final String OP_API_BEARER_TOKEN = "811fa888f3e0fdc9e01d4201bfeee46a";

    private static Path HTTP_TRUSTED_CERT;

    @BeforeClass
    public static void readTrustedCert() throws Exception {
        final URL resource = OpenIdConnectAuthIT.class.getResource("/testnode_ec.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /testnode_ec.crt");
        }
        HTTP_TRUSTED_CERT = PathUtils.get(resource.toURI());
    }

    protected static String getEphemeralTcpPortFromProperty(String service, String port) {
        String key = "test.fixtures." + service + ".tcp." + port;
        final String value = System.getProperty(key);
        assertNotNull("Expected the actual value for port " + port + " to be in system property " + key, value);
        return value;
    }

    /**
     * Register one or more OIDC clients on the C2id server. This should be done once (per client) only.
     * C2id server only supports dynamic registration, so we can't pre-seed its config with our client data.
     */
    protected static void registerClients(String... jsonBody) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final BasicHttpContext context = new BasicHttpContext();

            final List<HttpPost> requests = new ArrayList<>(jsonBody.length);
            for (String body : jsonBody) {
                HttpPost httpPost = new HttpPost(C2ID_REGISTRATION_URL);
                httpPost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-type", "application/json");
                httpPost.setHeader("Authorization", "Bearer " + OP_API_BEARER_TOKEN);
                requests.add(httpPost);
            }

            SocketAccess.doPrivileged(() -> {
                for (HttpPost httpPost : requests) {
                    try (CloseableHttpResponse response = httpClient.execute(httpPost, context)) {
                        assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
                    }
                }
            });
        }
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CERTIFICATE_AUTHORITIES, HTTP_TRUSTED_CERT)
            .build();
    }

    protected String authenticateAtOP(URI opAuthUri) throws Exception {
        // C2ID doesn't have a non JS login page :/, so use their API directly
        // see https://connect2id.com/products/server/docs/guides/login-page
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final BasicHttpContext context = new BasicHttpContext();
            // Initiate the authentication process
            HttpPost httpPost = new HttpPost(C2ID_LOGIN_API + "initAuthRequest");
            String initJson = """
                {"qs":"%s"}
                """.formatted(opAuthUri.getRawQuery());
            configureJsonRequest(httpPost, initJson);
            JSONObject initResponse = execute(httpClient, httpPost, context, response -> {
                assertHttpOk(response.getStatusLine());
                return parseJsonResponse(response);
            });
            assertThat(initResponse.getAsString("type"), equalTo("auth"));
            final String sid = initResponse.getAsString("sid");
            // Actually authenticate the user with ldapAuth
            HttpPost loginHttpPost = new HttpPost(
                C2ID_LOGIN_API + "authenticateSubject?cacheBuster=" + randomAlphaOfLength(8) + "&authSessionId=" + sid
            );
            String loginJson = """
                {"username":"alice","password":"secret"}""";
            configureJsonRequest(loginHttpPost, loginJson);
            execute(httpClient, loginHttpPost, context, response -> {
                assertHttpOk(response.getStatusLine());
                return parseJsonResponse(response);
            });

            HttpPut consentHttpPut = new HttpPut(
                C2ID_LOGIN_API + "updateAuthRequest" + "/" + sid + "?cacheBuster=" + randomAlphaOfLength(8)
            );
            String consentJson = """
                {"claims":["name", "email"],"scope":["openid"]}""";
            configureJsonRequest(consentHttpPut, consentJson);
            JSONObject jsonConsentResponse = execute(httpClient, consentHttpPut, context, response -> {
                assertHttpOk(response.getStatusLine());
                return parseJsonResponse(response);
            });
            assertThat(jsonConsentResponse.getAsString("type"), equalTo("response"));
            JSONObject parameters = (JSONObject) jsonConsentResponse.get("parameters");
            return parameters.getAsString("uri");
        }
    }

    private <T> T execute(
        CloseableHttpClient client,
        HttpEntityEnclosingRequestBase request,
        HttpContext context,
        CheckedFunction<HttpResponse, T, Exception> body
    ) throws Exception {
        final int timeout = (int) TimeValue.timeValueSeconds(90).millis();
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectionRequestTimeout(timeout)
            .setConnectTimeout(timeout)
            .setSocketTimeout(timeout)
            .build();
        request.setConfig(requestConfig);
        logger.info(
            "Execute HTTP " + request.getMethod() + " " + request.getURI() + " with payload " + EntityUtils.toString(request.getEntity())
        );
        try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(request, context))) {
            return body.apply(response);
        } catch (Exception e) {
            logger.warn(() -> "HTTP Request [" + request.getURI() + "] failed", e);
            throw e;
        }
    }

    private JSONObject parseJsonResponse(HttpResponse response) throws Exception {
        JSONParser parser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
        String entity = EntityUtils.toString(response.getEntity());
        logger.info("Response entity as string: " + entity);
        return (JSONObject) parser.parse(entity);
    }

    private void configureJsonRequest(HttpEntityEnclosingRequestBase request, String jsonBody) {
        StringEntity entity = new StringEntity(jsonBody, ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        request.setHeader("Accept", "application/json");
        request.setHeader("Content-type", "application/json");
    }

    private void assertHttpOk(StatusLine status) {
        assertThat("Unexpected HTTP Response status: " + status, status.getStatusCode(), Matchers.equalTo(200));
    }

    protected RestClient getElasticsearchClient() throws IOException {
        return buildClient(restAdminSettings(), new HttpHost[] { new HttpHost("localhost", Integer.parseInt(ES_PORT), "https") });
    }

    protected Map<String, Object> callAuthenticateApiUsingBearerToken(String accessToken) throws Exception {
        return callAuthenticateApiUsingBearerToken(accessToken, RequestOptions.DEFAULT);
    }

    protected Map<String, Object> callAuthenticateApiUsingBearerToken(String accessToken, RequestOptions baseOptions) throws IOException {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = baseOptions.toBuilder();
        options.addHeader("Authorization", "Bearer " + accessToken);
        request.setOptions(options);
        try (RestClient restClient = getElasticsearchClient()) {
            return entityAsMap(restClient.performRequest(request));
        }
    }
}
