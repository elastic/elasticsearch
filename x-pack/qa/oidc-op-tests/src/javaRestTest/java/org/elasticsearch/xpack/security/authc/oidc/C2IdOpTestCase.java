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
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public abstract class C2IdOpTestCase extends ESRestTestCase {
    protected static final String TEST_SUBJECT_ID = "alice";

    // URLs for accessing the C2id OP
    private static final String C2OP_PORT = getEphemeralTcpPortFromProperty("oidc-provider", "8080");
    private static final String C2ID_HOST = "http://127.0.0.1:" + C2OP_PORT;
    protected static final String C2ID_ISSUER = C2ID_HOST + "/c2id";
    private static final String PROXY_PORT = getEphemeralTcpPortFromProperty("http-proxy", "8888");
    private static final String C2ID_LOGIN_API = C2ID_HOST + "/c2id-login/api/";
    private static final String C2ID_REGISTRATION_URL = C2ID_HOST + "/c2id/clients";
    protected static final String C2ID_AUTH_ENDPOINT = C2ID_HOST + "/c2id-login";

    // SHA256 of this is defined in x-pack/test/idp-fixture/oidc/override.properties
    private static final String OP_API_BEARER_TOKEN = "811fa888f3e0fdc9e01d4201bfeee46a";

    private static Path HTTP_TRUSTED_CERT;

    private static final String CLIENT_SECRET = "b07efb7a1cf6ec9462afe7b6d3ab55c6c7880262aa61ac28dded292aca47c9a2";
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(1)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "true")
        .setting("xpack.security.http.ssl.keystore.path", "testnode.jks")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.realms.file.file.order", "0")
        .setting("xpack.security.authc.realms.native.native.order", "1")
        .setting("xpack.security.authc.realms.oidc.c2id.order", "2")
        .setting("xpack.security.authc.realms.oidc.c2id.op.issuer", C2ID_ISSUER)
        .setting("xpack.security.authc.realms.oidc.c2id.op.authorization_endpoint", C2ID_HOST + "/c2id-login")
        .setting("xpack.security.authc.realms.oidc.c2id.op.token_endpoint", C2ID_HOST + "/c2id/token")
        .setting("xpack.security.authc.realms.oidc.c2id.op.userinfo_endpoint", C2ID_HOST + "/c2id/userinfo")
        .setting("xpack.security.authc.realms.oidc.c2id.op.jwkset_path", "op-jwks.json")
        .setting("xpack.security.authc.realms.oidc.c2id.rp.redirect_uri", "https://my.fantastic.rp/cb")
        .setting("xpack.security.authc.realms.oidc.c2id.rp.client_id", "https://my.elasticsearch.org/rp")
        .setting("xpack.security.authc.realms.oidc.c2id.rp.response_type", "code")
        .setting("xpack.security.authc.realms.oidc.c2id.claims.principal", "sub")
        .setting("xpack.security.authc.realms.oidc.c2id.claims.name", "name")
        .setting("xpack.security.authc.realms.oidc.c2id.claims.mail", "email")
        .setting("xpack.security.authc.realms.oidc.c2id.claims.groups", "groups")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.order", "3")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.op.issuer", C2ID_ISSUER)
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.op.authorization_endpoint", C2ID_HOST + "/c2id-login")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.op.token_endpoint", C2ID_HOST + "/c2id/token")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.op.userinfo_endpoint", C2ID_HOST + "/c2id/userinfo")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.op.jwkset_path", "op-jwks.json")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.rp.redirect_uri", "https://my.fantastic.rp/cb")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.rp.client_id", "elasticsearch-rp")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.rp.response_type", "id_token token")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.claims.principal", "sub")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.claims.name", "name")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.claims.mail", "email")
        .setting("xpack.security.authc.realms.oidc.c2id-implicit.claims.groups", "groups")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.order", "4")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.op.issuer", C2ID_ISSUER)
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.op.authorization_endpoint", C2ID_HOST + "/c2id-login")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.op.token_endpoint", C2ID_HOST + "/c2id/token")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.op.userinfo_endpoint", C2ID_HOST + "/c2id/userinfo")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.op.jwkset_path", "op-jwks.json")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.rp.redirect_uri", "https://my.fantastic.rp/cb")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.rp.client_id", "https://my.elasticsearch.org/rp")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.rp.response_type", "code")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.claims.principal", "sub")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.claims.name", "name")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.claims.mail", "email")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.claims.groups", "groups")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.http.proxy.host", "127.0.0.1")
        .setting("xpack.security.authc.realms.oidc.c2id-proxy.http.proxy.port", PROXY_PORT)
        .setting("xpack.security.authc.realms.oidc.c2id-post.order", "5")
        .setting("xpack.security.authc.realms.oidc.c2id-post.op.issuer", C2ID_ISSUER)
        .setting("xpack.security.authc.realms.oidc.c2id-post.op.authorization_endpoint", C2ID_HOST + "/c2id-login")
        .setting("xpack.security.authc.realms.oidc.c2id-post.op.token_endpoint", C2ID_HOST + "/c2id/token")
        .setting("xpack.security.authc.realms.oidc.c2id-post.op.userinfo_endpoint", C2ID_HOST + "/c2id/userinfo")
        .setting("xpack.security.authc.realms.oidc.c2id-post.op.jwkset_path", "op-jwks.json")
        .setting("xpack.security.authc.realms.oidc.c2id-post.rp.redirect_uri", "https://my.fantastic.rp/cb")
        .setting("xpack.security.authc.realms.oidc.c2id-post.rp.client_id", "elasticsearch-post")
        .setting("xpack.security.authc.realms.oidc.c2id-post.rp.client_auth_method", "client_secret_post")
        .setting("xpack.security.authc.realms.oidc.c2id-post.rp.response_type", "code")
        .setting("xpack.security.authc.realms.oidc.c2id-post.claims.principal", "sub")
        .setting("xpack.security.authc.realms.oidc.c2id-post.claims.name", "name")
        .setting("xpack.security.authc.realms.oidc.c2id-post.claims.mail", "email")
        .setting("xpack.security.authc.realms.oidc.c2id-post.claims.groups", "groups")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.order", "6")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.op.issuer", C2ID_ISSUER)
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.op.authorization_endpoint", C2ID_HOST + "/c2id-login")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.op.token_endpoint", C2ID_HOST + "/c2id/token")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.op.userinfo_endpoint", C2ID_HOST + "/c2id/userinfo")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.op.jwkset_path", "op-jwks.json")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.rp.redirect_uri", "https://my.fantastic.rp/cb")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.rp.client_id", "elasticsearch-post-jwt")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.rp.client_auth_method", "client_secret_jwt")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.rp.response_type", "code")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.claims.principal", "sub")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.claims.name", "name")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.claims.mail", "email")
        .setting("xpack.security.authc.realms.oidc.c2id-jwt.claims.groups", "groups")
        .setting("xpack.security.authc.realms.jwt.op-jwt.order", "7")
        .setting("xpack.security.authc.realms.jwt.op-jwt.allowed_issuer", C2ID_ISSUER)
        .setting("xpack.security.authc.realms.jwt.op-jwt.allowed_audiences", "elasticsearch-jwt1,elasticsearch-jwt2")
        .setting("xpack.security.authc.realms.jwt.op-jwt.pkc_jwkset_path", "op-jwks.json")
        .setting("xpack.security.authc.realms.jwt.op-jwt.claims.principal", "sub")
        .setting("xpack.security.authc.realms.jwt.op-jwt.claims.groups", "groups")
        .setting("xpack.security.authc.realms.jwt.op-jwt.client_authentication.type", "shared_secret")
        .keystore("bootstrap.password", "x-pack-test-password")
        .keystore("xpack.security.http.ssl.keystore.secure_password", "testnode")
        .keystore("xpack.security.authc.realms.oidc.c2id.rp.client_secret", CLIENT_SECRET)
        .keystore("xpack.security.authc.realms.oidc.c2id-implicit.rp.client_secret", CLIENT_SECRET)
        .keystore("xpack.security.authc.realms.oidc.c2id-proxy.rp.client_secret", CLIENT_SECRET)
        .keystore("xpack.security.authc.realms.oidc.c2id-post.rp.client_secret", CLIENT_SECRET)
        .keystore("xpack.security.authc.realms.oidc.c2id-jwt.rp.client_secret", CLIENT_SECRET)
        .keystore("xpack.security.authc.realms.jwt.op-jwt.client_authentication.shared_secret", "jwt-realm-shared-secret")
        .configFile("testnode.jks", Resource.fromClasspath("ssl/testnode.jks"))
        .configFile("op-jwks.json", Resource.fromClasspath("op-jwks.json"))
        .user("x_pack_rest_user", "x-pack-test-password", "superuser", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getProtocol() {
        return "https";
    }

    @BeforeClass
    public static void readTrustedCert() throws Exception {
        final URL resource = OpenIdConnectAuthIT.class.getResource("/ssl/testnode_ec.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/testnode_ec.crt");
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
                        assertBusy(() -> assertThat(response.getStatusLine().getStatusCode(), equalTo(201)), 30, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        fail("Encountered exception while registering client: " + e);
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
            String initJson = Strings.format("""
                {"qs":"%s"}
                """, opAuthUri.getRawQuery());
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
        return buildClient(restAdminSettings(), getClusterHosts().toArray(new HttpHost[0]));
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
