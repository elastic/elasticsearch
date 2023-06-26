/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
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
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class OpenIdConnectAuthIT extends ESRestTestCase {

    private static final String REALM_NAME = "c2id";
    private static final String REALM_NAME_IMPLICIT = "c2id-implicit";
    private static final String REALM_NAME_PROXY = "c2id-proxy";
    private static final String REALM_NAME_CLIENT_POST_AUTH = "c2id-post";
    private static final String REALM_NAME_CLIENT_JWT_AUTH = "c2id-jwt";
    private static final String FACILITATOR_PASSWORD = "f@cilit@t0rPassword"; // longer than 14 chars
    private static final String REGISTRATION_URL = "http://127.0.0.1:"
        + getEphemeralTcpPortFromProperty("oidc-provider", "8080")
        + "/c2id/clients";
    private static final String LOGIN_API = "http://127.0.0.1:"
        + getEphemeralTcpPortFromProperty("oidc-provider", "8080")
        + "/c2id-login/api/";
    private static final String CLIENT_SECRET = "b07efb7a1cf6ec9462afe7b6d3ab55c6c7880262aa61ac28dded292aca47c9a2";
    // SHA256 of this is defined in x-pack/test/idp-fixture/oidc/override.properties
    private static final String OP_API_BEARER_TOKEN = "811fa888f3e0fdc9e01d4201bfeee46a";
    private static final String ES_PORT = getEphemeralTcpPortFromProperty("elasticsearch-node", "9200");
    private static Path HTTP_TRUSTED_CERT;

    @Before
    public void setupUserAndRoles() throws Exception {
        setFacilitatorUser();
        setRoleMappings();
    }

    @BeforeClass
    public static void readTrustedCert() throws Exception {
        final URL resource = OpenIdConnectAuthIT.class.getResource("/testnode_ec.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /testnode_ec.crt");
        }
        HTTP_TRUSTED_CERT = PathUtils.get(resource.toURI());
    }

    /**
     * C2id server only supports dynamic registration, so we can't pre-seed it's config with our client data. Execute only once
     */
    @BeforeClass
    public static void registerClients() throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String codeClient = "{"
                + "\"grant_types\": [\"authorization_code\"],"
                + "\"response_types\": [\"code\"],"
                + "\"preferred_client_id\":\"https://my.elasticsearch.org/rp\","
                + "\"preferred_client_secret\":\""
                + CLIENT_SECRET
                + "\","
                + "\"redirect_uris\": [\"https://my.fantastic.rp/cb\"],"
                + "\"token_endpoint_auth_method\":\"client_secret_basic\""
                + "}";
            String implicitClient = "{"
                + "\"grant_types\": [\"implicit\"],"
                + "\"response_types\": [\"token id_token\"],"
                + "\"preferred_client_id\":\"elasticsearch-rp\","
                + "\"preferred_client_secret\":\""
                + CLIENT_SECRET
                + "\","
                + "\"redirect_uris\": [\"https://my.fantastic.rp/cb\"]"
                + "}";
            String postClient = "{"
                + "\"grant_types\": [\"authorization_code\"],"
                + "\"response_types\": [\"code\"],"
                + "\"preferred_client_id\":\"elasticsearch-post\","
                + "\"preferred_client_secret\":\""
                + CLIENT_SECRET
                + "\","
                + "\"redirect_uris\": [\"https://my.fantastic.rp/cb\"],"
                + "\"token_endpoint_auth_method\":\"client_secret_post\""
                + "}";
            String jwtClient = "{"
                + "\"grant_types\": [\"authorization_code\"],"
                + "\"response_types\": [\"code\"],"
                + "\"preferred_client_id\":\"elasticsearch-post-jwt\","
                + "\"preferred_client_secret\":\""
                + CLIENT_SECRET
                + "\","
                + "\"redirect_uris\": [\"https://my.fantastic.rp/cb\"],"
                + "\"token_endpoint_auth_method\":\"client_secret_jwt\""
                + "}";
            HttpPost httpPost = new HttpPost(REGISTRATION_URL);
            final BasicHttpContext context = new BasicHttpContext();
            httpPost.setEntity(new StringEntity(codeClient, ContentType.APPLICATION_JSON));
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            httpPost.setHeader("Authorization", "Bearer " + OP_API_BEARER_TOKEN);

            HttpPost httpPost2 = new HttpPost(REGISTRATION_URL);
            httpPost2.setEntity(new StringEntity(implicitClient, ContentType.APPLICATION_JSON));
            httpPost2.setHeader("Accept", "application/json");
            httpPost2.setHeader("Content-type", "application/json");
            httpPost2.setHeader("Authorization", "Bearer " + OP_API_BEARER_TOKEN);

            HttpPost httpPost3 = new HttpPost(REGISTRATION_URL);
            httpPost3.setEntity(new StringEntity(postClient, ContentType.APPLICATION_JSON));
            httpPost3.setHeader("Accept", "application/json");
            httpPost3.setHeader("Content-type", "application/json");
            httpPost3.setHeader("Authorization", "Bearer " + OP_API_BEARER_TOKEN);

            HttpPost httpPost4 = new HttpPost(REGISTRATION_URL);
            httpPost4.setEntity(new StringEntity(jwtClient, ContentType.APPLICATION_JSON));
            httpPost4.setHeader("Accept", "application/json");
            httpPost4.setHeader("Content-type", "application/json");
            httpPost4.setHeader("Authorization", "Bearer " + OP_API_BEARER_TOKEN);

            SocketAccess.doPrivileged(() -> {
                try (CloseableHttpResponse response = httpClient.execute(httpPost, context)) {
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
                }
                try (CloseableHttpResponse response2 = httpClient.execute(httpPost2, context)) {
                    assertThat(response2.getStatusLine().getStatusCode(), equalTo(201));
                }
                try (CloseableHttpResponse response3 = httpClient.execute(httpPost3, context)) {
                    assertThat(response3.getStatusLine().getStatusCode(), equalTo(201));
                }
                try (CloseableHttpResponse response4 = httpClient.execute(httpPost4, context)) {
                    assertThat(response4.getStatusLine().getStatusCode(), equalTo(201));
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

    private String authenticateAtOP(URI opAuthUri) throws Exception {
        // C2ID doesn't have a non JS login page :/, so use their API directly
        // see https://connect2id.com/products/server/docs/guides/login-page
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final BasicHttpContext context = new BasicHttpContext();
            // Initiate the authentication process
            HttpPost httpPost = new HttpPost(LOGIN_API + "initAuthRequest");
            String initJson = "{" + "  \"qs\":\"" + opAuthUri.getRawQuery() + "\"" + "}";
            configureJsonRequest(httpPost, initJson);
            JSONObject initResponse = execute(httpClient, httpPost, context, response -> {
                assertHttpOk(response.getStatusLine());
                return parseJsonResponse(response);
            });
            assertThat(initResponse.getAsString("type"), equalTo("auth"));
            final String sid = initResponse.getAsString("sid");
            // Actually authenticate the user with ldapAuth
            HttpPost loginHttpPost = new HttpPost(
                LOGIN_API + "authenticateSubject?cacheBuster=" + randomAlphaOfLength(8) + "&authSessionId=" + sid
            );
            String loginJson = "{" + "\"username\":\"alice\"," + "\"password\":\"secret\"" + "}";
            configureJsonRequest(loginHttpPost, loginJson);
            execute(httpClient, loginHttpPost, context, response -> {
                assertHttpOk(response.getStatusLine());
                return parseJsonResponse(response);
            });

            HttpPut consentHttpPut = new HttpPut(LOGIN_API + "updateAuthRequest" + "/" + sid + "?cacheBuster=" + randomAlphaOfLength(8));
            String consentJson = "{" + "\"claims\":[\"name\", \"email\"]," + "\"scope\":[\"openid\"]" + "}";
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

    private static String getEphemeralTcpPortFromProperty(String service, String port) {
        String key = "test.fixtures." + service + ".tcp." + port;
        final String value = System.getProperty(key);
        assertNotNull("Expected the actual value for port " + port + " to be in system property " + key, value);
        return value;
    }

    private Map<String, Object> callAuthenticateApiUsingAccessToken(String accessToken) throws Exception {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", "Bearer " + accessToken);
        request.setOptions(options);
        try (RestClient restClient = getClient()) {
            return entityAsMap(restClient.performRequest(request));
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
            logger.warn(new ParameterizedMessage("HTTP Request [{}] failed", request.getURI()), e);
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

    public void testAuthenticateWithCodeFlow() throws Exception {
        final PrepareAuthResponse prepareAuthResponse = getRedirectedFromFacilitator(REALM_NAME);
        final String redirectUri = authenticateAtOP(prepareAuthResponse.getAuthUri());
        Tuple<String, String> tokens = completeAuthentication(
            redirectUri,
            prepareAuthResponse.getState(),
            prepareAuthResponse.getNonce(),
            REALM_NAME
        );
        verifyElasticsearchAccessTokenForCodeFlow(tokens.v1());
    }

    public void testAuthenticateWithCodeFlowAndClientPost() throws Exception {
        final PrepareAuthResponse prepareAuthResponse = getRedirectedFromFacilitator(REALM_NAME_CLIENT_POST_AUTH);
        final String redirectUri = authenticateAtOP(prepareAuthResponse.getAuthUri());
        Tuple<String, String> tokens = completeAuthentication(
            redirectUri,
            prepareAuthResponse.getState(),
            prepareAuthResponse.getNonce(),
            REALM_NAME_CLIENT_POST_AUTH
        );
        verifyElasticsearchAccessTokenForCodeFlow(tokens.v1());
    }

    public void testAuthenticateWithCodeFlowAndClientJwtPost() throws Exception {
        final PrepareAuthResponse prepareAuthResponse = getRedirectedFromFacilitator(REALM_NAME_CLIENT_JWT_AUTH);
        final String redirectUri = authenticateAtOP(prepareAuthResponse.getAuthUri());
        Tuple<String, String> tokens = completeAuthentication(
            redirectUri,
            prepareAuthResponse.getState(),
            prepareAuthResponse.getNonce(),
            REALM_NAME_CLIENT_JWT_AUTH
        );
        verifyElasticsearchAccessTokenForCodeFlow(tokens.v1());
    }

    public void testAuthenticateWithImplicitFlow() throws Exception {
        final PrepareAuthResponse prepareAuthResponse = getRedirectedFromFacilitator(REALM_NAME_IMPLICIT);
        final String redirectUri = authenticateAtOP(prepareAuthResponse.getAuthUri());
        Tuple<String, String> tokens = completeAuthentication(
            redirectUri,
            prepareAuthResponse.getState(),
            prepareAuthResponse.getNonce(),
            REALM_NAME_IMPLICIT
        );
        verifyElasticsearchAccessTokenForImplicitFlow(tokens.v1());
    }

    public void testAuthenticateWithCodeFlowUsingHttpProxy() throws Exception {
        final PrepareAuthResponse prepareAuthResponse = getRedirectedFromFacilitator(REALM_NAME_PROXY);
        final String redirectUri = authenticateAtOP(prepareAuthResponse.getAuthUri());

        Tuple<String, String> tokens = completeAuthentication(
            redirectUri,
            prepareAuthResponse.getState(),
            prepareAuthResponse.getNonce(),
            REALM_NAME_PROXY
        );
        verifyElasticsearchAccessTokenForCodeFlow(tokens.v1());
    }

    public void testAuthenticateWithCodeFlowFailsForWrongRealm() throws Exception {
        final PrepareAuthResponse prepareAuthResponse = getRedirectedFromFacilitator(REALM_NAME);
        final String redirectUri = authenticateAtOP(prepareAuthResponse.getAuthUri());
        // Use existing realm that can't authenticate the response, or a non-existent realm
        ResponseException e = expectThrows(ResponseException.class, () -> {
            completeAuthentication(
                redirectUri,
                prepareAuthResponse.getState(),
                prepareAuthResponse.getNonce(),
                randomFrom(REALM_NAME_IMPLICIT, REALM_NAME + randomAlphaOfLength(8))
            );
        });
        assertThat(401, equalTo(e.getResponse().getStatusLine().getStatusCode()));
    }

    private void verifyElasticsearchAccessTokenForCodeFlow(String accessToken) throws Exception {
        final Map<String, Object> map = callAuthenticateApiUsingAccessToken(accessToken);
        logger.info("Authentication with token Response: " + map);
        assertThat(map.get("username"), equalTo("alice"));
        assertThat((List<?>) map.get("roles"), containsInAnyOrder("kibana_admin", "auditor"));

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertThat(metadata.get("oidc(sub)"), equalTo("alice"));
        assertThat(metadata.get("oidc(iss)"), equalTo("http://oidc-provider:8080/c2id"));
    }

    private void verifyElasticsearchAccessTokenForImplicitFlow(String accessToken) throws Exception {
        final Map<String, Object> map = callAuthenticateApiUsingAccessToken(accessToken);
        logger.info("Authentication with token Response: " + map);
        assertThat(map.get("username"), equalTo("alice"));
        assertThat((List<?>) map.get("roles"), containsInAnyOrder("limited_user", "auditor"));

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertThat(metadata.get("oidc(sub)"), equalTo("alice"));
        assertThat(metadata.get("oidc(iss)"), equalTo("http://oidc-provider:8080/c2id"));
    }

    private PrepareAuthResponse getRedirectedFromFacilitator(String realmName) throws Exception {
        final Map<String, String> body = Collections.singletonMap("realm", realmName);
        Request request = buildRequest("POST", "/_security/oidc/prepare?error_trace=true", body, facilitatorAuth());
        try (RestClient restClient = getClient()) {
            final Response prepare = restClient.performRequest(request);
            assertOK(prepare);
            final Map<String, Object> responseBody = parseResponseAsMap(prepare.getEntity());
            logger.info("Created OpenIDConnect authentication request {}", responseBody);
            final String state = (String) responseBody.get("state");
            final String nonce = (String) responseBody.get("nonce");
            final String authUri = (String) responseBody.get("redirect");
            final String realm = (String) responseBody.get("realm");
            return new PrepareAuthResponse(new URI(authUri), state, nonce, realm);
        }
    }

    private Tuple<String, String> completeAuthentication(String redirectUri, String state, String nonce, @Nullable String realm)
        throws Exception {
        final Map<String, String> body = new HashMap<>();
        body.put("redirect_uri", redirectUri);
        body.put("state", state);
        body.put("nonce", nonce);
        if (realm != null) {
            body.put("realm", realm);
        }
        Request request = buildRequest("POST", "/_security/oidc/authenticate", body, facilitatorAuth());
        try (RestClient restClient = getClient()) {
            final Response authenticate = restClient.performRequest(request);
            assertOK(authenticate);
            final Map<String, Object> responseBody = parseResponseAsMap(authenticate.getEntity());
            logger.info(" OpenIDConnect authentication response {}", responseBody);
            assertNotNull(responseBody.get("access_token"));
            assertNotNull(responseBody.get("refresh_token"));
            assertNotNull(responseBody.get("authentication"));
            assertEquals("alice", ((Map) responseBody.get("authentication")).get("username"));
            return Tuple.tuple(responseBody.get("access_token").toString(), responseBody.get("refresh_token").toString());
        }
    }

    private Request buildRequest(String method, String endpoint, Map<String, ?> body, Header... headers) throws IOException {
        Request request = new Request(method, endpoint);
        XContentBuilder builder = XContentFactory.jsonBuilder().map(body);
        if (body != null) {
            request.setJsonEntity(BytesReference.bytes(builder).utf8ToString());
        }
        final RequestOptions.Builder options = request.getOptions().toBuilder();
        for (Header header : headers) {
            options.addHeader(header.getName(), header.getValue());
        }
        request.setOptions(options);
        return request;
    }

    private static BasicHeader facilitatorAuth() {
        final String auth = UsernamePasswordToken.basicAuthHeaderValue("facilitator", new SecureString(FACILITATOR_PASSWORD.toCharArray()));
        return new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, auth);
    }

    private Map<String, Object> parseResponseAsMap(HttpEntity entity) throws IOException {
        return convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
    }

    private void assertHttpOk(StatusLine status) {
        assertThat("Unexpected HTTP Response status: " + status, status.getStatusCode(), Matchers.equalTo(200));
    }

    /**
     * We create a user named `facilitator` with the appropriate privileges ( `manage_oidc` ). A facilitator web app
     * would need to create one also, in order to access the OIDC related APIs on behalf of the user.
     */
    private void setFacilitatorUser() throws Exception {
        try (RestClient restClient = getClient()) {
            Request createRoleRequest = new Request("PUT", "/_security/role/facilitator");
            createRoleRequest.setJsonEntity("{ \"cluster\" : [\"manage_oidc\", \"manage_token\"] }");
            restClient.performRequest(createRoleRequest);
            Request createUserRequest = new Request("PUT", "/_security/user/facilitator");
            createUserRequest.setJsonEntity("{ \"password\" : \"" + FACILITATOR_PASSWORD + "\", \"roles\" : [\"facilitator\"] }");
            restClient.performRequest(createUserRequest);
        }
    }

    private void setRoleMappings() throws Exception {
        try (RestClient restClient = getClient()) {
            Request createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_kibana");
            createRoleMappingRequest.setJsonEntity(
                "{ \"roles\" : [\"kibana_admin\"],"
                    + "\"enabled\": true,"
                    + "\"rules\": {"
                    + "  \"any\" : ["
                    + "    {\"field\": { \"realm.name\": \""
                    + REALM_NAME
                    + "\"} },"
                    + "    {\"field\": { \"realm.name\": \""
                    + REALM_NAME_PROXY
                    + "\"} },"
                    + "    {\"field\": { \"realm.name\": \""
                    + REALM_NAME_CLIENT_POST_AUTH
                    + "\"} },"
                    + "    {\"field\": { \"realm.name\": \""
                    + REALM_NAME_CLIENT_JWT_AUTH
                    + "\"} }"
                    + "  ]"
                    + "}"
                    + "}"
            );
            restClient.performRequest(createRoleMappingRequest);

            createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_limited");
            createRoleMappingRequest.setJsonEntity(
                "{ \"roles\" : [\"limited_user\"],"
                    + "\"enabled\": true,"
                    + "\"rules\": {"
                    + "\"field\": { \"realm.name\": \""
                    + REALM_NAME_IMPLICIT
                    + "\"}"
                    + "}"
                    + "}"
            );
            restClient.performRequest(createRoleMappingRequest);

            createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_auditor");
            createRoleMappingRequest.setJsonEntity(
                "{ \"roles\" : [\"auditor\"]," + "\"enabled\": true," + "\"rules\": {" + "\"field\": { \"groups\": \"audit\"}" + "}" + "}"
            );
            restClient.performRequest(createRoleMappingRequest);
        }
    }

    private RestClient getClient() throws Exception {
        return buildClient(restAdminSettings(), new HttpHost[] { new HttpHost("localhost", Integer.parseInt(ES_PORT), "https") });
    }

    /**
     * Simple POJO encapsulating a response to calling /_security/oidc/prepare
     */
    class PrepareAuthResponse {
        private URI authUri;
        private String state;
        private String nonce;

        PrepareAuthResponse(URI authUri, String state, String nonce, @Nullable String realm) {
            this.authUri = authUri;
            this.state = state;
            this.nonce = nonce;
        }

        URI getAuthUri() {
            return authUri;
        }

        String getState() {
            return state;
        }

        String getNonce() {
            return nonce;
        }

    }
}
