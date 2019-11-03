/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
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
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class OpenIdConnectAuthIT extends ESRestTestCase {

    private static final String REALM_NAME = "c2id";
    private static final String REALM_NAME_IMPLICIT = "c2id-implicit";
    private static final String FACILITATOR_PASSWORD = "f@cilit@t0r";
    private static final String REGISTRATION_URL = "http://127.0.0.1:" + getEphemeralPortFromProperty("8080") + "/c2id/clients";
    private static final String LOGIN_API = "http://127.0.0.1:" + getEphemeralPortFromProperty("8080") + "/c2id-login/api/";

    @Before
    public void setupUserAndRoles() throws IOException {
        setFacilitatorUser();
        setRoleMappings();
    }

    /**
     * C2id server only supports dynamic registration, so we can't pre-seed it's config with our client data. Execute only once
     */
    @BeforeClass
    public static void registerClients() throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String codeClient = "{" +
                "\"grant_types\": [\"authorization_code\"]," +
                "\"response_types\": [\"code\"]," +
                "\"preferred_client_id\":\"https://my.elasticsearch.org/rp\"," +
                "\"preferred_client_secret\":\"b07efb7a1cf6ec9462afe7b6d3ab55c6c7880262aa61ac28dded292aca47c9a2\"," +
                "\"redirect_uris\": [\"https://my.fantastic.rp/cb\"]" +
                "}";
            String implicitClient = "{" +
                "\"grant_types\": [\"implicit\"]," +
                "\"response_types\": [\"token id_token\"]," +
                "\"preferred_client_id\":\"elasticsearch-rp\"," +
                "\"preferred_client_secret\":\"b07efb7a1cf6ec9462afe7b6d3ab55c6c7880262aa61ac28dded292aca47c9a2\"," +
                "\"redirect_uris\": [\"https://my.fantastic.rp/cb\"]" +
                "}";
            HttpPost httpPost = new HttpPost(REGISTRATION_URL);
            final BasicHttpContext context = new BasicHttpContext();
            httpPost.setEntity(new StringEntity(codeClient, ContentType.APPLICATION_JSON));
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            httpPost.setHeader("Authorization", "Bearer 811fa888f3e0fdc9e01d4201bfeee46a");
            CloseableHttpResponse response = SocketAccess.doPrivileged(() -> httpClient.execute(httpPost, context));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            httpPost.setEntity(new StringEntity(implicitClient, ContentType.APPLICATION_JSON));
            HttpPost httpPost2 = new HttpPost(REGISTRATION_URL);
            httpPost2.setEntity(new StringEntity(implicitClient, ContentType.APPLICATION_JSON));
            httpPost2.setHeader("Accept", "application/json");
            httpPost2.setHeader("Content-type", "application/json");
            httpPost2.setHeader("Authorization", "Bearer 811fa888f3e0fdc9e01d4201bfeee46a");
            CloseableHttpResponse response2 = SocketAccess.doPrivileged(() -> httpClient.execute(httpPost2, context));
            assertThat(response2.getStatusLine().getStatusCode(), equalTo(200));
        }
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    private String authenticateAtOP(URI opAuthUri) throws Exception {
        // C2ID doesn't have a non JS login page :/, so use their API directly
        // see https://connect2id.com/products/server/docs/guides/login-page
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final BasicHttpContext context = new BasicHttpContext();
            // Initiate the authentication process
            HttpPost httpPost = new HttpPost(LOGIN_API + "initAuthRequest");
            String initJson = "{" +
                "  \"qs\":\"" + opAuthUri.getRawQuery() + "\"" +
                "}";
            configureJsonRequest(httpPost, initJson);
            JSONObject initResponse = execute(httpClient, httpPost, context, response -> {
                assertHttpOk(response.getStatusLine());
                return parseJsonResponse(response);
            });
            assertThat(initResponse.getAsString("type"), equalTo("auth"));
            final String sid = initResponse.getAsString("sid");
            // Actually authenticate the user with ldapAuth
            HttpPost loginHttpPost = new HttpPost(LOGIN_API + "authenticateSubject?cacheBuster=" + randomAlphaOfLength(8));
            String loginJson = "{" +
                "\"username\":\"alice\"," +
                "\"password\":\"secret\"" +
                "}";
            configureJsonRequest(loginHttpPost, loginJson);
            JSONObject loginJsonResponse = execute(httpClient, loginHttpPost, context, response -> {
                assertHttpOk(response.getStatusLine());
                return parseJsonResponse(response);
            });
            // Get the consent screen
            HttpPut consentFetchHttpPut =
                new HttpPut(LOGIN_API + "updateAuthRequest" + "/" + sid + "?cacheBuster=" + randomAlphaOfLength(8));
            String consentFetchJson = "{" +
                "\"sub\": \"" + loginJsonResponse.getAsString("id") + "\"," +
                "\"acr\": \"http://loa.c2id.com/basic\"," +
                "\"amr\": [\"pwd\"]," +
                "\"data\": {" +
                "\"email\": \"" + loginJsonResponse.getAsString("email") + "\"," +
                "\"name\": \"" + loginJsonResponse.getAsString("name") + "\"" +
                "}" +
                "}";
            configureJsonRequest(consentFetchHttpPut, consentFetchJson);
            JSONObject consentFetchResponse = execute(httpClient, consentFetchHttpPut, context, response -> {
                assertHttpOk(response.getStatusLine());
                return parseJsonResponse(response);
            });
            if (consentFetchResponse.getAsString("type").equals("consent")) {
                // If needed, submit the consent
                HttpPut consentHttpPut =
                    new HttpPut(LOGIN_API + "updateAuthRequest" + "/" + sid + "?cacheBuster=" + randomAlphaOfLength(8));
                String consentJson = "{" +
                    "\"claims\":[\"name\", \"email\"]," +
                    "\"scope\":[\"openid\"]" +
                    "}";
                configureJsonRequest(consentHttpPut, consentJson);
                JSONObject jsonConsentResponse = execute(httpClient, consentHttpPut, context, response -> {
                    assertHttpOk(response.getStatusLine());
                    return parseJsonResponse(response);
                });
                assertThat(jsonConsentResponse.getAsString("type"), equalTo("response"));
                JSONObject parameters = (JSONObject) jsonConsentResponse.get("parameters");
                return parameters.getAsString("uri");
            } else if (consentFetchResponse.getAsString("type").equals("response")) {
                JSONObject parameters = (JSONObject) consentFetchResponse.get("parameters");
                return parameters.getAsString("uri");
            } else {
                fail("Received an invalid response from the OP");
                return null;
            }
        }
    }

    private static String getEphemeralPortFromProperty(String port) {
        String key = "test.fixtures.oidc-provider.tcp." + port;
        final String value = System.getProperty(key);
        assertNotNull("Expected the actual value for port " + port + " to be in system property " + key, value);
        return value;
    }

    private Map<String, Object> callAuthenticateApiUsingAccessToken(String accessToken) throws IOException {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", "Bearer " + accessToken);
        request.setOptions(options);
        return entityAsMap(client().performRequest(request));
    }

    private <T> T execute(CloseableHttpClient client, HttpEntityEnclosingRequestBase request,
                          HttpContext context, CheckedFunction<HttpResponse, T, Exception> body)
        throws Exception {
        final int timeout = (int) TimeValue.timeValueSeconds(90).millis();
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectionRequestTimeout(timeout)
            .setConnectTimeout(timeout)
            .setSocketTimeout(timeout)
            .build();
        request.setConfig(requestConfig);
        logger.info("Execute HTTP " + request.getMethod() + " " + request.getURI() +
            " with payload " + EntityUtils.toString(request.getEntity()));
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
        final String realm = randomBoolean() ? null : prepareAuthResponse.getRealm();
        Tuple<String, String> tokens = completeAuthentication(redirectUri, prepareAuthResponse.getState(),
            prepareAuthResponse.getNonce(), realm);
        verifyElasticsearchAccessTokenForCodeFlow(tokens.v1());
    }

    public void testAuthenticateWithImplicitFlow() throws Exception {
        final PrepareAuthResponse prepareAuthResponse = getRedirectedFromFacilitator(REALM_NAME_IMPLICIT);
        final String redirectUri = authenticateAtOP(prepareAuthResponse.getAuthUri());
        final String realm = randomBoolean() ? null : prepareAuthResponse.getRealm();

        Tuple<String, String> tokens = completeAuthentication(redirectUri, prepareAuthResponse.getState(),
            prepareAuthResponse.getNonce(), realm);
        verifyElasticsearchAccessTokenForImplicitFlow(tokens.v1());
    }

    public void testAuthenticateWithCodeFlowFailsForWrongRealm() throws Exception {
        final PrepareAuthResponse prepareAuthResponse = getRedirectedFromFacilitator(REALM_NAME);
        final String redirectUri = authenticateAtOP(prepareAuthResponse.getAuthUri());
        // Use existing realm that can't authenticate the response, or a non-existent realm
        ResponseException e = expectThrows(ResponseException.class, () -> {
            completeAuthentication(redirectUri,
                prepareAuthResponse.getState(),
                prepareAuthResponse.getNonce(), randomFrom(REALM_NAME_IMPLICIT, REALM_NAME + randomAlphaOfLength(8)));
        });
        assertThat(401, equalTo(e.getResponse().getStatusLine().getStatusCode()));
    }

    private void verifyElasticsearchAccessTokenForCodeFlow(String accessToken) throws IOException {
        final Map<String, Object> map = callAuthenticateApiUsingAccessToken(accessToken);
        logger.info("Authentication with token Response: " + map);
        assertThat(map.get("username"), equalTo("alice"));
        assertThat((List<?>) map.get("roles"), containsInAnyOrder("kibana_user", "auditor"));

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertThat(metadata.get("oidc(sub)"), equalTo("alice"));
        assertThat(metadata.get("oidc(iss)"), equalTo("http://localhost:8080"));
    }

    private void verifyElasticsearchAccessTokenForImplicitFlow(String accessToken) throws IOException {
        final Map<String, Object> map = callAuthenticateApiUsingAccessToken(accessToken);
        logger.info("Authentication with token Response: " + map);
        assertThat(map.get("username"), equalTo("alice"));
        assertThat((List<?>) map.get("roles"), containsInAnyOrder("limited_user", "auditor"));

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertThat(metadata.get("oidc(sub)"), equalTo("alice"));
        assertThat(metadata.get("oidc(iss)"), equalTo("http://localhost:8080"));
    }


    private PrepareAuthResponse getRedirectedFromFacilitator(String realmName) throws Exception {
        final Map<String, String> body = Collections.singletonMap("realm", realmName);
        Request request = buildRequest("POST", "/_security/oidc/prepare", body, facilitatorAuth());
        final Response prepare = client().performRequest(request);
        assertOK(prepare);
        final Map<String, Object> responseBody = parseResponseAsMap(prepare.getEntity());
        logger.info("Created OpenIDConnect authentication request {}", responseBody);
        final String state = (String) responseBody.get("state");
        final String nonce = (String) responseBody.get("nonce");
        final String authUri = (String) responseBody.get("redirect");
        final String realm = (String) responseBody.get("realm");
        return new PrepareAuthResponse(new URI(authUri), state, nonce, realm);
    }

    private Tuple<String, String> completeAuthentication(String redirectUri, String state, String nonce, @Nullable String realm)
        throws Exception {
        final Map<String, String> body = new HashMap<>();
        body.put("redirect_uri", redirectUri);
        body.put("state", state);
        body.put("nonce", nonce);
        if (realm != null){
            body.put("realm", realm);
        }
        Request request = buildRequest("POST", "/_security/oidc/authenticate", body, facilitatorAuth());
        final Response authenticate = client().performRequest(request);
        assertOK(authenticate);
        final Map<String, Object> responseBody = parseResponseAsMap(authenticate.getEntity());
        logger.info(" OpenIDConnect authentication response {}", responseBody);
        assertNotNull(responseBody.get("access_token"));
        assertNotNull(responseBody.get("refresh_token"));
        return new Tuple(responseBody.get("access_token"), responseBody.get("refresh_token"));
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
        final String auth =
            UsernamePasswordToken.basicAuthHeaderValue("facilitator", new SecureString(FACILITATOR_PASSWORD.toCharArray()));
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
    private void setFacilitatorUser() throws IOException {
        Request createRoleRequest = new Request("PUT", "/_security/role/facilitator");
        createRoleRequest.setJsonEntity("{ \"cluster\" : [\"manage_oidc\", \"manage_token\"] }");
        adminClient().performRequest(createRoleRequest);
        Request createUserRequest = new Request("PUT", "/_security/user/facilitator");
        createUserRequest.setJsonEntity("{ \"password\" : \"" + FACILITATOR_PASSWORD + "\", \"roles\" : [\"facilitator\"] }");
        adminClient().performRequest(createUserRequest);
    }

    private void setRoleMappings() throws IOException {
        Request createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_kibana");
        createRoleMappingRequest.setJsonEntity("{ \"roles\" : [\"kibana_user\"]," +
            "\"enabled\": true," +
            "\"rules\": {" +
            "\"field\": { \"realm.name\": \"" + REALM_NAME + "\"}" +
            "}" +
            "}");
        adminClient().performRequest(createRoleMappingRequest);

        createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_limited");
        createRoleMappingRequest.setJsonEntity("{ \"roles\" : [\"limited_user\"]," +
            "\"enabled\": true," +
            "\"rules\": {" +
            "\"field\": { \"realm.name\": \"" + REALM_NAME_IMPLICIT + "\"}" +
            "}" +
            "}");
        adminClient().performRequest(createRoleMappingRequest);

        createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_auditor");
        createRoleMappingRequest.setJsonEntity("{ \"roles\" : [\"auditor\"]," +
            "\"enabled\": true," +
            "\"rules\": {" +
            "\"field\": { \"groups\": \"audit\"}" +
            "}" +
            "}");
        adminClient().performRequest(createRoleMappingRequest);
    }


    /**
     * Simple POJO encapsulating a response to calling /_security/oidc/prepare
     */
    class PrepareAuthResponse {
        private URI authUri;
        private String state;
        private String nonce;
        private String realm;

        PrepareAuthResponse(URI authUri, String state, String nonce, @Nullable String realm) {
            this.authUri = authUri;
            this.state = state;
            this.nonce = nonce;
            this.realm = realm;
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

        String getRealm() { return realm;}
    }
}
