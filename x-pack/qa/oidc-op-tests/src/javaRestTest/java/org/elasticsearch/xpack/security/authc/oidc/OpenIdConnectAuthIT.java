/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class OpenIdConnectAuthIT extends C2IdOpTestCase {

    private static final String REALM_NAME = "c2id";
    private static final String REALM_NAME_IMPLICIT = "c2id-implicit";
    private static final String REALM_NAME_PROXY = "c2id-proxy";
    private static final String REALM_NAME_CLIENT_POST_AUTH = "c2id-post";
    private static final String REALM_NAME_CLIENT_JWT_AUTH = "c2id-jwt";
    private static final String FACILITATOR_PASSWORD = "f@cilit@t0rPassword"; // longer than 14 chars
    private static final String CLIENT_SECRET = "b07efb7a1cf6ec9462afe7b6d3ab55c6c7880262aa61ac28dded292aca47c9a2";

    @Before
    public void setupUserAndRoles() throws Exception {
        setFacilitatorUser();
        setRoleMappings();
    }

    /**
     * Register each of the clients that we want to test (C2id only supports dynamic configuration).
     */
    @BeforeClass
    public static void registerClients() throws Exception {
        String codeClient = Strings.format("""
            {
              "grant_types": [ "authorization_code" ],
              "response_types": [ "code" ],
              "preferred_client_id": "https://my.elasticsearch.org/rp",
              "preferred_client_secret": "%s",
              "redirect_uris": [ "https://my.fantastic.rp/cb" ],
              "token_endpoint_auth_method": "client_secret_basic"
            }""", CLIENT_SECRET);
        String implicitClient = Strings.format("""
            {
              "grant_types": [ "implicit" ],
              "response_types": [ "token id_token" ],
              "preferred_client_id": "elasticsearch-rp",
              "preferred_client_secret": "%s",
              "redirect_uris": [ "https://my.fantastic.rp/cb" ]
            }""", CLIENT_SECRET);
        String postClient = Strings.format("""
            {
              "grant_types": [ "authorization_code" ],
              "response_types": [ "code" ],
              "preferred_client_id": "elasticsearch-post",
              "preferred_client_secret": "%s",
              "redirect_uris": [ "https://my.fantastic.rp/cb" ],
              "token_endpoint_auth_method": "client_secret_post"
            }""", CLIENT_SECRET);
        String jwtClient = Strings.format("""
            {
              "grant_types": [ "authorization_code" ],
              "response_types": [ "code" ],
              "preferred_client_id": "elasticsearch-post-jwt",
              "preferred_client_secret": "%s",
              "redirect_uris": [ "https://my.fantastic.rp/cb" ],
              "token_endpoint_auth_method": "client_secret_jwt"
            }""", CLIENT_SECRET);
        registerClients(codeClient, implicitClient, postClient, jwtClient);
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
        final Map<String, Object> map = callAuthenticateApiUsingBearerToken(accessToken);
        logger.info("Authentication with token Response: " + map);
        assertThat(map.get("username"), equalTo(TEST_SUBJECT_ID));
        assertThat((List<?>) map.get("roles"), containsInAnyOrder("kibana_admin", "auditor"));

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertThat(metadata.get("oidc(sub)"), equalTo("alice"));
        assertThat(metadata.get("oidc(iss)"), equalTo(C2ID_ISSUER));
    }

    private void verifyElasticsearchAccessTokenForImplicitFlow(String accessToken) throws Exception {
        final Map<String, Object> map = callAuthenticateApiUsingBearerToken(accessToken);
        logger.info("Authentication with token Response: " + map);
        assertThat(map.get("username"), equalTo("alice"));
        assertThat((List<?>) map.get("roles"), containsInAnyOrder("limited_user", "auditor"));

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertThat(metadata.get("oidc(sub)"), equalTo("alice"));
        assertThat(metadata.get("oidc(iss)"), equalTo(C2ID_ISSUER));
    }

    private PrepareAuthResponse getRedirectedFromFacilitator(String realmName) throws Exception {
        final Map<String, String> body = Collections.singletonMap("realm", realmName);
        Request request = buildRequest("POST", "/_security/oidc/prepare?error_trace=true", body, facilitatorAuth());
        try (RestClient restClient = getElasticsearchClient()) {
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
        try (RestClient restClient = getElasticsearchClient()) {
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

    /**
     * We create a user named `facilitator` with the appropriate privileges ( `manage_oidc` ). A facilitator web app
     * would need to create one also, in order to access the OIDC related APIs on behalf of the user.
     */
    private void setFacilitatorUser() throws Exception {
        try (RestClient restClient = getElasticsearchClient()) {
            Request createRoleRequest = new Request("PUT", "/_security/role/facilitator");
            createRoleRequest.setJsonEntity("""
                { "cluster" : ["manage_oidc", "manage_token"] }""");
            restClient.performRequest(createRoleRequest);
            Request createUserRequest = new Request("PUT", "/_security/user/facilitator");
            createUserRequest.setJsonEntity(Strings.format("""
                { "password" : "%s", "roles" : ["facilitator"] }""", FACILITATOR_PASSWORD));
            restClient.performRequest(createUserRequest);
        }
    }

    private void setRoleMappings() throws Exception {
        try (RestClient restClient = getElasticsearchClient()) {
            Request createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_kibana");
            createRoleMappingRequest.setJsonEntity(Strings.format("""
                {
                  "roles": [ "kibana_admin" ],
                  "enabled": true,
                  "rules": {
                    "any": [
                      {
                        "field": {
                          "realm.name": "%s"
                        }
                      },
                      {
                        "field": {
                          "realm.name": "%s"
                        }
                      },
                      {
                        "field": {
                          "realm.name": "%s"
                        }
                      },
                      {
                        "field": {
                          "realm.name": "%s"
                        }
                      }
                    ]
                  }
                }""", REALM_NAME, REALM_NAME_PROXY, REALM_NAME_CLIENT_POST_AUTH, REALM_NAME_CLIENT_JWT_AUTH));
            restClient.performRequest(createRoleMappingRequest);

            createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_limited");
            createRoleMappingRequest.setJsonEntity(Strings.format("""
                {
                  "roles": [ "limited_user" ],
                  "enabled": true,
                  "rules": {
                    "field": {
                      "realm.name": "%s"
                    }
                  }
                }""", REALM_NAME_IMPLICIT));
            restClient.performRequest(createRoleMappingRequest);

            createRoleMappingRequest = new Request("PUT", "/_security/role_mapping/oidc_auditor");
            createRoleMappingRequest.setJsonEntity("""
                {
                  "roles": [ "auditor" ],
                  "enabled": true,
                  "rules": {
                    "field": {
                      "groups": "audit"
                    }
                  }
                }""");
            restClient.performRequest(createRoleMappingRequest);
        }
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
