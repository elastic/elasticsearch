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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RunAsIntegTests extends SecurityIntegTestCase {

    private static final String RUN_AS_USER = "run_as_user";
    private static final String CLIENT_USER = "transport_user";
    private static final String NO_ROLE_USER = "no_role_user";
    private static final String ROLES = "run_as_role:\n"
        + "  cluster: ['manage_own_api_key', 'manage_token']\n"
        + "  run_as: [ '"
        + SecuritySettingsSource.TEST_USER_NAME
        + "', '"
        + NO_ROLE_USER
        + "', 'idontexist' ]\n"
        + "anonymous_role:\n"
        + "  cluster: ['manage_token']\n"
        + "  run_as: ['"
        + NO_ROLE_USER
        + "']\n";

    // indicates whether the RUN_AS_USER that is being authenticated is also a superuser
    private static boolean runAsHasSuperUserRole;

    @BeforeClass
    public static void configureRunAsHasSuperUserRole() {
        runAsHasSuperUserRole = randomBoolean();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    public String configRoles() {
        return ROLES + super.configRoles();
    }

    @Override
    public String configUsers() {
        return super.configUsers()
            + RUN_AS_USER
            + ":"
            + SecuritySettingsSource.TEST_PASSWORD_HASHED
            + "\n"
            + CLIENT_USER
            + ":"
            + SecuritySettingsSource.TEST_PASSWORD_HASHED
            + "\n"
            + NO_ROLE_USER
            + ":"
            + SecuritySettingsSource.TEST_PASSWORD_HASHED
            + "\n";
    }

    @Override
    public String configUsersRoles() {
        String roles = super.configUsersRoles() + "run_as_role:" + RUN_AS_USER + "\n" + "transport_client:" + CLIENT_USER;
        if (runAsHasSuperUserRole) {
            roles = roles + "\n" + "superuser:" + RUN_AS_USER;
        }
        return roles;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), "true")
            .putList(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_role");
        return builder.build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return false;
    }

    public void testUserImpersonationUsingHttp() throws Exception {
        // use the http user and try to run as
        try {
            Request request = new Request("GET", "/_nodes");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(CLIENT_USER, TEST_PASSWORD_SECURE_STRING));
            options.addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, SecuritySettingsSource.TEST_USER_NAME);
            request.setOptions(options);
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }

        if (runAsHasSuperUserRole == false) {
            try {
                // the run as user shouldn't have access to the nodes api
                Request request = new Request("GET", "/_nodes");
                RequestOptions.Builder options = request.getOptions().toBuilder();
                options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING));
                request.setOptions(options);
                getRestClient().performRequest(request);
                fail("request should have failed");
            } catch (ResponseException e) {
                assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
            }
        }

        // but when running as a different user it should work
        getRestClient().performRequest(requestForUserRunAsUser(SecuritySettingsSource.TEST_USER_NAME));
    }

    public void testEmptyHeaderUsingHttp() throws Exception {
        try {
            getRestClient().performRequest(requestForUserRunAsUser(""));
            fail("request should have failed");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
        }
    }

    public void testNonExistentRunAsUserUsingHttp() throws Exception {
        try {
            getRestClient().performRequest(requestForUserRunAsUser("idontexist"));
            fail("request should have failed");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    public void testRunAsUsingApiKey() throws IOException {
        final Request createApiKeyRequest = new Request("PUT", "/_security/api_key");
        createApiKeyRequest.setJsonEntity("{\"name\":\"k1\"}\n");
        createApiKeyRequest.setOptions(
            createApiKeyRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING))
        );
        final Response createApiKeyResponse = getRestClient().performRequest(createApiKeyRequest);
        final XContentTestUtils.JsonMapView apiKeyMapView = XContentTestUtils.createJsonMapView(
            createApiKeyResponse.getEntity().getContent()
        );

        // randomly using either the API key itself or a token created for it
        final boolean useOAuth2Token = randomBoolean();
        final String authHeader;
        if (useOAuth2Token) {
            final Request createTokenRequest = new Request("POST", "/_security/oauth2/token");
            createTokenRequest.setOptions(
                createTokenRequest.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + apiKeyMapView.get("encoded"))
            );
            createTokenRequest.setJsonEntity("{\"grant_type\":\"client_credentials\"}");
            final Response createTokenResponse = getRestClient().performRequest(createTokenRequest);
            final XContentTestUtils.JsonMapView createTokenJsonView = XContentTestUtils.createJsonMapView(
                createTokenResponse.getEntity().getContent()
            );
            authHeader = "Bearer " + createTokenJsonView.get("access_token");
        } else {
            authHeader = "ApiKey " + apiKeyMapView.get("encoded");
        }

        final boolean runAsTestUser = randomBoolean();

        final XContentTestUtils.JsonMapView authenticateJsonView = authenticateWithOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", authHeader)
                .addHeader(
                    AuthenticationServiceField.RUN_AS_USER_HEADER,
                    runAsTestUser ? SecuritySettingsSource.TEST_USER_NAME : NO_ROLE_USER
                )
        );
        assertThat(authenticateJsonView.get("username"), equalTo(runAsTestUser ? SecuritySettingsSource.TEST_USER_NAME : NO_ROLE_USER));
        assertThat(authenticateJsonView.get("authentication_realm.type"), equalTo("_es_api_key"));
        assertThat(authenticateJsonView.get("lookup_realm.type"), equalTo("file"));
        assertThat(authenticateJsonView.get("authentication_type"), equalTo(useOAuth2Token ? "token" : "api_key"));

        final Request getUserRequest = new Request("GET", "/_security/user");
        getUserRequest.setOptions(
            getUserRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", "ApiKey " + apiKeyMapView.get("encoded"))
                .addHeader(
                    AuthenticationServiceField.RUN_AS_USER_HEADER,
                    runAsTestUser ? SecuritySettingsSource.TEST_USER_NAME : NO_ROLE_USER
                )
        );
        if (runAsTestUser) {
            assertThat(getRestClient().performRequest(getUserRequest).getStatusLine().getStatusCode(), equalTo(200));
        } else {
            final ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(getUserRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        }

        // Run-As ignored if using the token is already created with run-as
        final Request createTokenRequest = new Request("POST", "/_security/oauth2/token");
        createTokenRequest.setOptions(
            createTokenRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", "ApiKey " + apiKeyMapView.get("encoded"))
                .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, SecuritySettingsSource.TEST_USER_NAME)
        );
        createTokenRequest.setJsonEntity("{\"grant_type\":\"client_credentials\"}");
        final Response createTokenResponse = getRestClient().performRequest(createTokenRequest);
        final XContentTestUtils.JsonMapView createTokenJsonView = XContentTestUtils.createJsonMapView(
            createTokenResponse.getEntity().getContent()
        );

        final XContentTestUtils.JsonMapView authenticateJsonView2 = authenticateWithOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", "Bearer " + createTokenJsonView.get("access_token"))
                .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, NO_ROLE_USER)
        );
        // run-as header is ignored, the user is still test_user
        assertThat(authenticateJsonView2.get("username"), equalTo(SecuritySettingsSource.TEST_USER_NAME));
        assertThat(authenticateJsonView2.get("authentication_type"), equalTo("token"));
    }

    public void testRunAsForOAuthToken() throws IOException {
        // Run-as works for oauth tokens
        final Request createTokenRequest = new Request("POST", "/_security/oauth2/token");
        createTokenRequest.setJsonEntity("{\"grant_type\":\"client_credentials\"}");
        createTokenRequest.setOptions(
            createTokenRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING))
        );
        final Response createTokenResponse = getRestClient().performRequest(createTokenRequest);
        final XContentTestUtils.JsonMapView tokenMapView = XContentTestUtils.createJsonMapView(
            createTokenResponse.getEntity().getContent()
        );

        final XContentTestUtils.JsonMapView authenticateJsonView = authenticateWithOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", "Bearer " + tokenMapView.get("access_token"))
                .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, NO_ROLE_USER)
        );
        assertThat(authenticateJsonView.get("username"), equalTo(NO_ROLE_USER));
        assertThat(authenticateJsonView.get("authentication_type"), equalTo("token"));

        // Run-as is ignored if the token itself already has run-as
        final Request createTokenRequest2 = new Request("POST", "/_security/oauth2/token");
        createTokenRequest2.setJsonEntity("{\"grant_type\":\"client_credentials\"}");
        createTokenRequest2.setOptions(
            createTokenRequest2.getOptions()
                .toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING))
                .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, SecuritySettingsSource.TEST_USER_NAME)
        );
        final Response createTokenResponse2 = getRestClient().performRequest(createTokenRequest2);
        final XContentTestUtils.JsonMapView tokenMapView2 = XContentTestUtils.createJsonMapView(
            createTokenResponse2.getEntity().getContent()
        );

        final XContentTestUtils.JsonMapView authenticateJsonView2 = authenticateWithOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", "Bearer " + tokenMapView2.get("access_token"))
                .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, NO_ROLE_USER)
        );
        assertThat(authenticateJsonView2.get("username"), equalTo(SecuritySettingsSource.TEST_USER_NAME));
        assertThat(authenticateJsonView2.get("authentication_type"), equalTo("token"));
    }

    public void testRunAsIsIgnoredForAnonymousUser() throws IOException {
        final RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder()
            .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, NO_ROLE_USER);

        // randomly use a token created for the anonymous user
        final boolean useOauth2Token = randomBoolean();
        if (useOauth2Token) {
            final Request createTokenRequest = new Request("POST", "/_security/oauth2/token");
            createTokenRequest.setJsonEntity("{\"grant_type\":\"client_credentials\"}");
            createTokenRequest.setOptions(createTokenRequest.getOptions().toBuilder());
            final Response createTokenResponse = getRestClient().performRequest(createTokenRequest);
            final XContentTestUtils.JsonMapView tokenMapView = XContentTestUtils.createJsonMapView(
                createTokenResponse.getEntity().getContent()
            );
            optionsBuilder.addHeader("Authorization", "Bearer " + tokenMapView.get("access_token"));
        }

        final XContentTestUtils.JsonMapView authenticateJsonView = authenticateWithOptions(optionsBuilder);
        assertThat(authenticateJsonView.get("username"), equalTo(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME));
        assertThat(authenticateJsonView.get("authentication_type"), equalTo(useOauth2Token ? "token" : "anonymous"));
    }

    private XContentTestUtils.JsonMapView authenticateWithOptions(RequestOptions.Builder optionsBuilder) throws IOException {
        final Request authenticateRequest = new Request("GET", "/_security/_authenticate");
        authenticateRequest.setOptions(optionsBuilder);
        final Response authenticateResponse = getRestClient().performRequest(authenticateRequest);
        return XContentTestUtils.createJsonMapView(authenticateResponse.getEntity().getContent());
    }

    private static Request requestForUserRunAsUser(String user) {
        Request request = new Request("GET", "/_nodes");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING));
        options.addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, user);
        request.setOptions(options);
        return request;
    }
}
