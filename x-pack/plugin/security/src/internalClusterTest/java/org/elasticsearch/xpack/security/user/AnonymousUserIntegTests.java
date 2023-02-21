/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.user;

import org.apache.http.Header;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class AnonymousUserIntegTests extends SecurityIntegTestCase {
    private static boolean authorizationExceptionsEnabled;

    @BeforeClass
    public static void maybeEnableAnonymousAuthorizationException() {
        authorizationExceptionsEnabled = randomBoolean();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous")
            .put(AuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), authorizationExceptionsEnabled)
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Override
    public String configRoles() {
        return Strings.format("""
            %s
            anonymous:
              cluster: [ manage_token ]
              indices:
                - names: '*'
                  privileges: [ READ ]
              run_as: [ test_user ]
            """, super.configRoles());
    }

    public void testAnonymousViaHttp() throws Exception {
        try {
            getRestClient().performRequest(new Request("GET", "/_nodes"));
            fail("request should have failed");
        } catch (ResponseException e) {
            int statusCode = e.getResponse().getStatusLine().getStatusCode();
            Response response = e.getResponse();
            if (authorizationExceptionsEnabled) {
                assertThat(statusCode, is(403));
                assertThat(response.getHeader("WWW-Authenticate"), nullValue());
                assertThat(EntityUtils.toString(response.getEntity()), containsString("security_exception"));
            } else {
                assertThat(statusCode, is(401));
                final List<String> wwwAuthenticateHeaders = Arrays.stream(response.getHeaders())
                    .filter(header -> "WWW-Authenticate".equalsIgnoreCase(header.getName()))
                    .map(Header::getValue)
                    .toList();
                assertThat(wwwAuthenticateHeaders, hasItems(containsString("Basic"), containsString("ApiKey")));
                assertThat(EntityUtils.toString(response.getEntity()), containsString("security_exception"));
            }
        }
    }

    public void testAnonymousRoleShouldBeCaptureWhenCreatingApiKey() throws IOException {
        final CreateApiKeyResponse createApiKeyResponse = client().execute(
            CreateApiKeyAction.INSTANCE,
            new CreateApiKeyRequest(randomAlphaOfLength(8), null, null)
        ).actionGet();

        final Map<String, Object> apiKeyDocument = getApiKeyDocument(createApiKeyResponse.getId());

        @SuppressWarnings("unchecked")
        final Map<String, Object> limitedByRoleDescriptors = (Map<String, Object>) apiKeyDocument.get("limited_by_role_descriptors");
        assertThat(limitedByRoleDescriptors, hasKey("anonymous"));
    }

    public void testAnonymousRoleShouldNotBeCapturedWhenCreatingApiKeyWithServiceAccount() {
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest = new CreateServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            randomAlphaOfLength(8)
        );
        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse = client().execute(
            CreateServiceAccountTokenAction.INSTANCE,
            createServiceAccountTokenRequest
        ).actionGet();

        final CreateApiKeyResponse createApiKeyResponse = client().filterWithHeader(
            Map.of("Authorization", "Bearer " + createServiceAccountTokenResponse.getValue())
        ).execute(CreateApiKeyAction.INSTANCE, new CreateApiKeyRequest(randomAlphaOfLength(8), null, null)).actionGet();

        final Map<String, Object> apiKeyDocument = getApiKeyDocument(createApiKeyResponse.getId());

        @SuppressWarnings("unchecked")
        final Map<String, Object> limitedByRoleDescriptors = (Map<String, Object>) apiKeyDocument.get("limited_by_role_descriptors");
        assertThat(limitedByRoleDescriptors, not(hasKey("anonymous")));
    }

    public void testGrantApiKeyForAnonymousUserTokenWithRunAsWillFail() throws IOException {
        final TestSecurityClient.OAuth2Token oAuth2Token = getSecurityClient(RequestOptions.DEFAULT)
            .createTokenWithClientCredentialsGrant();
        assertThat(
            getSecurityClient(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + oAuth2Token.accessToken()).build())
                .authenticate()
                .get("username"),
            equalTo("_anonymous")
        );

        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        grantApiKeyRequest.getApiKeyRequest().setName("granted-api-key-cannot-have-anonymous-user-token-with-run-as");
        grantApiKeyRequest.getGrant().setType("access_token");
        grantApiKeyRequest.getGrant().setAccessToken(new SecureString(oAuth2Token.accessToken().toCharArray()));
        grantApiKeyRequest.getGrant().setRunAsUsername("test_user");
        final ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet()
        );
        assertThat(e.getMessage(), containsString("the provided grant credentials do not support run-as"));
    }

    private Map<String, Object> getApiKeyDocument(String apiKeyId) {
        final GetResponse getResponse = client().execute(GetAction.INSTANCE, new GetRequest(".security-7", apiKeyId)).actionGet();
        return getResponse.getSource();
    }
}
