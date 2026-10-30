/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGrantApiKeyAction.RequestTranslator;

import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.action.Grant.USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class RestGrantApiKeyActionTests extends ESTestCase {

    public void testParseXContentForGrantApiKeyRequest() throws Exception {
        final String grantType = randomAlphaOfLength(8);
        final String username = randomAlphaOfLength(8);
        final String password = randomAlphaOfLength(8);
        final String accessToken = randomAlphaOfLength(8);
        final String serviceAccountToken = randomAlphaOfLength(8);
        final String clientAuthenticationScheme = randomAlphaOfLength(8);
        final String clientAuthenticationValue = randomAlphaOfLength(8);
        final String apiKeyName = randomAlphaOfLength(8);
        final var apiKeyExpiration = randomTimeValue();
        final String runAs = randomAlphaOfLength(8);
        try (
            XContentParser content = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("grant_type", grantType)
                    .field("username", username)
                    .field("password", password)
                    .field("access_token", accessToken)
                    .field("service_account_token", serviceAccountToken)
                    .startObject("client_authentication")
                    .field("scheme", clientAuthenticationScheme)
                    .field("value", clientAuthenticationValue)
                    .endObject()
                    .startObject("api_key")
                    .field("name", apiKeyName)
                    .field("expiration", apiKeyExpiration.getStringRep())
                    .endObject()
                    .field("run_as", runAs)
                    .endObject()
            )
        ) {
            GrantApiKeyRequest grantApiKeyRequest = RestGrantApiKeyAction.RequestTranslator.Default.fromXContent(content);
            assertThat(grantApiKeyRequest.getGrant().getType(), is(grantType));
            assertThat(grantApiKeyRequest.getGrant().getUsername(), is(username));
            assertThat(grantApiKeyRequest.getGrant().getPassword(), is(new SecureString(password.toCharArray())));
            assertThat(grantApiKeyRequest.getGrant().getAccessToken(), is(new SecureString(accessToken.toCharArray())));
            assertThat(grantApiKeyRequest.getGrant().getServiceAccountToken(), is(new SecureString(serviceAccountToken.toCharArray())));
            assertThat(grantApiKeyRequest.getGrant().getClientAuthentication().scheme(), is(clientAuthenticationScheme));
            assertThat(
                grantApiKeyRequest.getGrant().getClientAuthentication().value(),
                is(new SecureString(clientAuthenticationValue.toCharArray()))
            );
            assertThat(grantApiKeyRequest.getGrant().getRunAsUsername(), is(runAs));
            assertThat(grantApiKeyRequest.getApiKeyRequest().getName(), is(apiKeyName));
            assertThat(grantApiKeyRequest.getApiKeyRequest().getExpiration(), is(apiKeyExpiration));
        }
    }

    public void testUserManagedServiceAccountGrantIsRejectedForServerlessRequest() throws Exception {
        final AtomicReference<GrantApiKeyRequest> parsed = new AtomicReference<>();
        final RestGrantApiKeyAction action = action(request -> {
            final GrantApiKeyRequest grantRequest = new RequestTranslator.Default().translate(request);
            parsed.set(grantRequest);
            return grantRequest;
        });
        final FakeRestRequest restRequest = restRequest(
            "{\"grant_type\":\"" + USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE + "\",\"service_account_token\":\"secret-token\"}"
        );
        restRequest.markAsServerlessRequest();

        final ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> action.innerPrepareRequest(restRequest, null)
        );
        assertThat(
            e,
            throwableWithMessage(
                "grant_type [" + USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE + "] is not available when running in serverless mode"
            )
        );
        assertThat(e.status(), is(RestStatus.BAD_REQUEST));
        expectThrows(IllegalStateException.class, () -> parsed.get().getGrant().getServiceAccountToken().getChars());
    }

    public void testUserManagedServiceAccountGrantIsPreparedForStatefulRequest() throws Exception {
        final RestGrantApiKeyAction action = action(new RequestTranslator.Default());
        final FakeRestRequest restRequest = restRequest("{\"grant_type\":\"" + USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE + "\"}");

        assertThat(action.innerPrepareRequest(restRequest, null), notNullValue());
    }

    public void testPasswordGrantIsPreparedForServerlessRequest() throws Exception {
        final RestGrantApiKeyAction action = action(new RequestTranslator.Default());
        final FakeRestRequest restRequest = restRequest("""
            { "grant_type": "password" }""");
        restRequest.markAsServerlessRequest();

        assertThat(action.innerPrepareRequest(restRequest, null), notNullValue());
    }

    private static RestGrantApiKeyAction action(RequestTranslator translator) {
        return new RestGrantApiKeyAction(Settings.EMPTY, mock(XPackLicenseState.class), translator);
    }

    private static FakeRestRequest restRequest(String body) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
            .withPath("/_security/api_key/grant")
            .withContent(new BytesArray(body), XContentType.JSON)
            .build();
    }

}
