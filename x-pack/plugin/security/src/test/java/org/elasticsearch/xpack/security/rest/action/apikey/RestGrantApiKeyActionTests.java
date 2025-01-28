/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;

import static org.hamcrest.Matchers.is;

public class RestGrantApiKeyActionTests extends ESTestCase {

    public void testParseXContentForGrantApiKeyRequest() throws Exception {
        final String grantType = randomAlphaOfLength(8);
        final String username = randomAlphaOfLength(8);
        final String password = randomAlphaOfLength(8);
        final String accessToken = randomAlphaOfLength(8);
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

}
