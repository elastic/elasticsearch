/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;

import static org.hamcrest.Matchers.is;

public class RestActivateProfileActionTests extends ESTestCase {

    public void testParseXContentForGrantApiKeyRequest() throws Exception {
        final String grantType = randomAlphaOfLength(8);
        final String username = randomAlphaOfLength(8);
        final String password = randomAlphaOfLength(8);
        final String accessToken = randomAlphaOfLength(8);
        final String clientAuthenticationScheme = randomAlphaOfLength(8);
        final String clientAuthenticationValue = randomAlphaOfLength(8);
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
                    .endObject()
            )
        ) {
            ActivateProfileRequest activateProfileRequest = RestActivateProfileAction.fromXContent(content);
            assertThat(activateProfileRequest.getGrant().getType(), is(grantType));
            assertThat(activateProfileRequest.getGrant().getUsername(), is(username));
            assertThat(activateProfileRequest.getGrant().getPassword(), is(new SecureString(password.toCharArray())));
            assertThat(activateProfileRequest.getGrant().getAccessToken(), is(new SecureString(accessToken.toCharArray())));
            assertThat(activateProfileRequest.getGrant().getClientAuthentication().scheme(), is(clientAuthenticationScheme));
            assertThat(
                activateProfileRequest.getGrant().getClientAuthentication().value(),
                is(new SecureString(clientAuthenticationValue.toCharArray()))
            );
        }
    }
}
