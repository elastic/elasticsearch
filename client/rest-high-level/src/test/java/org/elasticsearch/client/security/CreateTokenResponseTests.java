/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class CreateTokenResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String accessToken = randomAlphaOfLengthBetween(12, 24);
        final TimeValue expiresIn = TimeValue.timeValueSeconds(randomIntBetween(30, 10_000));
        final String refreshToken = randomBoolean() ? null : randomAlphaOfLengthBetween(12, 24);
        final String scope = randomBoolean() ? null : randomAlphaOfLength(4);
        final String type = randomAlphaOfLength(6);
        final String kerberosAuthenticationResponseToken = randomBoolean() ? null : randomAlphaOfLength(7);
        final AuthenticateResponse authentication = new AuthenticateResponse(new User(randomAlphaOfLength(7),
            Arrays.asList( randomAlphaOfLength(9) )),
            true, new AuthenticateResponse.RealmInfo(randomAlphaOfLength(5), randomAlphaOfLength(7) ),
            new AuthenticateResponse.RealmInfo(randomAlphaOfLength(5), randomAlphaOfLength(5) ), "realm");

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject()
            .field("access_token", accessToken)
            .field("type", type)
            .field("expires_in", expiresIn.seconds());
        if (refreshToken != null || randomBoolean()) {
            builder.field("refresh_token", refreshToken);
        }
        if (scope != null || randomBoolean()) {
            builder.field("scope", scope);
        }
        if (kerberosAuthenticationResponseToken != null) {
            builder.field("kerberos_authentication_response_token", kerberosAuthenticationResponseToken);
        }
        builder.field("authentication", authentication);
        builder.endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        final CreateTokenResponse response = CreateTokenResponse.fromXContent(createParser(xContentType.xContent(), xContent));
        assertThat(response.getAccessToken(), equalTo(accessToken));
        assertThat(response.getRefreshToken(), equalTo(refreshToken));
        assertThat(response.getScope(), equalTo(scope));
        assertThat(response.getType(), equalTo(type));
        assertThat(response.getExpiresIn(), equalTo(expiresIn));
        assertThat(response.getKerberosAuthenticationResponseToken(), equalTo(kerberosAuthenticationResponseToken));
        assertThat(response.getAuthentication(), equalTo(authentication));
    }
}
