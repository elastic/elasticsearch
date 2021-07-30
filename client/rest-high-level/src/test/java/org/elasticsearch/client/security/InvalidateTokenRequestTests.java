/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class InvalidateTokenRequestTests extends ESTestCase {

    public void testInvalidateAccessToken() {
        String token = "Tf01rrAymdUjxMY4VlG3gV3gsFFUWxVVPrztX+4uhe0=";
        final InvalidateTokenRequest request = InvalidateTokenRequest.accessToken(token);
        assertThat(request.getAccessToken(), equalTo(token));
        assertThat(request.getRefreshToken(), nullValue());
        assertThat(Strings.toString(request), equalTo("{\"token\":\"Tf01rrAymdUjxMY4VlG3gV3gsFFUWxVVPrztX+4uhe0=\"}"));
    }

    public void testInvalidateRefreshToken() {
        String token = "4rE0YPT/oHODS83TbTtYmuh8";
        final InvalidateTokenRequest request = InvalidateTokenRequest.refreshToken(token);
        assertThat(request.getAccessToken(), nullValue());
        assertThat(request.getRefreshToken(), equalTo(token));
        assertThat(Strings.toString(request), equalTo("{\"refresh_token\":\"4rE0YPT/oHODS83TbTtYmuh8\"}"));
    }

    public void testInvalidateRealmTokens() {
        String realmName = "native";
        final InvalidateTokenRequest request = InvalidateTokenRequest.realmTokens(realmName);
        assertThat(request.getAccessToken(), nullValue());
        assertThat(request.getRefreshToken(), nullValue());
        assertThat(request.getRealmName(), equalTo(realmName));
        assertThat(request.getUsername(), nullValue());
        assertThat(Strings.toString(request), equalTo("{\"realm_name\":\"native\"}"));
    }

    public void testInvalidateUserTokens() {
        String username = "user";
        final InvalidateTokenRequest request = InvalidateTokenRequest.userTokens(username);
        assertThat(request.getAccessToken(), nullValue());
        assertThat(request.getRefreshToken(), nullValue());
        assertThat(request.getRealmName(), nullValue());
        assertThat(request.getUsername(), equalTo(username));
        assertThat(Strings.toString(request), equalTo("{\"username\":\"user\"}"));
    }

    public void testInvalidateUserTokensInRealm() {
        String username = "user";
        String realmName = "native";
        final InvalidateTokenRequest request = new InvalidateTokenRequest(null, null, realmName, username);
        assertThat(request.getAccessToken(), nullValue());
        assertThat(request.getRefreshToken(), nullValue());
        assertThat(request.getRealmName(), equalTo(realmName));
        assertThat(request.getUsername(), equalTo(username));
        assertThat(Strings.toString(request), equalTo("{\"realm_name\":\"native\",\"username\":\"user\"}"));
    }

    public void testEqualsAndHashCode() {
        final String token = randomAlphaOfLength(8);
        final boolean accessToken = randomBoolean();
        final InvalidateTokenRequest request = accessToken ? InvalidateTokenRequest.accessToken(token)
            : InvalidateTokenRequest.refreshToken(token);
        final EqualsHashCodeTestUtils.MutateFunction<InvalidateTokenRequest> mutate = r -> {
            int randomCase = randomIntBetween(1, 4);
            switch (randomCase) {
                case 1:
                    return InvalidateTokenRequest.refreshToken(randomAlphaOfLength(5));
                case 2:
                    return InvalidateTokenRequest.accessToken(randomAlphaOfLength(5));
                case 3:
                    return InvalidateTokenRequest.realmTokens(randomAlphaOfLength(5));
                case 4:
                    return InvalidateTokenRequest.userTokens(randomAlphaOfLength(5));
                default:
                    return new InvalidateTokenRequest(null, null, randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
        };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request,
            r -> new InvalidateTokenRequest(r.getAccessToken(), r.getRefreshToken()), mutate);
    }
}
