/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.security.test.SecurityMocks;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Because {@link TokenService} is {@code final}, we can't mock it.
 * Instead, we use this class to control the client that underlies the token service and trigger certain conditions
 */
public class TokenServiceMock {
    public final TokenService tokenService;
    public final Client client;

    public final class MockToken {
        public final String baseToken;
        public final SecureString encodedToken;
        public final String hashedToken;

        public MockToken(String baseToken, SecureString encodedToken, String hashedToken) {
            this.baseToken = baseToken;
            this.encodedToken = encodedToken;
            this.hashedToken = hashedToken;
        }
    }

    public TokenServiceMock(TokenService tokenService, Client client) {
        this.tokenService = tokenService;
        this.client = client;
    }

    public MockToken mockAccessToken() throws Exception {
        final String uuid = UUIDs.randomBase64UUID();
        final SecureString encoded = new SecureString(tokenService.prependVersionAndEncodeAccessToken(Version.CURRENT, uuid).toCharArray());
        final String hashedToken = TokenService.hashTokenString(uuid);
        return new MockToken(uuid, encoded, hashedToken);
    }

    public void defineToken(MockToken token, Authentication authentication) throws IOException {
        defineToken(token, authentication, true);
    }

    public void defineToken(MockToken token, Authentication authentication, boolean valid) throws IOException {
        Instant expiration = Instant.now().plusSeconds(TimeUnit.MINUTES.toSeconds(20));
        final UserToken userToken = new UserToken(token.hashedToken, Version.CURRENT, authentication, expiration, Map.of());
        final Map<String, Object> document = new HashMap<>();
        document.put("access_token", Map.of("user_token", userToken, "invalidated", valid == false));

        SecurityMocks.mockGetRequest(client, RestrictedIndicesNames.SECURITY_TOKENS_ALIAS, "token_" + token.hashedToken,
            XContentTestUtils.convertToXContent(document, XContentType.JSON));
    }
}
