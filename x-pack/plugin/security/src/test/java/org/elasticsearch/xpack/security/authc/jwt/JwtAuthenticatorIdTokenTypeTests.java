/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.text.ParseException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwtAuthenticatorIdTokenTypeTests extends JwtAuthenticatorTests {

    @Override
    protected JwtRealmSettings.TokenType getTokenType() {
        return JwtRealmSettings.TokenType.ID_TOKEN;
    }

    public void testSubjectIsRequired() throws ParseException {
        final IllegalArgumentException e = doTestSubjectIsRequired(buildJwtAuthenticator());
        assertThat(e.getMessage(), containsString("missing required string claim [sub]"));
    }

    public void testInvalidIssuerIsCheckedBeforeAlgorithm() throws ParseException {
        doTestInvalidIssuerIsCheckedBeforeAlgorithm(buildJwtAuthenticator());
    }

    public void testAccessTokenHeaderTypeIsRejected() throws ParseException {
        final JWTClaimsSet claimsSet = JWTClaimsSet.parse(Map.of());
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", allowedAlgorithm, "typ", "at+jwt")).toBase64URL(),
            claimsSet.toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );

        final JwtAuthenticationToken jwtAuthenticationToken = mock(JwtAuthenticationToken.class);
        when(jwtAuthenticationToken.getSignedJWT()).thenReturn(signedJWT);
        when(jwtAuthenticationToken.getJWTClaimsSet()).thenReturn(signedJWT.getJWTClaimsSet());

        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        final JwtAuthenticator jwtAuthenticator = buildJwtAuthenticator();
        jwtAuthenticator.authenticate(jwtAuthenticationToken, future);
        final Exception e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("invalid jwt typ header"));
    }
}
