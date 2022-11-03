/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwtStringClaimValidatorTests extends ESTestCase {

    public void testClaimIsNotString() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(claimName, List.of(), randomBoolean());

        final SignedJWT jwt = prepareJwt(Map.of(claimName, List.of(42)));
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> validator.validate(jwt));
        assertThat(e.getMessage(), containsString("cannot parse string claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimIsNotSingleValued() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(3, 8);
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(claimName, List.of(), true);

        final SignedJWT jwt = prepareJwt(Map.of(claimName, List.of("foo", "bar")));
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> validator.validate(jwt));
        assertThat(e.getMessage(), containsString("cannot parse string claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimDoesNotExist() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(3, 8);
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(claimName, List.of(), randomBoolean());

        final SignedJWT jwt = prepareJwt(Map.of());
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> validator.validate(jwt));
        assertThat(e.getMessage(), containsString("missing string claim"));
    }

    public void testMatchingClaimValues() throws ParseException {
        final String claimName = randomFrom(randomAlphaOfLengthBetween(3, 8));
        final String claimValue = randomAlphaOfLength(10);
        final boolean singleValuedClaim = randomBoolean();
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(
            claimName,
            List.of(claimValue, randomAlphaOfLengthBetween(11, 20)),
            singleValuedClaim
        );

        final SignedJWT validJwt = prepareJwt(
            Map.of(claimName, singleValuedClaim ? claimValue : randomFrom(claimValue, List.of(claimValue, "other-stuff")))
        );
        validator.validate(validJwt);

        final SignedJWT invalidJwt = prepareJwt(Map.of(claimName, "not-" + claimValue));
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> validator.validate(invalidJwt));
        assertThat(e.getMessage(), containsString("does not match allowed claim values"));
    }

    private SignedJWT prepareJwt(Map<String, Object> m) throws ParseException {
        final SignedJWT jwt = mock(SignedJWT.class);
        final JWTClaimsSet jwtClaimsSet = JWTClaimsSet.parse(m);
        when(jwt.getJWTClaimsSet()).thenReturn(jwtClaimsSet);
        return jwt;
    }
}
