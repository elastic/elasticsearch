/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class FallbackClaimTests extends ESTestCase {

    public void testNoFallback() throws ParseException {
        final String name = randomAlphaOfLength(10);
        final String value = randomAlphaOfLength(10);
        final FallbackClaim fallbackClaim = new FallbackClaim(name, null, JWTClaimsSet.parse(Map.of(name, value)));
        assertThat(fallbackClaim.getActualName(), equalTo(name));
        assertThat(fallbackClaim.toString(), equalTo(name));
        assertThat(fallbackClaim.getStringClaimValue(), equalTo(value));
        assertThat(fallbackClaim.getStringListClaimValue(), equalTo(List.of(value)));
    }

    public void testFallback() throws ParseException {
        final String name = randomAlphaOfLength(10);
        final String fallbackName = randomAlphaOfLength(12);
        final String value = randomAlphaOfLength(10);

        // fallback ignored
        final JWTClaimsSet claimSet1 = JWTClaimsSet.parse(Map.of(name, value, fallbackName, randomAlphaOfLength(16)));
        final FallbackClaim fallbackClaim1 = new FallbackClaim(name, Map.of(name, fallbackName), claimSet1);
        assertThat(fallbackClaim1.getActualName(), equalTo(name));
        assertThat(fallbackClaim1.toString(), equalTo(name));
        assertThat(fallbackClaim1.getStringClaimValue(), equalTo(value));
        assertThat(fallbackClaim1.getStringListClaimValue(), equalTo(List.of(value)));

        // fallback active
        final JWTClaimsSet claimSet2 = JWTClaimsSet.parse(Map.of(fallbackName, value));
        final FallbackClaim fallbackClaim2 = new FallbackClaim(name, Map.of(name, fallbackName), claimSet2);
        assertThat(fallbackClaim2.getActualName(), equalTo(fallbackName));
        assertThat(fallbackClaim2.toString(), equalTo(fallbackName + " (fallback of " + name + ")"));
        assertThat(fallbackClaim2.getStringClaimValue(), equalTo(value));
        assertThat(fallbackClaim2.getStringListClaimValue(), equalTo(List.of(value)));
    }
}
