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

public class FallbackableClaimTests extends ESTestCase {

    public void testNoFallback() throws ParseException {
        final String name = randomAlphaOfLength(10);
        final String value = randomAlphaOfLength(10);
        final FallbackableClaim fallbackableClaim = new FallbackableClaim(name, null, JWTClaimsSet.parse(Map.of(name, value)));
        assertThat(fallbackableClaim.getActualName(), equalTo(name));
        assertThat(fallbackableClaim.toString(), equalTo(name));
        assertThat(fallbackableClaim.getStringClaimValue(), equalTo(value));
        assertThat(fallbackableClaim.getStringListClaimValue(), equalTo(List.of(value)));
    }

    public void testFallback() throws ParseException {
        final String name = randomAlphaOfLength(10);
        final String fallbackName = randomAlphaOfLength(12);
        final String value = randomAlphaOfLength(10);

        // fallback ignored
        final JWTClaimsSet claimSet1 = JWTClaimsSet.parse(Map.of(name, value, fallbackName, randomAlphaOfLength(16)));
        final FallbackableClaim fallbackableClaim1 = new FallbackableClaim(name, Map.of(name, fallbackName), claimSet1);
        assertThat(fallbackableClaim1.getActualName(), equalTo(name));
        assertThat(fallbackableClaim1.toString(), equalTo(name));
        assertThat(fallbackableClaim1.getStringClaimValue(), equalTo(value));
        assertThat(fallbackableClaim1.getStringListClaimValue(), equalTo(List.of(value)));

        // fallback active
        final JWTClaimsSet claimSet2 = JWTClaimsSet.parse(Map.of(fallbackName, value));
        final FallbackableClaim fallbackableClaim2 = new FallbackableClaim(name, Map.of(name, fallbackName), claimSet2);
        assertThat(fallbackableClaim2.getActualName(), equalTo(fallbackName));
        assertThat(fallbackableClaim2.toString(), equalTo(fallbackName + " (fallback of " + name + ")"));
        assertThat(fallbackableClaim2.getStringClaimValue(), equalTo(value));
        assertThat(fallbackableClaim2.getStringListClaimValue(), equalTo(List.of(value)));
    }
}
