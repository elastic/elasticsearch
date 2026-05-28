/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;

public class CachedTokenTests extends ESTestCase {

    private static final String TEST_BEARER = "bearer";

    public void testIsExpiringSoon_ReturnsFalseWhenExpiresAfterSkew() {
        var now = Instant.now();
        var skew = Duration.ofSeconds(60);
        var token = new CachedToken(TEST_BEARER, now.plus(skew).plusSeconds(1));
        assertFalse(token.isExpiringSoon(now, skew));
    }

    public void testIsExpiringSoon_ReturnsTrueWhenExpiresBeforeSkew() {
        var now = Instant.now();
        var skew = Duration.ofSeconds(60);
        var token = new CachedToken(TEST_BEARER, now.plus(skew).minusSeconds(1));
        assertTrue(token.isExpiringSoon(now, skew));
    }

    public void testIsExpiringSoon_ReturnsTrueAtExactExpiry() {
        var now = Instant.now();
        var skew = Duration.ofSeconds(60);
        var token = new CachedToken(TEST_BEARER, now.plus(skew));
        assertTrue(token.isExpiringSoon(now, skew));
    }
}
