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

    private static final String TEST_BEARER_VALUE = "bearer";

    public void testIsExpiringSoon_ReturnsFalseWhenExpiresAfterSkew() {
        var now = Instant.now();
        var skew = Duration.ofSeconds(60);
        // expires after now+skew so the token should be valid
        var token = new CachedToken(TEST_BEARER_VALUE, now.plus(skew).plusSeconds(1));
        assertFalse(token.isExpiringSoonHelper(now, skew));
    }

    public void testIsExpiringSoon_ReturnsTrueWhenExpiresBeforeSkew() {
        var now = Instant.now();
        var skew = Duration.ofSeconds(60);
        // expires before now+skew → too close, treat as expiring soon
        var token = new CachedToken(TEST_BEARER_VALUE, now.plus(skew).minusSeconds(1));
        assertTrue(token.isExpiringSoonHelper(now, skew));
    }

    public void testIsExpiringSoon_ReturnsTrueWhenExpiresExactlyAtSkewBoundary() {
        var now = Instant.now();
        var skew = Duration.ofSeconds(60);
        // expires exactly at now+skew → not strictly after, so expiring soon
        var token = new CachedToken(TEST_BEARER_VALUE, now.plus(skew));
        assertTrue(token.isExpiringSoonHelper(now, skew));
    }

    public void testIsExpiringSoon_ReturnsTrueWhenAlreadyExpired() {
        var now = Instant.now();
        var skew = Duration.ofSeconds(60);
        // already in the past
        var token = new CachedToken(TEST_BEARER_VALUE, now.minusSeconds(1));
        assertTrue(token.isExpiringSoonHelper(now, skew));
    }
}
