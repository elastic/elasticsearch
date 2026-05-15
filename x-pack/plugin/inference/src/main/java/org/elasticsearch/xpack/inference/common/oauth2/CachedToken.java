/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import java.time.Duration;
import java.time.Instant;

/**
 * An OAuth2 bearer token paired with its server-reported expiry instant.
 * Held only in heap memory — never serialized, dropped on JVM restart.
 */
public record CachedToken(String bearer, Instant expiresAt) {

    /**
     * Returns true when the token is expiring within the given skew window,
     * i.e. it is not safe to use for a request that may take up to {@code skew} to dispatch.
     */
    public boolean isExpiringSoon(Instant now, Duration skew) {
        // if it expires at or before now+skew, treat it as expiring soon. This ensures we don't dispatch a token that's on the verge of
        // expiry.
        return expiresAt.isAfter(now.plus(skew)) == false;
    }
}
