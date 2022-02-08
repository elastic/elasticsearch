/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.User;

/**
 * Cache entry of successful authentications in a JwtRealm.
 * No assumption is made about the cache key, this class is only for the cached contents.
 */
public class JwtRealmCacheValue {
    private final AuthenticationResult<User> authenticationResultUser;
    private final char[] hash; // even if tokenPrinciple matches, detect if JWT+SharedSecret changed

    /**
     * JwtRealm uses this object to wrap a successful AuthenticationResult object.
     * The constructor takes a copy of the mandatory jwt and optional client secret, concatenates them, and hashes them.
     * The hash is only used to detect if successful authentication is happening for the original cached JWT, or a re-issued JWT.
     *
     * @param authenticationResultUser A successful authentication result to be cached.
     * @param jwt The mandatory end-user JWT which was authenticated.
     * @param clientSharedSecret The optional client shared secret which was authenticated.
     * @param hasher A hasher to use for hashing the JWT and client secret, for comparison during cache hits.
     */
    public JwtRealmCacheValue(
        final AuthenticationResult<User> authenticationResultUser,
        final SecureString jwt,
        final SecureString clientSharedSecret,
        final Hasher hasher
    ) {
        assert authenticationResultUser != null : "AuthenticationResult must be non-null";
        assert authenticationResultUser.isAuthenticated() : "AuthenticationResult.isAuthenticated must be true";
        assert authenticationResultUser.getValue() != null : "AuthenticationResult.getValue=User must be non-null";
        assert jwt != null : "Cache key must be non-null";
        assert hasher != null : "Hasher must be non-null";
        this.authenticationResultUser = authenticationResultUser;
        this.hash = this.hash(jwt, clientSharedSecret, hasher);
    }

    public char[] hash(final SecureString jwt, final @Nullable SecureString clientSecret, final Hasher hasher) {
        return hasher.hash(JwtUtil.join("/", jwt, clientSecret));
    }

    public boolean verifySameCredentials(final SecureString jwt, final @Nullable SecureString clientSecret) {
        return Hasher.verifyHash(JwtUtil.join("/", jwt, clientSecret), this.hash);
    }

    public AuthenticationResult<User> get() {
        return this.authenticationResultUser;
    }
}
