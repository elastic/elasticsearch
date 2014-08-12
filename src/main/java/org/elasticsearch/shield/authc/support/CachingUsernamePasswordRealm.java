/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.transport.TransportMessage;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class CachingUsernamePasswordRealm extends AbstractComponent implements Realm<UsernamePasswordToken> {

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueHours(1);

    private final Cache<CacheKey, User> cache;

    protected CachingUsernamePasswordRealm(Settings settings) {
        super(settings);
        TimeValue ttl = componentSettings.getAsTime("cache.ttl", DEFAULT_TTL);
        if (ttl.millis() > 0) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(ttl.getMillis(), TimeUnit.MILLISECONDS)
                    .maximumSize(settings.getAsInt("cache.max_users", -1))
                    .build();
        } else {
            cache = null;
        }
    }

    @Override
    public UsernamePasswordToken token(TransportMessage<?> message) {
        return UsernamePasswordToken.extractToken(message, null);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof UsernamePasswordToken;
    }

    protected final void expire(String username) {
        if (cache != null) {
            cache.invalidate(username);
        }
    }

    protected final void expireAll() {
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    @Override
    public User authenticate(final UsernamePasswordToken token) {
        if (cache == null) {
            return doAuthenticate(token);
        }

        try {
            return cache.get(new CacheKey(token), new Callable<User>() {
                @Override
                public User call() throws Exception {
                    return doAuthenticate(token);
                }
            });
        } catch (ExecutionException ee) {
            throw new AuthenticationException("Could not authenticate ['" + token.principal() + "]", ee);
        }
    }

    protected abstract User doAuthenticate(UsernamePasswordToken token);

    static class CacheKey {

        private final String username;
        private final char[] passwdHash;

        CacheKey(UsernamePasswordToken token) {
            this.username = token.principal();
            this.passwdHash = Hasher.HTPASSWD.hash(token.credentials());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (!Arrays.equals(passwdHash, cacheKey.passwdHash)) return false;
            if (!username.equals(cacheKey.username)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = username.hashCode();
            result = 31 * result + Arrays.hashCode(passwdHash);
            return result;
        }
    }
}
