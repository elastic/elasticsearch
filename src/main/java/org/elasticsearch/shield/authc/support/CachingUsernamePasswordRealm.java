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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class CachingUsernamePasswordRealm extends AbstractComponent implements Realm<UsernamePasswordToken> {

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueHours(1);
    private static final int DEFAULT_MAX_USERS = 100000; //100k users
    public static final String CACHE_TTL = "cache.ttl";
    public static final String CACHE_MAX_USERS = "cache.max_users";

    private final Cache<String, UserWithHash> cache;

    protected CachingUsernamePasswordRealm(Settings settings) {
        super(settings);
        TimeValue ttl = componentSettings.getAsTime(CACHE_TTL, DEFAULT_TTL);
        if (ttl.millis() > 0) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(ttl.getMillis(), TimeUnit.MILLISECONDS)
                    .maximumSize(settings.getAsInt(CACHE_MAX_USERS, DEFAULT_MAX_USERS))
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

    /**
     * If the user exists in the cache (keyed by the principle name), then the password is validated
     * against a hash also stored in the cache.  Otherwise the subclass authenticates the user via
     * doAuthenticate
     *
     * @param token The authentication token
     * @return an authenticated user with roles
     */
    @Override
    public User authenticate(final UsernamePasswordToken token) {
        if (cache == null) {
            return doAuthenticate(token);
        }

        Callable<UserWithHash> callback = new Callable<UserWithHash>() {
            @Override
            public UserWithHash call() throws Exception {
                User user = doAuthenticate(token);
                if (user == null) {
                    throw new AuthenticationException("Could not authenticate ['" + token.principal() + "]");
                }
                return new UserWithHash(user, token.credentials());
            }
        };

        try {
            UserWithHash userWithHash = cache.get(token.principal(), callback);
            if (userWithHash.verify(token.credentials())) {
                return userWithHash.user;
            }
            //this handles when a user's password has changed:
            expire(token.principal());
            userWithHash = cache.get(token.principal(), callback);
            return userWithHash.user;
            
        } catch (ExecutionException ee) {
            logger.warn("Could not authenticate ['" + token.principal() + "]", ee);
            return null;
        }
    }

    protected abstract User doAuthenticate(UsernamePasswordToken token);

    public static class UserWithHash {
        User user;
        char[] hash;
        public UserWithHash(User user, char[] password){
            this.user = user;
            this.hash = Hasher.HTPASSWD.hash(password);
        }

        public boolean verify(char[] password){
            return Hasher.HTPASSWD.verify(password, hash);
        }
    }
}
