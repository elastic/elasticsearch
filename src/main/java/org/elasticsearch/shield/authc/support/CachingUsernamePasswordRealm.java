/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.UncheckedExecutionException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.shield.authc.RealmConfig;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class CachingUsernamePasswordRealm extends UsernamePasswordRealm {

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueMinutes(20);
    private static final int DEFAULT_MAX_USERS = 100000; //100k users
    public static final String CACHE_TTL = "cache.ttl";
    public static final String CACHE_MAX_USERS = "cache.max_users";

    private final Cache<String, UserWithHash> cache;
    private final Hasher hasher;

    protected CachingUsernamePasswordRealm(String type, RealmConfig config) {
        super(type, config);
        hasher = Hasher.resolve(config.settings().get("cache.hash_algo", null), Hasher.SHA2);
        TimeValue ttl = config.settings().getAsTime(CACHE_TTL, DEFAULT_TTL);
        if (ttl.millis() > 0) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(ttl.getMillis(), TimeUnit.MILLISECONDS)
                    .maximumSize(config.settings().getAsInt(CACHE_MAX_USERS, DEFAULT_MAX_USERS))
                    .build();
        } else {
            cache = null;
        }
    }

    public final void expire(String username) {
        if (cache != null) {
            cache.invalidate(username);
        }
    }

    public final void expireAll() {
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
                if (logger.isDebugEnabled()) {
                    logger.debug("User not found in cache, proceeding with normal authentication");
                }
                User user = doAuthenticate(token);
                if (user == null) {
                    throw new AuthenticationException("Could not authenticate [" + token.principal() + "]");
                }
                return new UserWithHash(user, token.credentials(), hasher);
            }
        };

        try {
            UserWithHash userWithHash = cache.get(token.principal(), callback);
            if (userWithHash.verify(token.credentials())) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Authenticated user [{}], with roles [{}]", token.principal(), userWithHash.user.roles());
                }
                return userWithHash.user;
            }
            //this handles when a user's password has changed:
            expire(token.principal());
            userWithHash = cache.get(token.principal(), callback);

            if (logger.isDebugEnabled()) {
                logger.debug("Cached user's password changed. Authenticated user [{}], with roles [{}]", token.principal(), userWithHash.user.roles());
            }
            return userWithHash.user;
            
        } catch (ExecutionException | UncheckedExecutionException ee) {
            if (logger.isTraceEnabled()) {
                logger.trace("Realm [" + type() + "] could not authenticate [" + token.principal() + "]", ee);
            } else if (logger.isDebugEnabled()) {
                logger.debug("Realm [" + type() + "] could not authenticate [" + token.principal() + "]");
            }
            return null;
        }
    }

    protected abstract User doAuthenticate(UsernamePasswordToken token);

    public static class UserWithHash {
        User user;
        char[] hash;
        Hasher hasher;
        public UserWithHash(User user, SecuredString password, Hasher hasher){
            this.user = user;
            this.hash = hasher.hash(password);
            this.hasher = hasher;
        }

        public boolean verify(SecuredString password){
            return hasher.verify(password, hash);
        }
    }
}
