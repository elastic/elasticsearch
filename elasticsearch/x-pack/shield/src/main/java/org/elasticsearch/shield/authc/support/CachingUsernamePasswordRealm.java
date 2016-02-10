/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.support.Exceptions;
import org.elasticsearch.shield.user.User;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class CachingUsernamePasswordRealm extends UsernamePasswordRealm implements CachingRealm {

    public static final String CACHE_HASH_ALGO_SETTING = "cache.hash_algo";
    public static final String CACHE_TTL_SETTING = "cache.ttl";
    public static final String CACHE_MAX_USERS_SETTING = "cache.max_users";

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueMinutes(20);
    private static final int DEFAULT_MAX_USERS = 100000; //100k users

    private final Cache<String, UserWithHash> cache;
    final Hasher hasher;

    protected CachingUsernamePasswordRealm(String type, RealmConfig config) {
        super(type, config);
        hasher = Hasher.resolve(config.settings().get(CACHE_HASH_ALGO_SETTING, null), Hasher.SSHA256);
        TimeValue ttl = config.settings().getAsTime(CACHE_TTL_SETTING, DEFAULT_TTL);
        if (ttl.millis() > 0) {
            cache = CacheBuilder.<String, UserWithHash>builder()
                    .setExpireAfterAccess(TimeUnit.MILLISECONDS.toNanos(ttl.getMillis()))
                    .setMaximumWeight(config.settings().getAsInt(CACHE_MAX_USERS_SETTING, DEFAULT_MAX_USERS))
                    .build();
        } else {
            cache = null;
        }
    }

    public final void expire(String username) {
        if (cache != null) {
            logger.trace("invalidating cache for user [{}] in realm [{}]", username, name());
            cache.invalidate(username);
        }
    }

    public final void expireAll() {
        if (cache != null) {
            logger.trace("invalidating cache for all users in realm [{}]", name());
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
    public final User authenticate(final UsernamePasswordToken token) {
        if (cache == null) {
            return doAuthenticate(token);
        }

        try {
            UserWithHash userWithHash = cache.get(token.principal());
            if (userWithHash == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("user not found in cache, proceeding with normal authentication");
                }
                User user = doAuthenticate(token);
                if (user == null) {
                    return null;
                }
                userWithHash = new UserWithHash(user, token.credentials(), hasher);
                // it doesn't matter if we already computed it elsewhere
                cache.put(token.principal(), userWithHash);
                if (logger.isDebugEnabled()) {
                    logger.debug("authenticated user [{}], with roles [{}]", token.principal(), user.roles());
                }
                return user;
            }

            final boolean hadHash = userWithHash.hasHash();
            if (hadHash) {
                if (userWithHash.verify(token.credentials())) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("authenticated user [{}], with roles [{}]", token.principal(), userWithHash.user.roles());
                    }
                    return userWithHash.user;
                }
            }
            //this handles when a user's password has changed or the user was looked up for run as and not authenticated
            cache.invalidate(token.principal());
            User user = doAuthenticate(token);
            if (user == null) {
                return null;
            }
            userWithHash = new UserWithHash(user, token.credentials(), hasher);
            // it doesn't matter if we already computed it elsewhere
            cache.put(token.principal(), userWithHash);
            if (logger.isDebugEnabled()) {
                if (hadHash) {
                    logger.debug("cached user's password changed. authenticated user [{}], with roles [{}]", token.principal(),
                            userWithHash.user.roles());
                } else {
                    logger.debug("cached user came from a lookup and could not be used for authentication. authenticated user [{}]" +
                            " with roles [{}]", token.principal(), userWithHash.user.roles());
                }
            }
            return userWithHash.user;

        } catch (Exception ee) {
            if (ee instanceof ElasticsearchSecurityException) {
                // this should bubble out
                throw ee;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("realm [{}] could not authenticate [{}]", ee, type(), token.principal());
            } else if (logger.isDebugEnabled()) {
                logger.debug("realm [{}] could not authenticate [{}]", type(), token.principal());
            }
            return null;
        }
    }

    @Override
    public final User lookupUser(final String username) {
        if (!userLookupSupported()) {
            return null;
        }

        CacheLoader<String, UserWithHash> callback = key -> {
            if (logger.isDebugEnabled()) {
                logger.debug("user not found in cache, proceeding with normal lookup");
            }
            User user = doLookupUser(username);
            if (user == null) {
                throw Exceptions.authenticationError("could not lookup [{}]", username);
            }
            return new UserWithHash(user, null, null);
        };

        try {
            UserWithHash userWithHash = cache.computeIfAbsent(username, callback);
            return userWithHash.user;
        } catch (ExecutionException ee) {
            if (logger.isTraceEnabled()) {
                logger.trace("realm [{}] could not lookup [{}]", ee, name(), username);
            } else if (logger.isDebugEnabled()) {
                logger.debug("realm [{}] could not authenticate [{}]", name(), username);
            }
            return null;
        }
    }

    protected abstract User doAuthenticate(UsernamePasswordToken token);

    protected abstract User doLookupUser(String username);

    private static class UserWithHash {
        User user;
        char[] hash;
        Hasher hasher;

        public UserWithHash(User user, SecuredString password, Hasher hasher) {
            this.user = user;
            this.hash = password == null ? null : hasher.hash(password);
            this.hasher = hasher;
        }

        public boolean verify(SecuredString password) {
            return hash != null && hasher.verify(password, hash);
        }

        public boolean hasHash() {
            return hash != null;
        }
    }
}
