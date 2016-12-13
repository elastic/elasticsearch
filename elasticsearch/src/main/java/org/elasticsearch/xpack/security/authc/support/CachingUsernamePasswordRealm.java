/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.user.User;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public abstract class CachingUsernamePasswordRealm extends UsernamePasswordRealm implements CachingRealm {

    public static final Setting<String> CACHE_HASH_ALGO_SETTING = Setting.simpleString("cache.hash_algo", Setting.Property.NodeScope);

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueMinutes(20);
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting("cache.ttl", DEFAULT_TTL, Setting.Property.NodeScope);

    private static final int DEFAULT_MAX_USERS = 100_000; //100k users
    public static final Setting<Integer> CACHE_MAX_USERS_SETTING = Setting.intSetting("cache.max_users", DEFAULT_MAX_USERS,
            Setting.Property.NodeScope);

    private final Cache<String, UserWithHash> cache;
    final Hasher hasher;

    protected CachingUsernamePasswordRealm(String type, RealmConfig config) {
        super(type, config);
        hasher = Hasher.resolve(CACHE_HASH_ALGO_SETTING.get(config.settings()), Hasher.SSHA256);
        TimeValue ttl = CACHE_TTL_SETTING.get(config.settings());
        if (ttl.getNanos() > 0) {
            cache = CacheBuilder.<String, UserWithHash>builder()
                    .setExpireAfterAccess(ttl)
                    .setMaximumWeight(CACHE_MAX_USERS_SETTING.get(config.settings()))
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
     * @param authToken The authentication token
     */
    @Override
    public final void authenticate(AuthenticationToken authToken, ActionListener<User> listener) {
        UsernamePasswordToken token = (UsernamePasswordToken)authToken;
        try {
            if (cache == null) {
                doAuthenticate(token, listener);
            } else {
                authenticateWithCache(token, listener);
            }
        } catch (Exception e) {
            // each realm should handle exceptions, if we get one here it should be considered fatal
            listener.onFailure(e);
        }
    }

    private void authenticateWithCache(UsernamePasswordToken token, ActionListener<User> listener) {
        UserWithHash userWithHash = cache.get(token.principal());
        if (userWithHash == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("user not found in cache, proceeding with normal authentication");
            }
            doAuthenticateAndCache(token, ActionListener.wrap((user) -> {
                if (user != null) {
                    logger.debug("authenticated user [{}], with roles [{}]", token.principal(), user.roles());
                }
                listener.onResponse(user);
            }, listener::onFailure));
        } else if (userWithHash.hasHash()) {
            if (userWithHash.verify(token.credentials())) {
                logger.debug("authenticated user [{}], with roles [{}]", token.principal(), userWithHash.user.roles());
                listener.onResponse(userWithHash.user);
            } else {
                cache.invalidate(token.principal());
                doAuthenticateAndCache(token, ActionListener.wrap((user) -> {
                    if (user != null) {
                        logger.debug("cached user's password changed. authenticated user [{}], with roles [{}]", token.principal(),
                                user.roles());
                    }
                    listener.onResponse(user);
                }, listener::onFailure));
            }
        } else {
            cache.invalidate(token.principal());
            doAuthenticateAndCache(token, ActionListener.wrap((user) -> {
                if (user != null) {
                    logger.debug("cached user came from a lookup and could not be used for authentication. authenticated user [{}]" +
                            " with roles [{}]", token.principal(), user.roles());
                }
                listener.onResponse(user);
            }, listener::onFailure));
        }
    }

    private void doAuthenticateAndCache(UsernamePasswordToken token, ActionListener<User> listener) {
        doAuthenticate(token, ActionListener.wrap((user) -> {
            if (user == null) {
                listener.onResponse(null);
            } else {
                UserWithHash userWithHash = new UserWithHash(user, token.credentials(), hasher);
                // it doesn't matter if we already computed it elsewhere
                cache.put(token.principal(), userWithHash);
                listener.onResponse(user);
            }
        }, listener::onFailure));
    }

    @Override
    public Map<String, Object> usageStats() {
        Map<String, Object> stats = super.usageStats();
        stats.put("size", cache.count());
        return stats;
    }

    protected abstract void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener);

    @Override
    public final void lookupUser(String username, ActionListener<User> listener) {
        if (cache != null) {
            UserWithHash withHash = cache.get(username);
            if (withHash == null) {
                try {
                    doLookupUser(username, ActionListener.wrap((user) -> {
                        Runnable action = () -> listener.onResponse(null);
                        if (user != null) {
                            UserWithHash userWithHash = new UserWithHash(user, null, null);
                            try {
                                // computeIfAbsent is used here to avoid overwriting a value from a concurrent authenticate call as it
                                // contains the password hash, which provides a performance boost and we shouldn't just erase that
                                cache.computeIfAbsent(username, (n) -> userWithHash);
                                action = () -> listener.onResponse(userWithHash.user);
                            } catch (ExecutionException e) {
                                action = () -> listener.onFailure(e);
                            }
                        }
                        action.run();
                    }, listener::onFailure));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            } else {
                listener.onResponse(withHash.user);
            }
        } else {
            doLookupUser(username, listener);
        }
    }

    protected abstract void doLookupUser(String username, ActionListener<User> listener);

    /**
     * Returns the {@link Setting setting configuration} that is common for all caching realms
     */
    protected static Set<Setting<?>> getCachingSettings() {
        return new HashSet<>(Arrays.asList(CACHE_HASH_ALGO_SETTING, CACHE_TTL_SETTING, CACHE_MAX_USERS_SETTING));
    }

    private static class UserWithHash {
        User user;
        char[] hash;
        Hasher hasher;

        UserWithHash(User user, SecuredString password, Hasher hasher) {
            this.user = user;
            this.hash = password == null ? null : hasher.hash(password);
            this.hasher = hasher;
        }

        boolean verify(SecuredString password) {
            return hash != null && hasher.verify(password, hash);
        }

        boolean hasHash() {
            return hash != null;
        }
    }
}
