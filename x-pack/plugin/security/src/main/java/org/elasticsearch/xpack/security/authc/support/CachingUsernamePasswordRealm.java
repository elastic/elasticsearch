/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class CachingUsernamePasswordRealm extends UsernamePasswordRealm implements CachingRealm {

    private final Cache<String, ListenableFuture<UserWithHash>> cache;
    private final ThreadPool threadPool;
    final Hasher cacheHasher;

    protected CachingUsernamePasswordRealm(String type, RealmConfig config, ThreadPool threadPool) {
        super(type, config);
        cacheHasher = Hasher.resolve(CachingUsernamePasswordRealmSettings.CACHE_HASH_ALGO_SETTING.get(config.settings()));
        this.threadPool = threadPool;
        final TimeValue ttl = CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING.get(config.settings());
        if (ttl.getNanos() > 0) {
            cache = CacheBuilder.<String, ListenableFuture<UserWithHash>>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(CachingUsernamePasswordRealmSettings.CACHE_MAX_USERS_SETTING.get(config.settings()))
                    .build();
        } else {
            cache = null;
        }
    }

    @Override
    public final void expire(String username) {
        if (cache != null) {
            logger.trace("invalidating cache for user [{}] in realm [{}]", username, name());
            cache.invalidate(username);
        }
    }

    @Override
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
     * @param authToken The authentication token
     * @param listener to be called at completion
     */
    @Override
    public final void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult> listener) {
        final UsernamePasswordToken token = (UsernamePasswordToken) authToken;
        try {
            if (cache == null) {
                doAuthenticate(token, listener);
            } else {
                authenticateWithCache(token, listener);
            }
        } catch (final Exception e) {
            // each realm should handle exceptions, if we get one here it should be considered fatal
            listener.onFailure(e);
        }
    }

    /**
     * This validates the {@code token} while making sure there is only one inflight
     * request to the authentication source. Only successful responses are cached
     * and any subsequent requests, bearing the <b>same</b> password, will succeed
     * without reaching to the authentication source. A different password in a
     * subsequent request, however, will clear the cache and <b>try</b> to reach to
     * the authentication source.
     *
     * @param token The authentication token
     * @param listener to be called at completion
     */
    private void authenticateWithCache(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener) {
        try {
            final AtomicBoolean authenticationInCache = new AtomicBoolean(true);
            final ListenableFuture<UserWithHash> listenableCacheEntry = cache.computeIfAbsent(token.principal(), k -> {
                authenticationInCache.set(false);
                return new ListenableFuture<>();
            });
            if (authenticationInCache.get()) {
                // there is a cached or an inflight authenticate request
                listenableCacheEntry.addListener(ActionListener.wrap(authenticatedUserWithHash -> {
                    if (authenticatedUserWithHash != null && authenticatedUserWithHash.verify(token.credentials())) {
                        // cached credential hash matches the credential hash for this forestalled request
                        final User user = authenticatedUserWithHash.user;
                        logger.debug("realm [{}] authenticated user [{}], with roles [{}], from cache", name(), token.principal(),
                                user.roles());
                        listener.onResponse(AuthenticationResult.success(user));
                    } else {
                        // The inflight request has failed or its credential hash does not match the
                        // hash of the credential for this forestalled request.
                        // clear cache and try to reach the authentication source again because password
                        // might have changed there and the local cached hash got stale
                        cache.invalidate(token.principal(), listenableCacheEntry);
                        authenticateWithCache(token, listener);
                    }
                }, e -> {
                    // the inflight request failed, so try again, but first (always) make sure cache
                    // is cleared of the failed authentication
                    cache.invalidate(token.principal(), listenableCacheEntry);
                    authenticateWithCache(token, listener);
                }), threadPool.executor(ThreadPool.Names.GENERIC));
            } else {
                // attempt authentication against the authentication source
                doAuthenticate(token, ActionListener.wrap(authResult -> {
                    if (authResult.isAuthenticated() && authResult.getUser().enabled()) {
                        // compute the credential hash of this successful authentication request
                        final UserWithHash userWithHash = new UserWithHash(authResult.getUser(), token.credentials(), cacheHasher);
                        // notify any forestalled request listeners; they will not reach to the
                        // authentication request and instead will use this hash for comparison
                        listenableCacheEntry.onResponse(userWithHash);
                    } else {
                        // notify any forestalled request listeners; they will retry the request
                        listenableCacheEntry.onResponse(null);
                    }
                    // notify the listener of the inflight authentication request; this request is not retried
                    listener.onResponse(authResult);
                }, e -> {
                    // notify any staved off listeners; they will retry the request
                    listenableCacheEntry.onFailure(e);
                    // notify the listener of the inflight authentication request; this request is not retried
                    listener.onFailure(e);
                }));
            }
        } catch (final ExecutionException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("cache", Collections.singletonMap("size", getCacheSize()));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    protected int getCacheSize() {
        return cache.count();
    }

    protected abstract void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener);

    @Override
    public final void lookupUser(String username, ActionListener<User> listener) {
        try {
            if (cache == null) {
                doLookupUser(username, listener);
            } else {
                lookupWithCache(username, listener);
            }
        } catch (final Exception e) {
            // each realm should handle exceptions, if we get one here it should be
            // considered fatal
            listener.onFailure(e);
        }
    }

    private void lookupWithCache(String username, ActionListener<User> listener) {
        try {
            final AtomicBoolean lookupInCache = new AtomicBoolean(true);
            final ListenableFuture<UserWithHash> listenableCacheEntry = cache.computeIfAbsent(username, key -> {
                lookupInCache.set(false);
                return new ListenableFuture<>();
            });
            if (false == lookupInCache.get()) {
                // attempt lookup against the user directory
                doLookupUser(username, ActionListener.wrap(user -> {
                    if (user != null) {
                        // user found
                        final UserWithHash userWithHash = new UserWithHash(user, null, null);
                        // notify forestalled request listeners
                        listenableCacheEntry.onResponse(userWithHash);
                    } else {
                        // user not found, invalidate cache so that subsequent requests are forwarded to
                        // the user directory
                        cache.invalidate(username, listenableCacheEntry);
                        // notify forestalled request listeners
                        listenableCacheEntry.onResponse(null);
                    }
                }, e -> {
                    // the next request should be forwarded, not halted by a failed lookup attempt
                    cache.invalidate(username, listenableCacheEntry);
                    // notify forestalled listeners
                    listenableCacheEntry.onFailure(e);
                }));
            }
            listenableCacheEntry.addListener(ActionListener.wrap(userWithHash -> {
                if (userWithHash != null) {
                    listener.onResponse(userWithHash.user);
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure), threadPool.executor(ThreadPool.Names.GENERIC));
        } catch (final ExecutionException e) {
            listener.onFailure(e);
        }
    }

    protected abstract void doLookupUser(String username, ActionListener<User> listener);

    private static class UserWithHash {
        final User user;
        final char[] hash;

        UserWithHash(User user, SecureString password, Hasher hasher) {
            this.user = Objects.requireNonNull(user);
            this.hash = password == null ? null : hasher.hash(password);
        }

        boolean verify(SecureString password) {
            return hash != null && Hasher.verifyHash(password, hash);
        }
    }
}
