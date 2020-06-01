/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
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

    private final Cache<String, ListenableFuture<CachedResult>> stallCache;
    private final Cache<String, CachedResult> latestKnownGoodCredentialsCache;
    private final ThreadPool threadPool;
    private final boolean authenticationEnabled;
    final Hasher cacheHasher;

    protected CachingUsernamePasswordRealm(RealmConfig config, ThreadPool threadPool) {
        super(config);
        cacheHasher = Hasher.resolve(this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_HASH_ALGO_SETTING));
        this.threadPool = threadPool;
        final TimeValue ttl = this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING);
        if (ttl.getNanos() > 0) {
            stallCache = CacheBuilder.<String, ListenableFuture<CachedResult>>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_MAX_USERS_SETTING))
                    .build();
            latestKnownGoodCredentialsCache = CacheBuilder.<String, CachedResult>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_MAX_USERS_SETTING))
                    .build();
        } else {
            stallCache = null;
            latestKnownGoodCredentialsCache = null;
        }
        this.authenticationEnabled = config.getSetting(CachingUsernamePasswordRealmSettings.AUTHC_ENABLED_SETTING);
    }

    @Override
    public final void expire(String username) {
        if (stallCache != null) {
            assert latestKnownGoodCredentialsCache != null;
            logger.trace("invalidating cache for user [{}] in realm [{}]", username, name());
            stallCache.invalidate(username);
            latestKnownGoodCredentialsCache.invalidate(username);
        }
    }

    @Override
    public final void expireAll() {
        if (stallCache != null) {
            assert latestKnownGoodCredentialsCache != null;
            logger.trace("invalidating cache for all users in realm [{}]", name());
            stallCache.invalidateAll();
            latestKnownGoodCredentialsCache.invalidateAll();
        }
    }

    @Override
    public UsernamePasswordToken token(ThreadContext threadContext) {
        if (authenticationEnabled == false) {
            return null;
        }
        return super.token(threadContext);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return authenticationEnabled && super.supports(token);
    }

    /**
     * If the user exists in the cache (keyed by the principle name), then the password is validated
     * against a hash also stored in the cache.  Otherwise the subclass authenticates the user via
     * doAuthenticate.
     * This method will respond with {@link AuthenticationResult#notHandled()} if
     * {@link CachingUsernamePasswordRealmSettings#AUTHC_ENABLED_SETTING authentication is not enabled}.
     * @param authToken The authentication token
     * @param listener  to be called at completion
     */
    @Override
    public final void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult> listener) {
        if (authenticationEnabled == false) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        final UsernamePasswordToken token = (UsernamePasswordToken) authToken;
        try {
            if (stallCache == null) {
                assert latestKnownGoodCredentialsCache == null;
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
        assert stallCache != null;
        assert latestKnownGoodCredentialsCache != null;
        try {
            CachedResult latestKnownGoodCredentials = latestKnownGoodCredentialsCache.get(token.principal());
            if (latestKnownGoodCredentials != null && latestKnownGoodCredentials.verify(token.credentials())) {
                // these credentials have been previously validated with the authentication source
                handleCachedAuthentication(latestKnownGoodCredentials.user, ActionListener.wrap(cacheResult -> {
                    if (cacheResult.isAuthenticated()) {
                        logger.debug("realm [{}] authenticated user [{}], with roles [{}]",
                                name(), token.principal(), cacheResult.getUser().roles());
                    } else {
                        logger.debug("realm [{}] authenticated user [{}] from cache, but then failed [{}]",
                                name(), token.principal(), cacheResult.getMessage());
                    }
                    listener.onResponse(cacheResult);
                }, listener::onFailure));
                return;
            }
            final AtomicBoolean authenticationInCache = new AtomicBoolean(true);
            final ListenableFuture<CachedResult> listenableCacheEntry = stallCache.computeIfAbsent(token.principal(), k -> {
                // let through at most one concurrent request (to the authentication source) for a given principal
                authenticationInCache.set(false);
                return new ListenableFuture<>();
            });
            if (authenticationInCache.get()) {
                // there is already an inflight authenticate request for the given principal
                // register as a listener for its response
                // if the inflight request returned already, then reuse its result if credentials validated successfully
                // stop reusing if the current credentials are different
                listenableCacheEntry.addListener(ActionListener.wrap(cachedResult -> {
                    final boolean credsMatch = cachedResult.verify(token.credentials());
                    if (cachedResult.authenticationResult.isAuthenticated()) {
                        if (credsMatch) {
                            // the validated credentials match the credentials for the current request
                            handleCachedAuthentication(cachedResult.user, ActionListener.wrap(cacheResult -> {
                                if (cacheResult.isAuthenticated()) {
                                    logger.debug("realm [{}] authenticated user [{}], with roles [{}]",
                                        name(), token.principal(), cacheResult.getUser().roles());
                                } else {
                                    logger.debug("realm [{}] authenticated user [{}] from cache, but then failed [{}]",
                                        name(), token.principal(), cacheResult.getMessage());
                                }
                                listener.onResponse(cacheResult);
                            }, listener::onFailure));
                        } else {
                            // the validated credentials do NOT match the credentials for the current request
                            // the validated credentials MIGHT be stale, hence the credentials for the current request need to be
                            // validated against the authentication source
                            stallCache.invalidate(token.principal(), listenableCacheEntry);
                            authenticateWithCache(token, listener);
                        }
                    } else if (credsMatch) {
                        // the current credentials match the credentials of the request that reached the authentication source (for the
                        // same given principal) but they are invalid
                        listener.onResponse(cachedResult.authenticationResult);
                    } else {
                        // the credentials for the request that reached the authentication source are invalid
                        // retry the current request against the authentication source
                        stallCache.invalidate(token.principal(), listenableCacheEntry);
                        authenticateWithCache(token, listener);
                    }
                }, listener::onFailure), threadPool.executor(ThreadPool.Names.GENERIC), threadPool.getThreadContext());
            } else {
                // attempt authentication against the authentication source
                doAuthenticate(token, ActionListener.wrap(authResult -> {
                    CachedResult cachedResult = new CachedResult(authResult, cacheHasher, authResult.getUser(), token.credentials());
                    if (authResult.isAuthenticated() == false || authResult.getUser().enabled() == false) {
                        // do not cache failures, a subsequent request must reach for the authentication source
                        stallCache.invalidate(token.principal(), listenableCacheEntry);
                    } else {
                        latestKnownGoodCredentialsCache.put(token.principal(), cachedResult);
                    }
                    // notify the stalled request listeners; they will not reach to the
                    // authentication source but instead reuse the current result if their credentials match
                    listenableCacheEntry.onResponse(cachedResult);
                    listener.onResponse(authResult);
                }, e -> {
                    stallCache.invalidate(token.principal(), listenableCacheEntry);
                    // notify the stalled listeners to propagate the current error
                    listenableCacheEntry.onFailure(e);
                    // notify the listener of the inflight authentication request
                    listener.onFailure(e);
                }));
            }
        } catch (final ExecutionException e) {
            listener.onFailure(e);
        }
    }

    /**
     * {@code handleCachedAuthentication} is called when a {@link User} is retrieved from the cache.
     * The first {@code user} parameter is the user object that was found in the cache.
     * The default implementation returns a {@link AuthenticationResult#success(User) success result} with the
     * provided user, but sub-classes can return a different {@code User} object, or an unsuccessful result.
     */
    protected void handleCachedAuthentication(User user, ActionListener<AuthenticationResult> listener) {
        listener.onResponse(AuthenticationResult.success(user));
    }

    @Override
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("cache", Collections.singletonMap("size", getCacheSize()));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    protected int getCacheSize() {
        return stallCache == null ? -1 : stallCache.count();
    }

    protected abstract void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener);

    @Override
    public final void lookupUser(String username, ActionListener<User> listener) {
        try {
            if (stallCache == null) {
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
        assert stallCache != null;
        try {
            final AtomicBoolean lookupInCache = new AtomicBoolean(true);
            final ListenableFuture<CachedResult> listenableCacheEntry = stallCache.computeIfAbsent(username, key -> {
                lookupInCache.set(false);
                return new ListenableFuture<>();
            });
            if (false == lookupInCache.get()) {
                // attempt lookup against the user directory
                doLookupUser(username, ActionListener.wrap(user -> {
                    final CachedResult result = new CachedResult(AuthenticationResult.notHandled(), cacheHasher, user, null);
                    if (user == null) {
                        // user not found, invalidate cache so that subsequent requests are forwarded to
                        // the user directory
                        stallCache.invalidate(username, listenableCacheEntry);
                    }
                    // notify forestalled request listeners
                    listenableCacheEntry.onResponse(result);
                }, e -> {
                    // the next request should be forwarded, not halted by a failed lookup attempt
                    stallCache.invalidate(username, listenableCacheEntry);
                    // notify forestalled listeners
                    listenableCacheEntry.onFailure(e);
                }));
            }
            listenableCacheEntry.addListener(ActionListener.wrap(cachedResult -> {
                if (cachedResult.user != null) {
                    listener.onResponse(cachedResult.user);
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure), threadPool.executor(ThreadPool.Names.GENERIC), threadPool.getThreadContext());
        } catch (final ExecutionException e) {
            listener.onFailure(e);
        }
    }

    protected abstract void doLookupUser(String username, ActionListener<User> listener);

    private static class CachedResult {
        private final AuthenticationResult authenticationResult;
        private final User user;
        private final char[] hash;

        private CachedResult(AuthenticationResult result, Hasher hasher, @Nullable User user, @Nullable SecureString password) {
            this.authenticationResult = Objects.requireNonNull(result);
            if (authenticationResult.isAuthenticated() && user == null) {
                throw new IllegalArgumentException("authentication cannot be successful with a null user");
            }
            this.user = user;
            this.hash = password == null ? null : hasher.hash(password);
        }

        private boolean verify(SecureString password) {
            return hash != null && Hasher.verifyHash(password, hash);
        }
    }
}
