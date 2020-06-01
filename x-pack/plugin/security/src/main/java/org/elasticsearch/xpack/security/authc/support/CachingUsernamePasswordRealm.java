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

    private final Cache<String, ListenableFuture<CachedResult>> authenticationStallCache;
    private final Cache<String, ListenableFuture<CachedResult>> lookupCache;
    private final Cache<String, CachedResult> latestValidCredentialsCache;
    private final boolean cacheEnabled;
    private final ThreadPool threadPool;
    private final boolean authenticationEnabled;
    // package-private for tests
    final Hasher cacheHasher;

    protected CachingUsernamePasswordRealm(RealmConfig config, ThreadPool threadPool) {
        super(config);
        this.cacheHasher = Hasher.resolve(this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_HASH_ALGO_SETTING));
        this.threadPool = threadPool;
        final TimeValue ttl = this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING);
        this.cacheEnabled = ttl.getNanos() > 0;
        if (cacheEnabled) {
            authenticationStallCache = CacheBuilder.<String, ListenableFuture<CachedResult>>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_MAX_USERS_SETTING))
                    .build();
            lookupCache = CacheBuilder.<String, ListenableFuture<CachedResult>>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_MAX_USERS_SETTING))
                    .build();
            latestValidCredentialsCache = CacheBuilder.<String, CachedResult>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(this.config.getSetting(CachingUsernamePasswordRealmSettings.CACHE_MAX_USERS_SETTING))
                    .build();
        } else {
            authenticationStallCache = null;
            lookupCache = null;
            latestValidCredentialsCache = null;
        }
        this.authenticationEnabled = config.getSetting(CachingUsernamePasswordRealmSettings.AUTHC_ENABLED_SETTING);
    }

    @Override
    public final void expire(String username) {
        if (cacheEnabled) {
            assert authenticationStallCache != null;
            assert lookupCache != null;
            assert latestValidCredentialsCache != null;
            logger.trace("invalidating cache for user [{}] in realm [{}]", username, name());
            authenticationStallCache.invalidate(username);
            lookupCache.invalidate(username);
            latestValidCredentialsCache.invalidate(username);
        }
    }

    @Override
    public final void expireAll() {
        if (cacheEnabled) {
            assert authenticationStallCache != null;
            assert lookupCache != null;
            assert latestValidCredentialsCache != null;
            logger.trace("invalidating cache for all users in realm [{}]", name());
            authenticationStallCache.invalidateAll();
            lookupCache.invalidateAll();
            latestValidCredentialsCache.invalidateAll();
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
            if (false == cacheEnabled) {
                assert latestValidCredentialsCache == null;
                assert authenticationStallCache == null;
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
        assert authenticationStallCache != null;
        assert latestValidCredentialsCache != null;
        final CachedResult latestValidCredentials = latestValidCredentialsCache.get(token.principal());
        if (latestValidCredentials != null && latestValidCredentials.verify(token.credentials())) {
            // these credentials have been previously validated with the authentication source
            handleCachedAuthentication(latestValidCredentials.user, ActionListener.wrap(cacheResult -> {
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

        // the current credentials for the given principal must be validated against the authentication source
        final AtomicBoolean stallCurrentRequest = new AtomicBoolean(true);
        final ListenableFuture<CachedResult> listenableCacheEntry;
        try {
            listenableCacheEntry = authenticationStallCache.computeIfAbsent(token.principal(), k -> {
                // let through at most one concurrent request (to the authentication source) for a given principal
                stallCurrentRequest.set(false);
                return new ListenableFuture<>();
            });
        } catch (ExecutionException e) {
            listener.onFailure(e);
            return;
        }

        if (stallCurrentRequest.get()) {
            // there is already an inflight authenticate request for the given principal;
            // register as a listener for its response
            listenableCacheEntry.addListener(ActionListener.wrap(cachedResult -> {
                final boolean credsMatch = cachedResult.verify(token.credentials());
                if (cachedResult.authenticationResult.isAuthenticated() && credsMatch) {
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
                } else if (credsMatch) {
                    // the current credentials match the credentials of the request that reached the authentication source (for the
                    // same given principal) and they are both invalid
                    listener.onResponse(cachedResult.authenticationResult);
                } else {
                    // the credentials for the request that reached the authentication source MAY be valid but the current credentials
                    // differ; retry the current credentials against the authentication source
                    authenticateWithCache(token, listener);
                }
            }, listener::onFailure), threadPool.executor(ThreadPool.Names.GENERIC), threadPool.getThreadContext());
        } else {
            // attempt authentication against the authentication source
            doAuthenticate(token, ActionListener.wrap(authResult -> {
                final CachedResult cachedResult = new CachedResult(authResult, cacheHasher, authResult.getUser(), token.credentials());
                if (authResult.isAuthenticated() && authResult.getUser().enabled()) {
                    if (authResult.getUser().enabled()) {
                        // subsequent requests for the same credentials for this given principal will be honored from the
                        // {@code latestValidCredentialsCache} until the cache entry expires
                        latestValidCredentialsCache.put(token.principal(), cachedResult);
                    } else {
                        latestValidCredentialsCache.invalidate(token.principal());
                    }
                }
                // always invalidate the {@code authenticationStallCache} so that subsequent requests reach for the authentication source
                // (if they are not honored from the {@code latestValidCredentialsCache})
                authenticationStallCache.invalidate(token.principal(), listenableCacheEntry);
                // notify the stalled request listeners so they can reuse the current authentication response or retry reaching the
                // authentication source
                listenableCacheEntry.onResponse(cachedResult);
                listener.onResponse(authResult);
            }, e -> {
                // always invalidate the {@code authenticationStallCache}
                authenticationStallCache.invalidate(token.principal(), listenableCacheEntry);
                // notify the stalled listeners to propagate the current error
                listenableCacheEntry.onFailure(e);
                // notify the listener of the current authentication request
                listener.onFailure(e);
            }));
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
        // the {@code latestValidCredentialsCache} contains entries for successfully validated credentials, against which
        // incoming to-be-validated credentials are first checked
        // the {@code stalledCache} is only used momentarily to defer authentication requests for the same principal, hence its size is
        // not important (but it is still bounded)
        if (cacheEnabled) {
            assert lookupCache != null;
            assert latestValidCredentialsCache != null;
            return latestValidCredentialsCache.count() + lookupCache.count();
        } else {
            return -1;
        }
    }

    protected abstract void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener);

    @Override
    public final void lookupUser(String username, ActionListener<User> listener) {
        try {
            if (lookupCache == null) {
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
        assert lookupCache != null;
        assert latestValidCredentialsCache != null;
        final CachedResult latestValidCredentials = latestValidCredentialsCache.get(username);
        if (latestValidCredentials != null) {
            listener.onResponse(latestValidCredentials.user);
            return;
        }

        final AtomicBoolean lookupInCache = new AtomicBoolean(true);
        final ListenableFuture<CachedResult> listenableCacheEntry;
        try {
            listenableCacheEntry = lookupCache.computeIfAbsent(username, key -> {
                lookupInCache.set(false);
                return new ListenableFuture<>();
            });
        } catch (ExecutionException e) {
            listener.onFailure(e);
            return;
        }

        if (false == lookupInCache.get()) {
            // attempt lookup against the user directory
            doLookupUser(username, ActionListener.wrap(user -> {
                final CachedResult result = new CachedResult(AuthenticationResult.notHandled(), cacheHasher, user, null);
                if (user == null) {
                    // user not found, invalidate cache so that subsequent requests are forwarded to
                    // the user directory
                    lookupCache.invalidate(username, listenableCacheEntry);
                }
                // notify forestalled request listeners
                listenableCacheEntry.onResponse(result);
            }, e -> {
                // the next request should be forwarded, not halted by a failed lookup attempt
                lookupCache.invalidate(username, listenableCacheEntry);
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
