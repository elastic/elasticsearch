/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class CachingUsernamePasswordRealm extends UsernamePasswordRealm implements CachingRealm {

    private final Cache<String, ListenableFuture<Tuple<AuthenticationResult, UserWithHash>>> cache;
    private final ThreadPool threadPool;
    final Hasher hasher;

    protected CachingUsernamePasswordRealm(String type, RealmConfig config, ThreadPool threadPool) {
        super(type, config);
        hasher = Hasher.resolve(CachingUsernamePasswordRealmSettings.CACHE_HASH_ALGO_SETTING.get(config.settings()), Hasher.SSHA256);
        this.threadPool = threadPool;
        TimeValue ttl = CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING.get(config.settings());
        if (ttl.getNanos() > 0) {
            cache = CacheBuilder.<String, ListenableFuture<Tuple<AuthenticationResult, UserWithHash>>>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(CachingUsernamePasswordRealmSettings.CACHE_MAX_USERS_SETTING.get(config.settings()))
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
     * @param authToken The authentication token
     * @param listener to be called at completion
     */
    @Override
    public final void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult> listener) {
        UsernamePasswordToken token = (UsernamePasswordToken) authToken;
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

    private void authenticateWithCache(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener) {
        try {
            final SetOnce<User> authenticatedUser = new SetOnce<>();
            final AtomicBoolean createdAndStartedFuture = new AtomicBoolean(false);
            final ListenableFuture<Tuple<AuthenticationResult, UserWithHash>> future = cache.computeIfAbsent(token.principal(), k -> {
                final ListenableFuture<Tuple<AuthenticationResult, UserWithHash>> created = new ListenableFuture<>();
                if (createdAndStartedFuture.compareAndSet(false, true) == false) {
                    throw new IllegalStateException("something else already started this. how?");
                }
                return created;
            });

            if (createdAndStartedFuture.get()) {
                doAuthenticate(token, ActionListener.wrap(result -> {
                    if (result.isAuthenticated()) {
                        final User user = result.getUser();
                        authenticatedUser.set(user);
                        final UserWithHash userWithHash = new UserWithHash(user, token.credentials(), hasher);
                        future.onResponse(new Tuple<>(result, userWithHash));
                    } else {
                        future.onResponse(new Tuple<>(result, null));
                    }
                }, future::onFailure));
            }

            future.addListener(ActionListener.wrap(tuple -> {
                if (tuple != null) {
                    final UserWithHash userWithHash = tuple.v2();
                    final boolean performedAuthentication = createdAndStartedFuture.get() && userWithHash != null &&
                        tuple.v2().user == authenticatedUser.get();
                    handleResult(future, createdAndStartedFuture.get(), performedAuthentication, token, tuple, listener);
                } else {
                    handleFailure(future, createdAndStartedFuture.get(), token, new IllegalStateException("unknown error authenticating"),
                        listener);
                }
            }, e -> handleFailure(future, createdAndStartedFuture.get(), token, e, listener)),
                threadPool.executor(ThreadPool.Names.GENERIC));
        } catch (ExecutionException e) {
            listener.onResponse(AuthenticationResult.unsuccessful("", e));
        }
    }

    private void handleResult(ListenableFuture<Tuple<AuthenticationResult, UserWithHash>> future, boolean createdAndStartedFuture,
                              boolean performedAuthentication, UsernamePasswordToken token,
                              Tuple<AuthenticationResult, UserWithHash> result, ActionListener<AuthenticationResult> listener) {
        final AuthenticationResult authResult = result.v1();
        if (authResult == null) {
            // this was from a lookup; clear and redo
            cache.invalidate(token.principal(), future);
            authenticateWithCache(token, listener);
        } else if (authResult.isAuthenticated()) {
            if (performedAuthentication) {
                listener.onResponse(authResult);
            } else {
                UserWithHash userWithHash = result.v2();
                if (userWithHash.verify(token.credentials())) {
                    if (userWithHash.user.enabled()) {
                        User user = userWithHash.user;
                        logger.debug("realm [{}] authenticated user [{}], with roles [{}]",
                            name(), token.principal(), user.roles());
                        listener.onResponse(AuthenticationResult.success(user));
                    } else {
                        // re-auth to see if user has been enabled
                        cache.invalidate(token.principal(), future);
                        authenticateWithCache(token, listener);
                    }
                } else {
                    // could be a password change?
                    cache.invalidate(token.principal(), future);
                    authenticateWithCache(token, listener);
                }
            }
        } else {
            cache.invalidate(token.principal(), future);
            if (createdAndStartedFuture) {
                listener.onResponse(authResult);
            } else {
                authenticateWithCache(token, listener);
            }
        }
    }

    private void handleFailure(ListenableFuture<Tuple<AuthenticationResult, UserWithHash>> future, boolean createdAndStarted,
                               UsernamePasswordToken token, Exception e, ActionListener<AuthenticationResult> listener) {
        cache.invalidate(token.principal(), future);
        if (createdAndStarted) {
            listener.onFailure(e);
        } else {
            authenticateWithCache(token, listener);
        }
    }

    @Override
    public Map<String, Object> usageStats() {
        Map<String, Object> stats = super.usageStats();
        stats.put("size", cache.count());
        return stats;
    }

    protected abstract void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener);

    @Override
    public final void lookupUser(String username, ActionListener<User> listener) {
        if (cache != null) {
            try {
                ListenableFuture<Tuple<AuthenticationResult, UserWithHash>> future = cache.computeIfAbsent(username, key -> {
                    ListenableFuture<Tuple<AuthenticationResult, UserWithHash>> created = new ListenableFuture<>();
                    doLookupUser(username, ActionListener.wrap(user -> {
                        if (user != null) {
                            UserWithHash userWithHash = new UserWithHash(user, null, null);
                            created.onResponse(new Tuple<>(null, userWithHash));
                        } else {
                            created.onResponse(new Tuple<>(null, null));
                        }
                    }, created::onFailure));
                    return created;
                });

                future.addListener(ActionListener.wrap(tuple -> {
                    if (tuple != null) {
                        if (tuple.v2() == null) {
                            cache.invalidate(username, future);
                            listener.onResponse(null);
                        } else {
                            listener.onResponse(tuple.v2().user);
                        }
                    } else {
                        listener.onResponse(null);
                    }
                }, listener::onFailure), threadPool.executor(ThreadPool.Names.GENERIC));
            } catch (ExecutionException e) {
                listener.onFailure(e);
            }
        } else {
            doLookupUser(username, listener);
        }
    }

    protected abstract void doLookupUser(String username, ActionListener<User> listener);

    private static class UserWithHash {
        final User user;
        final char[] hash;
        final Hasher hasher;

        UserWithHash(User user, SecureString password, Hasher hasher) {
            this.user = Objects.requireNonNull(user);
            this.hash = password == null ? null : hasher.hash(password);
            this.hasher = hasher;
        }

        boolean verify(SecureString password) {
            return hash != null && hasher.verify(password, hash);
        }
    }
}
