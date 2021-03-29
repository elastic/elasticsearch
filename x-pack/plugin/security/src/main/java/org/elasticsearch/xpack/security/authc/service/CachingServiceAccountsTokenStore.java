/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class CachingServiceAccountsTokenStore implements ServiceAccountsTokenStore, CacheInvalidatorRegistry.CacheInvalidator {

    private static final Logger logger = LogManager.getLogger(CachingServiceAccountsTokenStore.class);

    public static final Setting<String> CACHE_HASH_ALGO_SETTING = Setting.simpleString("xpack.security.authc.service_token.cache.hash_algo",
        "ssha256", Setting.Property.NodeScope);

    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting("xpack.security.authc.service_token.cache.ttl",
        TimeValue.timeValueMinutes(20), Setting.Property.NodeScope);
    public static final Setting<Integer> CACHE_MAX_TOKENS_SETTING = Setting.intSetting(
        "xpack.security.authc.service_token.cache.max_tokens", 100_000, Setting.Property.NodeScope);

    private final ThreadPool threadPool;
    private final Cache<String, ListenableFuture<CachedResult>> cache;
    private final Hasher hasher;

    CachingServiceAccountsTokenStore(Settings settings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        if (ttl.getNanos() > 0) {
            cache = CacheBuilder.<String, ListenableFuture<CachedResult>>builder()
                .setExpireAfterWrite(ttl)
                .setMaximumWeight(CACHE_MAX_TOKENS_SETTING.get(settings))
                .build();
        } else {
            cache = null;
        }
        hasher = Hasher.resolve(CACHE_HASH_ALGO_SETTING.get(settings));
    }

    @Override
    public void authenticate(ServiceAccountToken token, ActionListener<Boolean> listener) {
        try {
            if (cache == null) {
                doAuthenticate(token, listener);
            } else {
                authenticateWithCache(token, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void authenticateWithCache(ServiceAccountToken token, ActionListener<Boolean> listener) {
        assert cache != null;
        try {
            final AtomicBoolean valueAlreadyInCache = new AtomicBoolean(true);
            final ListenableFuture<CachedResult> listenableCacheEntry = cache.computeIfAbsent(token.getQualifiedName(), k -> {
                valueAlreadyInCache.set(false);
                return new ListenableFuture<>();
            });
            if (valueAlreadyInCache.get()) {
                listenableCacheEntry.addListener(ActionListener.wrap(result -> {
                    if (result.success) {
                        listener.onResponse(result.verify(token));
                    } else if (result.verify(token)) {
                        // same wrong token
                        listener.onResponse(false);
                    } else {
                        cache.invalidate(token.getQualifiedName(), listenableCacheEntry);
                        authenticateWithCache(token, listener);
                    }
                }, listener::onFailure), threadPool.generic(), threadPool.getThreadContext());
            } else {
                doAuthenticate(token, ActionListener.wrap(success -> {
                    logger.trace("cache service token [{}] authentication result", token.getQualifiedName());
                    listenableCacheEntry.onResponse(new CachedResult(hasher, success, token));
                    listener.onResponse(success);
                }, e -> {
                    // In case of failure, evict the cache entry and notify all listeners
                    cache.invalidate(token.getQualifiedName(), listenableCacheEntry);
                    listenableCacheEntry.onFailure(e);
                    listener.onFailure(e);
                }));
            }
        } catch (final ExecutionException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public final void invalidate(Collection<String> qualifiedTokenNames) {
        if (cache != null) {
            logger.trace("invalidating cache for service token [{}]",
                Strings.collectionToCommaDelimitedString(qualifiedTokenNames));
            qualifiedTokenNames.forEach(cache::invalidate);
        }
    }

    @Override
    public final void invalidateAll() {
        if (cache != null) {
            logger.trace("invalidating cache for all service tokens");
            cache.invalidateAll();
        }
    }

    protected ThreadPool getThreadPool() {
        return threadPool;
    }

    abstract void doAuthenticate(ServiceAccountToken token, ActionListener<Boolean> listener);

    // package private for testing
    Cache<String, ListenableFuture<CachedResult>> getCache() {
        return cache;
    }

    static class CachedResult {

        private final boolean success;
        private final char[] hash;

        private CachedResult(Hasher hasher, boolean success, ServiceAccountToken token) {
            this.success = success;
            this.hash = hasher.hash(token.getSecret());
        }

        private boolean verify(ServiceAccountToken token) {
            return hash != null && Hasher.verifyHash(token.getSecret(), hash);
        }
    }
}
