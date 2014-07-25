/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.AuthenticationException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A base class for username/password stores that caches the users and their pwd hashes in-memory. The cache
 * has an expiration time (defaults to 1hr, but it's configurable and can also be disabled by setting the cache
 * ttl to 0).
 */
public abstract class CachingUserPasswdStore extends AbstractComponent implements UserPasswdStore {

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueHours(1);

    private final LoadingCache<String, PasswordHash> cache;

    protected CachingUserPasswdStore(Settings settings) {
        super(settings);
        TimeValue ttl = componentSettings.getAsTime("cache.ttl", DEFAULT_TTL);
        if (ttl.millis() > 0) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(ttl.getMillis(), TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, PasswordHash>() {
                        @Override
                        public PasswordHash load(String username) throws Exception {
                            PasswordHash hash = passwordHash(username);
                            if (hash == null) {
                                throw new AuthenticationException("Authentication failed");
                            }
                            return hash;
                        }
                    });
        } else {
            cache = null;
        }
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
    public final boolean verifyPassword(final String username, final char[] password) {
        if (cache == null) {
            return doVerifyPassword(username, password);
        }

        try {

            PasswordHash hash = cache.get(username);
            return hash.verify(password);

        } catch (ExecutionException ee) {
            return false;
        }
    }

    /**
     * Verifies the given password. Both the given username, and if the username is verified, then the
     * given password. This method is used when the caching is disabled.
     */
    protected abstract boolean doVerifyPassword(String username, char[] password);

    protected abstract PasswordHash passwordHash(String username);


    public static abstract class Writable extends CachingUserPasswdStore implements UserPasswdStore.Writable {

        protected Writable(Settings settings) {
            super(settings);
        }

        @Override
        public final void store(String username, char[] key) {
            doStore(username, key);
            expire(username);
        }

        @Override
        public void remove(String username) {
            doRemove(username);
            expire(username);
        }

        protected abstract void doStore(String username, char[] password);

        protected abstract void doRemove(String username);
    }

    /**
     * Represents a hash of a password.
     */
    protected static interface PasswordHash {

        boolean verify(char[] password);

    }
}
