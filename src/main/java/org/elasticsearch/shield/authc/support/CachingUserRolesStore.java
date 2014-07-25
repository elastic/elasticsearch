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
 * A base class for user roles store that caches the roles per username in-memory. The cache
 * has an expiration time (defaults to 1hr, but it's configurable and can also be disabled by setting the cache
 * ttl to 0).
 */
public abstract class CachingUserRolesStore extends AbstractComponent implements UserRolesStore {

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueHours(1);

    private final LoadingCache<String, String[]> cache;

    protected CachingUserRolesStore(Settings settings) {
        super(settings);
        TimeValue ttl = componentSettings.getAsTime("cache.ttl", DEFAULT_TTL);
        if (ttl.millis() > 0) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(ttl.getMillis(), TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, String[]>() {
                        @Override
                        public String[] load(String username) throws Exception {
                            return doLoadRoles(username);
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
    public String[] roles(final String username) {
        if (cache == null) {
            return doLoadRoles(username);
        }
        try {
            return cache.get(username);
        } catch (ExecutionException ee) {
            throw new AuthenticationException("Could not load user roles", ee);
        }
    }

    protected abstract String[] doLoadRoles(String username);

    public static abstract class Writable extends CachingUserRolesStore implements UserRolesStore.Writable {

        protected Writable(Settings settings) {
            super(settings);
        }

        @Override
        public void setRoles(String username, String... roles) {
            doSetRoles(username, roles);
            expire(username);
        }

        @Override
        public void addRoles(String username, String... roles) {
            doAddRoles(username, roles);
            expire(username);
        }

        @Override
        public void removeRoles(String username, String... roles) {
            doRemoveRoles(username, roles);
            expire(username);
        }

        @Override
        public void removeUser(String username) {
            doRemoveUser(username);
            expire(username);
        }

        public abstract void doSetRoles(String username, String... roles);

        public abstract void doAddRoles(String username, String... roles);

        public abstract void doRemoveRoles(String username, String... roles);

        public abstract void doRemoveUser(String username);

    }

}
