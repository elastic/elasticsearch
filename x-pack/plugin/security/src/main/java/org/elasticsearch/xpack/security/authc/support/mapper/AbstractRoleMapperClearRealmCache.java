/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * This is the base class for {@link UserRoleMapper} implementations that need to notify registered {@link CachingRealm}s,
 * when the role mapping rules change, to invalidate their caches that could rely on the obsolete role mapping rules.
 */
public abstract class AbstractRoleMapperClearRealmCache implements UserRoleMapper {

    private static final Logger logger = LogManager.getLogger(AbstractRoleMapperClearRealmCache.class);
    private final List<String> realmNamesToClearCaches = new CopyOnWriteArrayList<>();
    private final List<Runnable> localRealmCacheInvalidators = new CopyOnWriteArrayList<>();

    /**
     * Indicates that the provided realm should have its cache cleared if this store is updated.
     * @see ClearRealmCacheAction
     */
    @Override
    public void clearRealmCacheOnChange(CachingRealm realm) {
        realmNamesToClearCaches.add(realm.name());
        localRealmCacheInvalidators.add(realm::expireAll);
    }

    /**
     * {@link UserRoleMapper} implementations should be calling this method after role mappings changed,
     * in order to clear realm caches across the cluster.
     */
    protected void clearRealmCachesOnAllNodes(Client client, ActionListener<Void> listener) {
        if (realmNamesToClearCaches.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final String[] realmNames = this.realmNamesToClearCaches.toArray(Strings.EMPTY_ARRAY);
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            ClearRealmCacheAction.INSTANCE,
            new ClearRealmCacheRequest().realms(realmNames),
            ActionListener.wrap(response -> {
                logger.debug(() -> format("Cleared cached in realms [%s] due to role mapping change", Arrays.toString(realmNames)));
                listener.onResponse(null);
            }, ex -> {
                logger.warn(() -> "Failed to clear cache for realms [" + Arrays.toString(realmNames) + "]", ex);
                listener.onFailure(ex);
            })
        );
    }

    // public for testing
    /**
     * {@link UserRoleMapper} implementations should be calling this method after role mappings changed,
     * in order to clear realm caches on the local node only.
     */
    public void clearRealmCachesOnLocalNode() {
        localRealmCacheInvalidators.forEach(Runnable::run);
    }
}
