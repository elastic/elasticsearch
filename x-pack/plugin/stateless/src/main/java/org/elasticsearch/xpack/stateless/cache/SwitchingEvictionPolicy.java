/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * An {@link EvictionPolicy} that delegates to a swappable underlying policy, enabling the eviction
 * strategy to be changed at runtime without restarting the node. Only used on search nodes.
 */
class SwitchingEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    private volatile EvictionPolicy<FileCacheKey> delegate;
    private final Releasable closeOnce;

    SwitchingEvictionPolicy(Settings settings, ClusterService clusterService, IndicesService indicesService, TimeProvider timeProvider) {
        assert DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        final var clusterSettings = Objects.requireNonNull(clusterService).getClusterSettings();
        this.delegate = StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.get(settings)
            .create(clusterService, indicesService, timeProvider);
        final Releasable releasePolicyTypeUpdater = Releasables.releaseOnce(
            clusterSettings.addRemovableSettingsUpdateConsumer(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING,
                newEvictionPolicyType -> {
                    final var oldDelegate = this.delegate;
                    this.delegate = newEvictionPolicyType.create(clusterService, indicesService, timeProvider);
                    oldDelegate.close();
                }
            )
        );
        this.closeOnce = Releasables.releaseOnce(() -> Releasables.close(releasePolicyTypeUpdater, this.delegate));
    }

    // visible for testing
    EvictionPolicy<FileCacheKey> getDelegate() {
        return delegate;
    }

    @Override
    public Predicate<CacheRegion<FileCacheKey>> createPredicate(CacheRegion<FileCacheKey> incoming) {
        return delegate.createPredicate(incoming);
    }

    /**
     * The underlying policy can change when the eviction scan is in progress. Hence, it is possible that the eviction is
     * checked by the old delegate and onCached is called on the new delegate. This is intentional since the newly cached
     * region should be accounted for by the current policy regardless of how eviction itself is determined. The old policy
     * performs the necessary cleanup when its close method is called.
     */
    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {
        delegate.onCached(region);
    }

    /**
     * See comment on {@link #onCached(CacheRegion)} for swapping delegate policy during eviction scan.
     */
    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {
        delegate.onEvicted(region);
    }

    @Override
    public boolean isProtected(CacheRegion<FileCacheKey> region) {
        return delegate.isProtected(region);
    }

    @Override
    public void close() {
        // Stop watching for policy-type changes, then close the current delegate
        closeOnce.close();
    }
}
