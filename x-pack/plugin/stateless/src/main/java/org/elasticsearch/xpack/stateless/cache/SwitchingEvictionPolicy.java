/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An {@link EvictionPolicy} that delegates to a swappable underlying policy, enabling the eviction
 * strategy to be changed at runtime without restarting the node. Only used on search nodes.
 */
class SwitchingEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    /**
     * Published briefly while swapping delegates so concurrent callers never observe a closed policy,
     * while the old policy's gauges can still be deregistered before the replacement registers.
     */
    private static final EvictionPolicy<FileCacheKey> NOOP_DELEGATE = new DefaultEvictionPolicy<>();

    private volatile EvictionPolicy<FileCacheKey> delegate;
    private final Releasable closeOnce;

    SwitchingEvictionPolicy(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        assert DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        final var clusterSettings = Objects.requireNonNull(clusterService).getClusterSettings();
        Objects.requireNonNull(meterRegistry);
        this.delegate = StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.get(settings)
            .create(clusterService, indicesService, threadPool, meterRegistry);
        final Releasable releasePolicyTypeUpdater = Releasables.releaseOnce(
            clusterSettings.addRemovableSettingsUpdateConsumer(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING,
                newEvictionPolicyType -> {
                    final var oldDelegate = this.delegate;
                    // Swap to a no-op first so concurrent createPredicate / updatePeriodicMetrics do not hit
                    // a closed policy; then close (deregister gauges) before the replacement re-registers.
                    this.delegate = NOOP_DELEGATE;
                    oldDelegate.close();
                    this.delegate = newEvictionPolicyType.create(clusterService, indicesService, threadPool, meterRegistry);
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
    public void updatePeriodicMetrics(Consumer<BiConsumer<CacheRegion<FileCacheKey>, Integer>> regions) {
        delegate.updatePeriodicMetrics(regions);
    }

    @Override
    public void close() {
        // Stop watching for policy-type changes, then close the current delegate
        closeOnce.close();
    }
}
