/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.GlobalBalancingWeightsFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RolloverWaitForActiveShardsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, DelayReroutePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), DelayReroutePlugin.ALLOCATOR_NAME)
            .build();
    }

    private DelayReroutePlugin delayReroutePlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(DelayReroutePlugin.class).findFirst().orElseThrow();
    }

    public void testRolloverWithWaitForActiveShardsNoneSkipsRerouteWait() {
        DelayReroutePlugin plugin = delayReroutePlugin();
        try {
            final var indexPrefix = randomIndexName();
            final var indexName = indexPrefix + "-000001";
            final var aliasName = randomIdentifier("alias-");
            assertAcked(indicesAdmin().prepareCreate(indexName).addAlias(new Alias(aliasName).writeIndex(true)));
            ensureGreen(indexName);

            plugin.setDelayReroute(true);

            var rolloverFuture = indicesAdmin().prepareRolloverIndex(aliasName).waitForActiveShards(ActiveShardCount.NONE).execute();
            safeAwait(plugin.hasPendingRerouteLatch);

            final var response = safeGet(rolloverFuture);
            assertThat(response.getOldIndex(), equalTo(indexPrefix + "-000001"));
            assertThat(response.getNewIndex(), equalTo(indexPrefix + "-000002"));
            assertThat(response.isRolledOver(), is(true));
            assertThat(response.isAcknowledged(), is(true));
            assertThat(response.isShardsAcknowledged(), is(false));
        } finally {
            plugin.setDelayReroute(false);
        }
    }

    public void testRolloverWithWaitForActiveShardsWaitsForRerouteCompletion() {
        DelayReroutePlugin plugin = delayReroutePlugin();
        try {
            final var indexPrefix = randomIndexName();
            final var indexName = indexPrefix + "-000001";
            final var aliasName = randomIdentifier("alias-");
            assertAcked(
                indicesAdmin().prepareCreate(indexName)
                    .setSettings(indexSettings(1, 0).build())
                    .addAlias(new Alias(aliasName).writeIndex(true))
                    .get()
            );
            ensureGreen(indexName);

            plugin.setDelayReroute(true);

            final var activeShardCount = randomFrom(
                ActiveShardCount.ONE,
                ActiveShardCount.DEFAULT,
                ActiveShardCount.ALL,
                ActiveShardCount.from(1)
            );
            var rolloverFuture = indicesAdmin().prepareRolloverIndex(aliasName).waitForActiveShards(activeShardCount).execute();
            safeAwait(plugin.hasPendingRerouteLatch);
            assertFalse(rolloverFuture.isDone());

            plugin.setDelayReroute(false);
            safeExecute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT));

            final var response = safeGet(rolloverFuture);
            assertThat(response.getOldIndex(), equalTo(indexPrefix + "-000001"));
            assertThat(response.getNewIndex(), equalTo(indexPrefix + "-000002"));
            assertThat(response.isRolledOver(), is(true));
            assertThat(response.isAcknowledged(), is(true));
            assertThat(response.isShardsAcknowledged(), is(true));
        } finally {
            plugin.setDelayReroute(false);
        }
    }

    public static class DelayReroutePlugin extends Plugin implements ClusterPlugin {

        static final String ALLOCATOR_NAME = "delaying_reroute_test";

        private final AtomicReference<SubscribableListener<Void>> nextRerouteListenerRef = new AtomicReference<>(
            new SubscribableListener<>()
        );

        @Nullable // if not delaying reroutes
        volatile CountDownLatch hasPendingRerouteLatch = null;

        void setDelayReroute(boolean delayReroute) {
            this.hasPendingRerouteLatch = delayReroute ? new CountDownLatch(1) : null;
        }

        @Override
        public Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
            final var balancerSettings = new BalancerSettings(clusterSettings);

            return Map.of(ALLOCATOR_NAME, () -> new ShardsAllocator() {
                final ShardsAllocator delegate = new BalancedShardsAllocator(
                    balancerSettings,
                    WriteLoadForecaster.DEFAULT,
                    new GlobalBalancingWeightsFactory(balancerSettings)
                );

                @Override
                public void allocate(RoutingAllocation allocation) {
                    allocate(allocation, ActionListener.noop());
                }

                @Override
                public void allocate(RoutingAllocation allocation, ActionListener<Void> listener) {
                    final var nextRerouteListener = new SubscribableListener<Void>();
                    final var currentRerouteListener = nextRerouteListenerRef.getAndSet(nextRerouteListener);
                    nextRerouteListener.addListener(currentRerouteListener);
                    currentRerouteListener.addListener(listener);

                    final var hasPendingRerouteLatch = DelayReroutePlugin.this.hasPendingRerouteLatch;
                    if (hasPendingRerouteLatch == null) {
                        delegate.allocate(allocation, currentRerouteListener);
                    } else {
                        hasPendingRerouteLatch.countDown();
                    }
                }

                @Override
                public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                    return delegate.explainShardAllocation(shard, allocation);
                }
            });
        }
    }
}
