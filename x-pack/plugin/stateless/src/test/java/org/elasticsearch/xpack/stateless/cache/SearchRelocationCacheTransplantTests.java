/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SearchRelocationCacheTransplantTests extends ESTestCase {

    public void testLiveSettingsStopIssuingRegionRequests() {
        ClusterSettings clusterSettings = newClusterSettings();
        FakeClient client = new FakeClient(5);
        SearchRelocationCacheTransplant transplant = newTransplant(clusterSettings, client);
        // applySettings replaces the dynamic settings, so each update carries the full set.
        Settings.Builder desired = Settings.builder().put(SearchRelocationCacheTransplant.ENABLED_SETTING.getKey(), true);
        clusterSettings.applySettings(desired.build());
        assertTrue(transplant.isEnabled());
        assertThat(transplant.maxRegionsInFlight(), equalTo(1));

        AtomicBoolean finished = new AtomicBoolean();
        transplant.run(
            shardId(),
            "source",
            () -> false,
            ActionListener.wrap(ignored -> finished.set(true), e -> { throw new AssertionError(e); })
        );
        assertThat(client.regionCalls.get(), equalTo(1));
        assertThat(client.lastBatchSize, equalTo(1));
        assertFalse(finished.get());

        desired.put(SearchRelocationCacheTransplant.MAX_REGIONS_IN_FLIGHT_SETTING.getKey(), 3);
        clusterSettings.applySettings(desired.build());
        assertThat(transplant.maxRegionsInFlight(), equalTo(3));
        // The new limit is the size of the next message. Four regions remain, so the next message carries three.
        client.completeOne();
        assertThat(client.regionCalls.get(), equalTo(2));
        assertThat(client.lastBatchSize, equalTo(3));
        assertThat(client.pending.size(), equalTo(1));

        desired.put(SearchRelocationCacheTransplant.ENABLED_SETTING.getKey(), false);
        clusterSettings.applySettings(desired.build());
        int callsAtDisable = client.regionCalls.get();
        client.completeOne();
        assertThat(client.regionCalls.get(), equalTo(callsAtDisable));
        client.completeAll();
        assertThat(client.regionCalls.get(), equalTo(callsAtDisable));
        assertTrue(finished.get());

        desired.put(SearchRelocationCacheTransplant.ORDERING_SETTING.getKey(), "serial");
        clusterSettings.applySettings(desired.build());
        assertThat(transplant.ordering(), equalTo(SearchRelocationCacheTransplant.Ordering.SERIAL));

        desired.put(
            SearchRelocationCacheTransplant.MAX_REGIONS_IN_FLIGHT_SETTING.getKey(),
            TransportGetCacheTransplantRegionAction.MAX_REGIONS_PER_REQUEST + 1
        );
        expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(desired.build()));
    }

    public void testParallelStartsObjectStoreWarmingImmediately() {
        SearchRelocationCacheTransplant.objectStoreWarmingStarted.set(0);
        AtomicBoolean transplantStarted = new AtomicBoolean();
        AtomicBoolean warmStarted = new AtomicBoolean();
        SearchRelocationCacheTransplant.coordinate(
            shardId(),
            SearchRelocationCacheTransplant.Ordering.PARALLEL,
            () -> false,
            (stopped, listener) -> {
                transplantStarted.set(true);
                assertFalse(warmStarted.get());
            },
            listener -> {
                warmStarted.set(true);
                assertTrue(transplantStarted.get());
            },
            ActionListener.noop()
        );
        assertTrue(transplantStarted.get());
        assertTrue(warmStarted.get());
        assertThat(SearchRelocationCacheTransplant.objectStoreWarmingStarted.get(), equalTo(1));
    }

    public void testSerialStartsObjectStoreWarmingOnlyAfterTransplant() {
        SearchRelocationCacheTransplant.objectStoreWarmingStarted.set(0);
        AtomicBoolean warmStarted = new AtomicBoolean();
        List<ActionListener<Void>> transplantDone = new ArrayList<>();
        SearchRelocationCacheTransplant.coordinate(
            shardId(),
            SearchRelocationCacheTransplant.Ordering.SERIAL,
            () -> false,
            (stopped, listener) -> transplantDone.add(listener),
            listener -> warmStarted.set(true),
            ActionListener.noop()
        );
        assertFalse(warmStarted.get());
        assertThat(transplantDone.size(), equalTo(1));
        transplantDone.get(0).onResponse(null);
        assertTrue(warmStarted.get());
        assertThat(SearchRelocationCacheTransplant.objectStoreWarmingStarted.get(), greaterThan(0));
    }

    public void testSerialSkipsObjectStoreWarmingWhenRecoveryWaitFinished() {
        SearchRelocationCacheTransplant.objectStoreWarmingStarted.set(0);
        AtomicInteger warmStarts = new AtomicInteger();
        List<ActionListener<Void>> transplantDone = new ArrayList<>();
        BooleanSupplier finished = () -> true;
        SearchRelocationCacheTransplant.coordinate(
            shardId(),
            SearchRelocationCacheTransplant.Ordering.SERIAL,
            finished,
            (stopped, listener) -> transplantDone.add(listener),
            listener -> warmStarts.incrementAndGet(),
            ActionListener.noop()
        );
        transplantDone.get(0).onResponse(null);
        assertThat(warmStarts.get(), equalTo(0));
        assertThat(SearchRelocationCacheTransplant.objectStoreWarmingStarted.get(), equalTo(0));
    }

    private static SearchRelocationCacheTransplant newTransplant(ClusterSettings clusterSettings, FakeClient client) {
        return new SearchRelocationCacheTransplant(
            client,
            nodeId -> DiscoveryNodeUtils.create(nodeId),
            null,
            Runnable::run,
            clusterSettings
        );
    }

    private static ClusterSettings newClusterSettings() {
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.add(SearchRelocationCacheTransplant.ENABLED_SETTING);
        settings.add(SearchRelocationCacheTransplant.ORDERING_SETTING);
        settings.add(SearchRelocationCacheTransplant.MAX_REGIONS_IN_FLIGHT_SETTING);
        return new ClusterSettings(Settings.EMPTY, settings);
    }

    private static ShardId shardId() {
        return new ShardId("index", randomUUID(), 0);
    }

    private static final class FakeClient implements SearchRelocationCacheTransplant.Client {

        private final List<CacheTransplantRegionInfo> regions;
        private final List<ActionListener<TransportGetCacheTransplantRegionAction.Response>> pending = new ArrayList<>();
        private final AtomicInteger regionCalls = new AtomicInteger();
        private int lastBatchSize;

        private FakeClient(int regionCount) {
            List<CacheTransplantRegionInfo> infos = new ArrayList<>();
            for (int i = 0; i < regionCount; i++) {
                infos.add(new CacheTransplantRegionInfo(1L, "blob", i, 16, 1L, regionCount - i));
            }
            this.regions = List.copyOf(infos);
        }

        @Override
        public void manifest(
            org.elasticsearch.cluster.node.DiscoveryNode node,
            ShardId shardId,
            ActionListener<TransportGetCacheTransplantManifestAction.Response> listener
        ) {
            listener.onResponse(new TransportGetCacheTransplantManifestAction.Response(regions));
        }

        @Override
        public void regions(
            org.elasticsearch.cluster.node.DiscoveryNode node,
            TransportGetCacheTransplantRegionAction.Request request,
            ActionListener<TransportGetCacheTransplantRegionAction.Response> listener
        ) {
            regionCalls.incrementAndGet();
            lastBatchSize = request.regions().size();
            pending.add(listener);
        }

        private void completeOne() {
            assertFalse(pending.isEmpty());
            pending.remove(0).onResponse(TransportGetCacheTransplantRegionAction.Response.none());
        }

        private void completeAll() {
            List<ActionListener<TransportGetCacheTransplantRegionAction.Response>> outstanding = new ArrayList<>(pending);
            pending.clear();
            for (ActionListener<TransportGetCacheTransplantRegionAction.Response> listener : outstanding) {
                listener.onResponse(TransportGetCacheTransplantRegionAction.Response.none());
            }
        }
    }
}
