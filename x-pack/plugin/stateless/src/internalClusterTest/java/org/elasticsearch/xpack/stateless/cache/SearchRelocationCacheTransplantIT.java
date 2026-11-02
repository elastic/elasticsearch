/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Relocates one search shard and checks that the target copies resident cache regions from the source.
 */
public class SearchRelocationCacheTransplantIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(SharedBytes.PAGE_SIZE).getStringRep()
            )
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(32).getStringRep())
            .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), false)
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), false);
    }

    public void testDisabledRelocationDoesNotRequestRegions() throws Exception {
        String index = startClusterAndIndex();
        IndexShard shard = findSearchShard(index);
        String source = nodeName(shard.routingEntry().currentNodeId());
        String target = otherSearchNode(source);
        AtomicInteger manifests = new AtomicInteger();
        countManifests(source, manifests);
        updateSettings(Settings.builder().put(SearchRelocationCacheTransplant.ENABLED_SETTING.getKey(), false));
        relocate(index, source, target);
        assertThat(manifests.get(), equalTo(0));
    }

    public void testSerialRelocationCopiesRegionsAndReadsLessFromTheObjectStore() throws Exception {
        String index = startClusterAndIndex();
        warm(index);
        IndexShard shard = findSearchShard(index);
        String source = nodeName(shard.routingEntry().currentNodeId());
        String target = otherSearchNode(source);
        assertThat(residentRegions(source, shard), greaterThan(0));

        updateSettings(Settings.builder().put(SearchRelocationCacheTransplant.ENABLED_SETTING.getKey(), false));
        long readsWithoutTransplant = relocateAndReadObjectStoreBytes(index, source, target);
        int regionsWithoutTransplant = residentRegions(target, findSearchShard(index));

        warm(index);
        shard = findSearchShard(index);
        source = nodeName(shard.routingEntry().currentNodeId());
        target = otherSearchNode(source);
        assertThat("source cache should be warm before the transplanted relocation", residentRegions(source, shard), greaterThan(0));
        AtomicInteger manifests = new AtomicInteger();
        countManifests(source, manifests);
        SearchRelocationCacheTransplant.objectStoreWarmingStarted.set(0);
        updateSettings(
            Settings.builder()
                .put(SearchRelocationCacheTransplant.ENABLED_SETTING.getKey(), true)
                .put(SearchRelocationCacheTransplant.ORDERING_SETTING.getKey(), "serial")
                .put(SearchRelocationCacheTransplant.MAX_REGIONS_IN_FLIGHT_SETTING.getKey(), 1)
        );
        long readsWithTransplant = relocateAndReadObjectStoreBytes(index, source, target);
        IndexShard copied = findSearchShard(index);
        int copiedRegions = residentRegions(target, copied);
        assertThat(SearchRelocationCacheTransplant.objectStoreWarmingStarted.get(), greaterThan(0));
        assertThat(manifests.get(), greaterThan(0));
        assertThat(copiedRegions, greaterThan(regionsWithoutTransplant));
        assertThat(readsWithTransplant, lessThanOrEqualTo(readsWithoutTransplant));
        assertThat(readsWithTransplant, lessThan((long) copiedRegions * SharedBytes.PAGE_SIZE));
    }

    public void testMaxRegionsInFlight() throws Exception {
        SearchRelocationCacheTransplant.objectStoreWarmingStarted.set(0);
        String index = startClusterAndIndex();
        warm(index);
        IndexShard shard = findSearchShard(index);
        String source = nodeName(shard.routingEntry().currentNodeId());
        String target = otherSearchNode(source);
        assumeTrue("need several resident regions to fill the in-flight window", residentRegions(source, shard) >= 3);

        CountDownLatch firstBatch = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger entered = new AtomicInteger();
        AtomicInteger firstBatchRegions = new AtomicInteger();
        MockTransportService.getInstance(source)
            .addRequestHandlingBehavior(TransportGetCacheTransplantRegionAction.NAME, (handler, request, channel, task) -> {
                internalCluster().getInstance(ThreadPool.class, source).generic().execute(() -> {
                    int seen = entered.incrementAndGet();
                    if (seen == 1) {
                        firstBatchRegions.set(((TransportGetCacheTransplantRegionAction.Request) request).regions().size());
                        firstBatch.countDown();
                    }
                    await(release);
                    delegate(handler, request, channel, task);
                });
            });
        updateSettings(
            Settings.builder()
                .put(SearchRelocationCacheTransplant.ENABLED_SETTING.getKey(), true)
                .put(SearchRelocationCacheTransplant.ORDERING_SETTING.getKey(), "parallel")
                .put(SearchRelocationCacheTransplant.MAX_REGIONS_IN_FLIGHT_SETTING.getKey(), 2)
        );
        try {
            ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(index, 0, source, target));
            assertTrue(firstBatch.await(20, TimeUnit.SECONDS));
            assertThat(firstBatchRegions.get(), equalTo(2));
            assertThat(SearchRelocationCacheTransplant.objectStoreWarmingStarted.get(), greaterThan(0));
            assertThat(entered.get(), equalTo(1));
        } finally {
            release.countDown();
        }
        ensureGreen(index);
        assertThat(entered.get(), greaterThan(1));
    }

    public void testSerialTimeoutSkipsObjectStoreWarmingAndRecoveryResumes() throws Exception {
        SearchRelocationCacheTransplant.objectStoreWarmingStarted.set(0);
        String index = startClusterAndIndex();
        warm(index);
        IndexShard shard = findSearchShard(index);
        String source = nodeName(shard.routingEntry().currentNodeId());
        String target = otherSearchNode(source);
        CountDownLatch manifestArrived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean manifestHandled = new AtomicBoolean();
        MockTransportService.getInstance(source)
            .addRequestHandlingBehavior(
                TransportGetCacheTransplantManifestAction.NAME,
                (handler, request, channel, task) -> internalCluster().getInstance(ThreadPool.class, source).generic().execute(() -> {
                    manifestArrived.countDown();
                    await(release);
                    delegate(handler, request, channel, task);
                    manifestHandled.set(true);
                })
            );
        updateSettings(
            Settings.builder()
                .put(SearchRelocationCacheTransplant.ENABLED_SETTING.getKey(), true)
                .put(SearchRelocationCacheTransplant.ORDERING_SETTING.getKey(), "serial")
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING.getKey(), "1s")
        );
        var indexId = shard.shardId().getIndex();
        try {
            ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(index, 0, source, target));
            assertTrue(manifestArrived.await(20, TimeUnit.SECONDS));
            assertBusy(() -> {
                IndexShard targetShard = searchShard(indexId, target);
                assertNotNull(targetShard);
                assertTrue(targetShard.routingEntry().started());
            });
        } finally {
            release.countDown();
        }
        assertBusy(() -> assertTrue(manifestHandled.get()));
        assertThat(SearchRelocationCacheTransplant.objectStoreWarmingStarted.get(), equalTo(0));
    }

    private String startClusterAndIndex() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();
        startSearchNode();
        String index = randomIdentifier();
        createIndex(index, indexSettings(1, 1).build());
        String body = randomAlphaOfLength(8_000);
        for (int i = 0; i < 40; i++) {
            prepareIndex(index).setSource("field", body + i).get();
        }
        refresh(index);
        ensureGreen(index);
        return index;
    }

    private void warm(String index) {
        for (int i = 0; i < 5; i++) {
            SearchResponse response = prepareSearch(index).setSize(40).get();
            response.decRef();
        }
    }

    private static int residentRegions(String nodeName, IndexShard shard) {
        var cache = internalCluster().getInstance(StatelessPlugin.SharedBlobCacheServiceSupplier.class, nodeName).get();
        return cache.snapshotResidentRegions(shard.shardId()).size();
    }

    private static String nodeName(String nodeId) {
        return internalCluster().clusterService().state().nodes().get(nodeId).getName();
    }

    private String otherSearchNode(String sourceName) {
        for (String name : internalCluster().getNodeNames()) {
            if (name.equals(sourceName)) {
                continue;
            }
            var node = internalCluster().clusterService().state().nodes().resolveNode(name);
            if (node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE)) {
                return name;
            }
        }
        throw new AssertionError("no other search node besides " + sourceName);
    }

    private static IndexShard searchShard(Index index, String nodeName) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexService(index);
        if (indexService == null) {
            return null;
        }
        return indexService.getShardOrNull(0);
    }

    private void relocate(String index, String source, String target) throws Exception {
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(index, 0, source, target));
        ensureGreen(index);
    }

    private long relocateAndReadObjectStoreBytes(String index, String source, String target) throws Exception {
        relocate(index, source, target);
        IndexShard relocated = findSearchShard(index);
        return SearchDirectory.unwrapDirectory(relocated.store().directory()).totalBytesWarmedFromObjectStore();
    }

    private void updateSettings(Settings.Builder settings) {
        client().admin().cluster().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(settings).get();
    }

    private static void countManifests(String nodeName, AtomicInteger manifests) {
        MockTransportService.getInstance(nodeName)
            .addRequestHandlingBehavior(TransportGetCacheTransplantManifestAction.NAME, (handler, request, channel, task) -> {
                manifests.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static <R extends TransportRequest> void delegate(
        TransportRequestHandler<R> handler,
        R request,
        TransportChannel channel,
        Task task
    ) {
        try {
            handler.messageReceived(request, channel, task);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

}
