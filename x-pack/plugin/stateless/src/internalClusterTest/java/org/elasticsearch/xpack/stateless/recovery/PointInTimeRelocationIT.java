/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.search.SearchService.PIT_RELOCATION_FEATURE_FLAG;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.START_HANDOFF_ACTION_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class PointInTimeRelocationIT extends AbstractStatelessPluginIntegTestCase {
    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(PITRelocationTestPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    /**
     * Stateless plugin that wraps every {@link SearchDirectory} with a tracking subclass.
     * The tracker records any {@link IndexInput}s that are still open when the directory closes,
     * exposing reader-manager leaks as test failures.
     */
    public static class PITRelocationTestPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        public PITRelocationTestPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected SearchDirectory createSearchDirectory(
            StatelessSharedBlobCacheService cacheService,
            CacheBlobReaderService cacheBlobReaderService,
            MutableObjectStoreUploadTracker objectStoreUploadTracker,
            ShardId shardId
        ) {
            return new TrackingSearchDirectory(cacheService, cacheBlobReaderService, objectStoreUploadTracker, shardId);
        }

        private static class TrackingSearchDirectory extends SearchDirectory {

            /** Maps the unwrapped delegate IndexInput to its file name so we can report what was leaked. */
            private final Map<IndexInput, String> openInputs = new ConcurrentHashMap<>();

            TrackingSearchDirectory(
                StatelessSharedBlobCacheService cacheService,
                CacheBlobReaderService cacheBlobReaderService,
                MutableObjectStoreUploadTracker objectStoreUploadTracker,
                ShardId shardId
            ) {
                super(cacheService, cacheBlobReaderService, objectStoreUploadTracker, shardId);
            }

            @Override
            protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
                final IndexInput delegate = super.doOpenInput(name, context, blobFileRanges);
                openInputs.put(delegate, name);
                return new FilterIndexInput("tracked(" + name + ")", delegate) {
                    private final AtomicBoolean closed = new AtomicBoolean(false);

                    @Override
                    public void close() throws IOException {
                        if (closed.compareAndSet(false, true)) {
                            try {
                                super.close();
                            } finally {
                                openInputs.remove(delegate);
                            }
                        }
                    }
                };
            }

            @Override
            public void close() throws IOException {
                super.close();
                assert openInputs.isEmpty() : "open inputs " + openInputs.values() + " not closed";
            }
        }
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // ensure that we don't upload immediately to the object store, since some tests rely on it
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    private final Settings nodeSettings = Settings.builder()
        // speed up search context keepalive checks
        .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
        .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
        // ensure that we don't upload immediately to the object store, since some tests rely on it
        .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
        .put(disableIndexingDiskAndMemoryControllersNodeSettings())
        .build();

    public void testPointInTimeRelocation() throws Exception {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 1).build());
        ensureGreen(indexName);

        var testDataSetup = commonTestdataSetup(indexName, numberOfShards);

        var pitId1 = testDataSetup.pitId1;
        var numDocs_pit1 = testDataSetup.numDocs_pit1;
        var pitId2 = testDataSetup.pitId2;
        var numDocs_pit2 = testDataSetup.numDocs_pit2;

        // index even some more documents
        int additionalDocs = randomIntBetween(1, 100);
        indexDocs(indexName, additionalDocs);
        flushAndRefresh(indexName);

        // check both PITs with the initial search node
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1)), resp -> {
            assertThat(resp.pointInTimeId(), equalTo(pitId1));
            assertHitCount(resp, numDocs_pit1);
        });
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId2)), resp -> {
            assertThat(resp.pointInTimeId(), equalTo(pitId2));
            assertHitCount(resp, numDocs_pit2);
        });
        assertResponse(prepareSearch(), resp -> { assertHitCount(resp, numDocs_pit2 + additionalDocs); });

        var searchNodeCurrent = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeA : searchNodeB;
        var searchNodeNext = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeB : searchNodeA;

        SearchService searchService1 = internalCluster().getInstance(SearchService.class, searchNodeCurrent);
        SearchService searchService2 = internalCluster().getInstance(SearchService.class, searchNodeNext);
        assertEquals(numberOfShards * 2, searchService1.getActivePITContexts() + searchService2.getActivePITContexts());

        logger.info("Current search node: " + searchNodeCurrent);
        logger.info("Next search node: " + searchNodeNext);

        var startHandOffSent = new CountDownLatch(1);
        MockTransportService.getInstance(searchNodeNext).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(START_HANDOFF_ACTION_NAME)) {
                startHandOffSent.countDown();
                assertThat(connection.getNode().getName(), is(equalTo(searchNodeCurrent)));
            }
            connection.sendRequest(requestId, action, request, options);
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeCurrent));
        safeAwait(startHandOffSent);
        ensureGreen(indexName);
        assertBusy(
            () -> { assertEquals("Open contexts after shard relocation.", 0, searchService1.getActivePITContexts()); },
            5,
            TimeUnit.SECONDS
        );

        // stop the current search node in some cases, this i.e. checks that we close contexts after relocation and don't leak open contexts
        boolean stopFirstNode = randomBoolean();
        if (stopFirstNode) {
            internalCluster().stopNode(searchNodeCurrent);
            logger.info("Search node " + searchNodeCurrent + " stopped.");
            assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeCurrent)));
        }
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeNext));

        // search should still work without PIT
        assertResponse(prepareSearch(), resp -> { assertHitCount(resp, numDocs_pit2 + additionalDocs); });

        AtomicReference<BytesReference> updated_pit1 = new AtomicReference<>();
        AtomicReference<BytesReference> updated_pit2 = new AtomicReference<>();
        // search with PIT should still work
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1)), resp -> {
            assertFalse("pit1 should have changed.", isEquivalentId(resp.pointInTimeId(), pitId1));
            assertHitCount(resp, numDocs_pit1);
            updated_pit1.set(resp.pointInTimeId());
        });
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId2)), resp -> {
            assertFalse("pit2 should have changed.", isEquivalentId(resp.pointInTimeId(), pitId2));
            assertHitCount(resp, numDocs_pit2);
            updated_pit2.set(resp.pointInTimeId());
        });

        // repeated search with either updated or old PIT should still work and not change the PIT further
        BytesReference oldOrNewPit = randomBoolean() ? pitId1 : updated_pit1.get();
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(oldOrNewPit)), resp -> {
            assertTrue("pit1 should not have changed further.", isEquivalentId(resp.pointInTimeId(), updated_pit1.get()));
            assertHitCount(resp, numDocs_pit1);
            updated_pit1.set(resp.pointInTimeId());
        });
        oldOrNewPit = randomBoolean() ? pitId2 : updated_pit2.get();
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(oldOrNewPit)), resp -> {
            assertTrue("pit2 should not have changed further.", isEquivalentId(resp.pointInTimeId(), updated_pit2.get()));
            assertHitCount(resp, numDocs_pit2);
            updated_pit2.set(resp.pointInTimeId());
        });

        // close the PIT
        assertClosePit(updated_pit1.get(), numberOfShards);
        assertClosePit(updated_pit2.get(), numberOfShards);
        assertEquals("Open contexts on node " + searchNodeCurrent + ".", 0L, searchService1.getActivePITContexts());
        assertEquals("Open contexts on node " + searchNodeNext + ".", 0L, searchService2.getActivePITContexts());
    }

    public void testNoPointInTimeRelocationWithSettingDisabled() throws Exception {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 1).build());
        ensureGreen(indexName);

        // make sure all shards initially land on one search node
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeB), indexName);
        var testDataSetup = commonTestdataSetup(indexName, numberOfShards);

        var pitId1 = testDataSetup.pitId1;
        var numDocs_pit1 = testDataSetup.numDocs_pit1;
        var pitId2 = testDataSetup.pitId2;
        var numDocs_pit2 = testDataSetup.numDocs_pit2;

        // index even some more documents
        int additionalDocs = randomIntBetween(1, 100);
        indexDocs(indexName, additionalDocs);
        flushAndRefresh(indexName);

        // check both PITs with the initial search node
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1)), resp -> {
            assertThat(resp.pointInTimeId(), equalTo(pitId1));
            assertHitCount(resp, numDocs_pit1);
        });
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId2)), resp -> {
            assertThat(resp.pointInTimeId(), equalTo(pitId2));
            assertHitCount(resp, numDocs_pit2);
        });

        var searchNodeCurrent = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeA : searchNodeB;
        var searchNodeNext = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeB : searchNodeA;

        SearchService searchService1 = internalCluster().getInstance(SearchService.class, searchNodeCurrent);
        SearchService searchService2 = internalCluster().getInstance(SearchService.class, searchNodeNext);
        assertEquals(numberOfShards * 2, searchService1.getActivePITContexts() + searchService2.getActivePITContexts());

        logger.info("Current search node: " + searchNodeCurrent);
        logger.info("Next search node: " + searchNodeNext);

        // disable PIT relocation via setting
        assertAcked(
            clusterAdmin().updateSettings(
                new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                    Settings.builder().put(SearchService.PIT_RELOCATION_ENABLED.getKey(), false)
                )
            )
        );

        var startHandOffSent = new CountDownLatch(1);
        MockTransportService.getInstance(searchNodeNext).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(START_HANDOFF_ACTION_NAME)) {
                startHandOffSent.countDown();
                assertThat(connection.getNode().getName(), is(equalTo(searchNodeCurrent)));
            }
            connection.sendRequest(requestId, action, request, options);
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeCurrent), indexName);
        safeAwait(startHandOffSent);
        ensureGreen(indexName);

        assertBusy(() -> {
            assertEquals("Each PIT should have opened a context per shard.", 2 * numberOfShards, searchService1.getActivePITContexts());
        }, 5, TimeUnit.SECONDS);

        // PIT search still works even though PIT relocation is disabled because the node is still alive
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1)), resp -> {
            assertTrue("pit1 should not have changed.", isEquivalentId(resp.pointInTimeId(), pitId1));
            final TotalHits totalHits = resp.getHits().getTotalHits();
            assertTrue(
                "pit1: expected " + numDocs_pit1 + " hits but was " + totalHits,
                totalHits != null && totalHits.value() == numDocs_pit1
            );
        });
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId2)), resp -> {
            assertTrue("pit2 should not have changed.", isEquivalentId(resp.pointInTimeId(), pitId2));
            final TotalHits totalHits = resp.getHits().getTotalHits();
            assertTrue(
                "pit2 expected " + numDocs_pit2 + " hits but was " + totalHits,
                totalHits != null && totalHits.value() == numDocs_pit2
            );
        });

        // stop the current search node
        internalCluster().stopNode(searchNodeCurrent);
        logger.info("Search node " + searchNodeCurrent + " stopped.");
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeCurrent)));
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeNext));

        // search should still work without PIT
        assertResponse(prepareSearch(), resp -> { assertHitCount(resp, numDocs_pit2 + additionalDocs); });

        // search with PIT will fail on all shards when the original node is gone and PIT relocation is disabled
        assertFailures(
            prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1)),
            Set.of(NOT_FOUND),
            containsString("SearchContextMissingException")
        );
        assertFailures(
            prepareSearch().setPointInTime(new PointInTimeBuilder(pitId2)),
            Set.of(NOT_FOUND),
            containsString("SearchContextMissingException")
        );

        // close the PIT
        closePointInTime(pitId1);
        closePointInTime(pitId2);
        assertEquals("Open contexts on node " + searchNodeCurrent + ".", 0L, searchService1.getActivePITContexts());
        assertEquals("Open contexts on node " + searchNodeNext + ".", 0L, searchService2.getActivePITContexts());
    }

    public void testPointInTimeRelocationConcurrentSearches() throws Exception {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);
        // start a third search node that will always be up since we will restart both searchNodeA and searchNodeB later
        var searchCoordinationNode = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 1).build());
        ensureGreen(indexName);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchCoordinationNode), indexName);

        var testDataSetup = commonTestdataSetup(indexName, numberOfShards);
        AtomicReference<BytesReference> pitId1 = new AtomicReference<>(testDataSetup.pitId1);
        var numDocs_pit1 = testDataSetup.numDocs_pit1;
        AtomicReference<BytesReference> pitId2 = new AtomicReference<>(testDataSetup.pitId2);
        var numDocs_pit2 = testDataSetup.numDocs_pit2;

        SearchService searchService1 = internalCluster().getInstance(SearchService.class, searchNodeA);
        SearchService searchService2 = internalCluster().getInstance(SearchService.class, searchNodeB);

        AtomicReference<Boolean> pit1SearchRunning = new AtomicReference<>(true);
        AtomicReference<Boolean> pit2SearchRunning = new AtomicReference<>(true);
        CountDownLatch latch = new CountDownLatch(40);
        AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(latch);

        // start two threads that continuously search with either PIT1 or PIT2 and assert doc count until stopped
        Thread pit1SearchThread = new Thread(() -> {
            while (pit1SearchRunning.get()) {
                latchRef.get().countDown();
                assertResponse(
                    client(searchCoordinationNode).prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1.get())),
                    resp -> {
                        assertHitCount(resp, numDocs_pit1);
                        pitId1.set(resp.pointInTimeId());
                    }
                );
                // wait a bit to not flood the cluster
                safeSleep(randomTimeValue(50, 100, TimeUnit.MILLISECONDS));
            }
        });
        pit1SearchThread.start();

        Thread pit2SearchThread = new Thread(() -> {
            while (pit2SearchRunning.get()) {
                latchRef.get().countDown();
                assertResponse(
                    client(searchCoordinationNode).prepareSearch().setPointInTime(new PointInTimeBuilder(pitId2.get())),
                    resp -> {
                        assertHitCount(resp, numDocs_pit2);
                        pitId2.set(resp.pointInTimeId());
                    }
                );
                // wait a bit to not flood the cluster
                safeSleep(randomTimeValue(50, 100, TimeUnit.MILLISECONDS));
            }
        });
        pit2SearchThread.start();

        latch.await();
        updateIndexSettings(
            Settings.builder().put("index.routing.allocation.exclude._name", String.join(",", searchNodeA, searchCoordinationNode)),
            indexName
        );
        ensureGreen(indexName);
        internalCluster().restartNode(searchNodeA);
        ensureGreen(indexName);
        logger.info("Search node " + searchNodeA + " restarted.");

        CountDownLatch latch2 = new CountDownLatch(40);
        latchRef.set(latch2);
        latch2.await();

        updateIndexSettings(
            Settings.builder().put("index.routing.allocation.exclude._name", String.join(",", searchNodeB, searchCoordinationNode)),
            indexName
        );
        ensureGreen(indexName);
        internalCluster().restartNode(searchNodeB);
        ensureGreen(indexName);
        logger.info("Search node " + searchNodeB + " restarted.");

        CountDownLatch latch3 = new CountDownLatch(40);
        latchRef.set(latch3);
        latch3.await();

        pit1SearchRunning.set(false);
        pit2SearchRunning.set(false);
        try {
            pit1SearchThread.join();
            pit2SearchThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("Search threads stopped. Closing PITs now.");

        // close the PIT
        assertClosePit(pitId1.get(), numberOfShards);
        assertClosePit(pitId2.get(), numberOfShards);
        assertEquals("Open contexts on node " + searchNodeA + ".", 0L, searchService1.getActivePITContexts());
        assertEquals("Open contexts on node " + searchNodeB + ".", 0L, searchService2.getActivePITContexts());
    }

    public void testPointInTimeRelocationClosingSourceContexts() throws Exception {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 1).build());
        ensureGreen(indexName);

        var testDataSetup = commonTestdataSetup(indexName, numberOfShards);
        // only use PIT1 for this test, close the other
        assertClosePit(testDataSetup.pitId2, numberOfShards);
        var searchNodeCurrent = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeA : searchNodeB;
        var searchNodeNext = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeB : searchNodeA;

        AtomicReference<BytesReference> pitId1 = new AtomicReference<>(testDataSetup.pitId1);
        var numDocs_pit1 = testDataSetup.numDocs_pit1;

        SearchService searchService1 = internalCluster().getInstance(SearchService.class, searchNodeCurrent);
        SearchService searchService2 = internalCluster().getInstance(SearchService.class, searchNodeNext);

        AtomicReference<Boolean> thread1Running = new AtomicReference<>(true);
        AtomicReference<Boolean> thread2Running = new AtomicReference<>(true);
        AtomicInteger searchCount = new AtomicInteger(0);

        // start threads that continuously search with either PIT and assert doc count until stopped
        Thread searchThread1 = new Thread(() -> {
            while (thread1Running.get()) {
                int i = searchCount.incrementAndGet();
                logger.info("Executing search t1 #" + i);
                assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1.get())), resp -> {
                    final TotalHits totalHits = resp.getHits().getTotalHits();
                    assertEquals("Wrong hits for search " + i, totalHits.value(), numDocs_pit1);
                    pitId1.set(resp.pointInTimeId());
                });
            }
        });
        searchThread1.start();

        Thread searchThread2 = new Thread(() -> {
            while (thread2Running.get()) {
                int i = searchCount.incrementAndGet();
                logger.info("Executing search t2 #" + i);
                assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1.get())), resp -> {
                    final TotalHits totalHits = resp.getHits().getTotalHits();
                    assertEquals("Wrong hits for search " + i, totalHits.value(), numDocs_pit1);
                    pitId1.set(resp.pointInTimeId());
                });
            }
        });
        searchThread2.start();

        assertBusy(
            () -> assertTrue("At least one search with pit1 should happen before we continue.", searchCount.get() > 0),
            1,
            TimeUnit.SECONDS
        );

        // relocate all shards off the first node
        updateIndexSettings(
            Settings.builder().put("index.routing.allocation.exclude._name", String.join(",", searchNodeCurrent)),
            indexName
        );
        ensureGreen(indexName);
        logger.info("Search node " + searchNodeCurrent + " excluded.");

        safeSleep(new TimeValue(2, TimeUnit.SECONDS));
        // PIT id should have changed at some point, and all contexts on the source node should be closed
        assertEquals("Open contexts on node " + searchNodeCurrent + ".", 0L, searchService1.getActivePITContexts());
        assertFalse("pit1 should have changed.", isEquivalentId(pitId1.get(), testDataSetup.pitId1));

        thread1Running.set(false);
        thread2Running.set(false);
        try {
            searchThread1.join();
            searchThread2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("Search threads stopped. Closing PITs now.");

        // close the PIT
        assertClosePit(pitId1.get(), numberOfShards);
        assertEquals("Open contexts on node " + searchNodeCurrent + ".", 0L, searchService1.getActivePITContexts());
        assertEquals("Open contexts on node " + searchNodeNext + ".", 0L, searchService2.getActivePITContexts());
    }

    public void testPointInTimeRelocationWithUpdatesAndDeletes() {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        int numberOfShards = 1;
        createIndex(indexName, indexSettings(numberOfShards, 1).build());
        ensureGreen(indexName);

        var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var shardId = new ShardId(resolveIndex(indexName), 0);

        // Index initial documents and collect their auto-generated IDs
        int initialDocs = randomIntBetween(20, 100);
        var bulkResponse = indexDocs(indexName, initialDocs);
        List<String> docIds = Arrays.stream(bulkResponse.getItems()).map(BulkItemResponse::getId).toList();

        final long pit1Generation;
        if (randomBoolean()) {
            flush(indexName);
            var latestUploadedBcc = commitService.getLatestUploadedBcc(shardId);
            assertThat(latestUploadedBcc, is(notNullValue()));
            pit1Generation = lastUploadedCompoundCommitGeneration(latestUploadedBcc);
        } else {
            refresh(indexName);
            var currentVirtualBCC = commitService.getCurrentVirtualBcc(shardId);
            pit1Generation = currentVirtualBCC.getMaxGeneration();
        }
        awaitUntilSearchNodeGetsCommit(indexName, pit1Generation);

        // Open PIT1 capturing the initial state before any modifications
        var pitId1 = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId1);
        int numDocsPit1 = initialDocs;

        // Update a random subset of documents (update some documents — count stays the same)
        List<String> idsToUpdate = randomSubsetOf(randomIntBetween(1, initialDocs / 2), docIds);
        var updateBulk = client().prepareBulk();
        for (String id : idsToUpdate) {
            updateBulk.add(client().prepareIndex(indexName).setId(id).setSource(Map.of("value", "updated")));
        }
        assertNoFailures(updateBulk.get());

        // Delete a different random subset of documents
        List<String> remainingIds = new ArrayList<>(docIds);
        remainingIds.removeAll(idsToUpdate);
        List<String> idsToDelete = randomSubsetOf(randomIntBetween(1, Math.max(1, remainingIds.size() / 2)), remainingIds);
        var deleteBulk = client().prepareBulk();
        for (String id : idsToDelete) {
            deleteBulk.add(client().prepareDelete(indexName, id));
        }
        assertNoFailures(deleteBulk.get());

        // Index additional new documents
        int additionalDocs = randomIntBetween(1, 50);
        indexDocs(indexName, additionalDocs);
        final long pit2Generation;
        if (randomBoolean()) {
            flush(indexName);
            var latestUploadedBcc = commitService.getLatestUploadedBcc(shardId);
            assertThat(latestUploadedBcc, is(notNullValue()));
            pit2Generation = lastUploadedCompoundCommitGeneration(latestUploadedBcc);
        } else {
            refresh(indexName);
            var currentVirtualBCC = commitService.getCurrentVirtualBcc(shardId);
            pit2Generation = currentVirtualBCC.getMaxGeneration();
        }
        awaitUntilSearchNodeGetsCommit(indexName, pit2Generation);

        int numDocsPit2 = initialDocs - idsToDelete.size() + additionalDocs;

        // Open PIT2 capturing the state after updates, deletes, and additions
        var pitId2 = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId2);

        // Index even more documents after PIT2
        int additionalDocsAfterPit2 = randomIntBetween(1, 100);
        indexDocs(indexName, additionalDocsAfterPit2);
        flushAndRefresh(indexName);
        int totalDocsBeforeRelocation = numDocsPit2 + additionalDocsAfterPit2;

        if (randomBoolean()) {
            forceMerge(true);
        }

        // Verify both PITs return the correct counts before relocation
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1)), resp -> {
            assertThat(resp.pointInTimeId(), equalTo(pitId1));
            assertHitCount(resp, numDocsPit1);
        });

        pitId2 = searchAllPagesAndAssertHitCount(pitId2, numDocsPit2);
        assertResponse(
            prepareSearch().setPointInTime(new PointInTimeBuilder(pitId2)).setQuery(termQuery("value", "updated")),
            resp -> assertHitCount(resp, idsToUpdate.size())
        );

        assertResponse(prepareSearch(), resp -> { assertHitCount(resp, totalDocsBeforeRelocation); });

        var searchNodeCurrent = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeA : searchNodeB;
        var searchNodeNext = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeB : searchNodeA;

        // Relocate shards to the other search node
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeCurrent));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeNext));

        // search should still work without PIT
        assertResponse(prepareSearch(), resp -> { assertHitCount(resp, totalDocsBeforeRelocation); });

        var updatedPit1 = new AtomicReference<BytesReference>();
        // PIT1 must still see the original document count (before updates/deletes)
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId1)), resp -> {
            assertHitCount(resp, numDocsPit1);
            updatedPit1.set(resp.pointInTimeId());
        });
        var maybeUpdatedPit1 = searchAllPagesAndAssertHitCount(updatedPit1.get(), numDocsPit1);
        updatedPit1.set(maybeUpdatedPit1);

        var updatedPit2 = new AtomicReference<BytesReference>();
        // PIT2 must see the count after deletes and additions (updates don't change count)
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId2)), resp -> {
            assertHitCount(resp, numDocsPit2);
            updatedPit2.set(resp.pointInTimeId());
        });
        var maybeUpdatedPit2 = searchAllPagesAndAssertHitCount(updatedPit2.get(), numDocsPit2);
        updatedPit2.set(maybeUpdatedPit2);
        assertResponse(
            prepareSearch().setPointInTime(new PointInTimeBuilder(updatedPit2.get())).setQuery(termQuery("value", "updated")),
            resp -> assertHitCount(resp, idsToUpdate.size())
        );

        // Index more documents after relocation
        var indexResponseAfterRelocation = indexDocs(indexName, randomIntBetween(1, 100));
        var newDocIds = Arrays.stream(indexResponseAfterRelocation.getItems()).map(BulkItemResponse::getId).toList();
        if (randomBoolean()) {
            flush(indexName);
        } else {
            refresh(indexName);
        }

        forceMerge(true);

        // Ensure that the PIT with updates/deletes still returns the correct count after the force merge
        var maybeUpdatedPit2AfterForceMerge = searchAllPagesAndAssertHitCount(updatedPit2.get(), numDocsPit2);
        updatedPit2.set(maybeUpdatedPit2AfterForceMerge);
        assertResponse(
            prepareSearch().setPointInTime(new PointInTimeBuilder(updatedPit2.get())).setQuery(termQuery("value", "updated")),
            resp -> assertHitCount(resp, idsToUpdate.size())
        );

        // Delete more documents after relocation to ensure that generational files still work properly
        var newDocIdsToDelete = randomNonEmptySubsetOf(newDocIds);
        var deleteBulkAfterRelocation = client().prepareBulk();
        for (String id : newDocIdsToDelete) {
            deleteBulkAfterRelocation.add(client().prepareDelete(indexName, id));
        }
        assertNoFailures(deleteBulkAfterRelocation.get());
        flush(indexName);
        final long generationAfterPostRelocationWrites = lastUploadedCompoundCommitGeneration(commitService.getLatestUploadedBcc(shardId));
        awaitUntilSearchNodeGetsCommit(indexName, generationAfterPostRelocationWrites);
        var totalDocsAfterRelocation = totalDocsBeforeRelocation + newDocIds.size() - newDocIdsToDelete.size();

        assertResponse(prepareSearch(), resp -> { assertHitCount(resp, totalDocsAfterRelocation); });

        // close the PITs
        closePointInTime(updatedPit1.get());
        closePointInTime(updatedPit2.get());
    }

    public void testPointInTimeRelocationReferencingTheSameCommit() {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var shardId = new ShardId(resolveIndex(indexName), 0);

        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);

        var latestUploadedBcc = commitService.getLatestUploadedBcc(shardId);
        assertThat(latestUploadedBcc, is(notNullValue()));
        var generation = lastUploadedCompoundCommitGeneration(latestUploadedBcc);
        awaitUntilSearchNodeGetsCommit(indexName, generation);

        var pitId = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId);

        // Verify PIT works before relocation
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
            assertThat(resp.pointInTimeId(), equalTo(pitId));
            assertHitCount(resp, numDocs);
        });

        var searchNodeCurrent = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeA : searchNodeB;
        var searchNodeNext = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeB : searchNodeA;

        // Relocate the search shard to the other node
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeCurrent));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeNext));

        // PIT search should still work after relocation, with an updated PIT id
        var updatedPitId = new AtomicReference<BytesReference>();
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
            assertFalse("PIT id should have changed after relocation.", isEquivalentId(resp.pointInTimeId(), pitId));
            assertHitCount(resp, numDocs);
            updatedPitId.set(resp.pointInTimeId());
        });

        // Close the PIT with the updated id
        closePointInTime(updatedPitId.get());
    }

    /**
     * Verifies that no {@link IndexInput}s are leaked when the target shard's engine closes while
     * a PIT relocation handoff is in progress.
     * <p>
     * Two PITs are opened on different commits. The blob read for one of the two PIT BCCs is randomly
     * blocked so that both cleanup paths are exercised: a reader registered before engine close (cleaned
     * up by {@code relocatedPITsReadersManager}) and a reader whose registration races with engine close
     * (cleaned up in the {@code acquireSearcherForCommit} finally block).
     * <p>
     * Leaks are detected by {@link PITRelocationTestPlugin.TrackingSearchDirectory}.
     */
    public void testRelocatedPitContextsReleasedWhenShardClosedDuringHandoff() throws Exception {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());

        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var shardId = new ShardId(resolveIndex(indexName), 0);

        int numDocs = randomIntBetween(1, 50);
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);
        var latestUploadedBcc = commitService.getLatestUploadedBcc(shardId);
        assertThat(latestUploadedBcc, is(notNullValue()));
        long pit1BccBlobGeneration = latestUploadedBcc.primaryTermAndGeneration().generation();
        awaitUntilSearchNodeGetsCommit(indexName, lastUploadedCompoundCommitGeneration(latestUploadedBcc));

        var pitId = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId);

        indexDocs(indexName, randomIntBetween(1, 50));
        flushAndRefresh(indexName);
        // ensure that the PIT commits do not reference between them to block the blob read
        forceMerge(true);
        var latestUploadedBcc2 = commitService.getLatestUploadedBcc(shardId);
        assertThat(latestUploadedBcc2, is(notNullValue()));
        long pit2BccBlobGeneration = latestUploadedBcc2.primaryTermAndGeneration().generation();
        awaitUntilSearchNodeGetsCommit(indexName, lastUploadedCompoundCommitGeneration(latestUploadedBcc2));

        var pitId2 = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId2);

        // Index more docs so the target's current reader is on a different commit than the PIT,
        // ensuring openIfChanged returns a new reader (not null) during the handoff.
        indexDocs(indexName, randomIntBetween(1, 50));
        flushAndRefresh(indexName);
        forceMerge(true);

        var searchNodeCurrent = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeA : searchNodeB;
        var searchNodeNext = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeB : searchNodeA;

        // Block blob reads on the target for the PIT's BCC blob. This intercepts
        // acquireSearcherForCommit while it is inside openIfChanged opening the PIT commit.
        var blobReadStarted = new CountDownLatch(1);
        var blobReadBlocked = new CountDownLatch(1);
        var blobNameToBlock = BatchedCompoundCommit.blobNameFromGeneration(randomBoolean() ? pit1BccBlobGeneration : pit2BccBlobGeneration);
        setNodeRepositoryStrategy(searchNodeNext, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                if (blobNameToBlock.equals(blobName)) {
                    blobReadStarted.countDown();
                    safeAwait(blobReadBlocked);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });
        // Trigger the relocation; acquireSearcherForCommit will block inside openIfChanged
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeCurrent));
        safeAwait(blobReadStarted);

        // Delete the index while one PIT relocation is opening its reader. The engine closes,
        // disposing of any PendingPITReaders already registered by the non-blocked PIT relocation.
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Unblock. The blocked PIT relocation resumes opening its reader and tries to register it
        // on a closed engine; the reader is cleaned up in the finally block.
        blobReadBlocked.countDown();
    }

    public void testRelocationWithPITReferencingPinnedGenFiles() {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var shardId = new ShardId(resolveIndex(indexName), 0);

        // Index initial documents and collect their auto-generated IDs
        int initialDocs = randomIntBetween(75, 100);
        var bulkResponse = indexDocs(indexName, initialDocs);
        List<String> docIds = Arrays.stream(bulkResponse.getItems()).map(BulkItemResponse::getId).toList();
        flush(indexName);

        var idDocumentInSegment1 = "random";
        {
            // First generational doc values
            var updateBulk = client().prepareBulk();
            // Update one document from segment _0 to create a generational file targeting that segment
            String updatedDoc = randomFrom(docIds);
            updateBulk.add(
                client().prepareIndex(indexName).setId(updatedDoc).setSource(Map.of("field", randomUnicodeOfCodepointLengthBetween(10, 25)))
            );
            // Add a new document to target the segment in a future update in segment _1
            updateBulk.add(
                client().prepareIndex(indexName)
                    .setId(idDocumentInSegment1)
                    .setSource(Map.of("field", randomUnicodeOfCodepointLengthBetween(10, 25)))
            );
            BulkResponse response = updateBulk.get();
            assertNoFailures(response);
            flush(indexName);
        }

        {
            var updateBulk = client().prepareBulk();
            // Update one document from segment _1 so the PIT references generational files in different commits
            updateBulk.add(
                client().prepareIndex(indexName)
                    .setId(idDocumentInSegment1)
                    .setSource(Map.of("field", randomUnicodeOfCodepointLengthBetween(10, 25)))
            );
            updateBulk.add(client().prepareIndex(indexName).setSource(Map.of("field", randomUnicodeOfCodepointLengthBetween(10, 25))));
            assertNoFailures(updateBulk.get());
            flush(indexName);
        }

        // Open the PIT capturing the second commit which would include references to generational files with two generations,
        // the first one is pinned to the initial commit generation in the blob locations
        var latestUploadedBcc = commitService.getLatestUploadedBcc(shardId);
        assertThat(latestUploadedBcc, is(notNullValue()));
        awaitUntilSearchNodeGetsCommit(indexName, lastUploadedCompoundCommitGeneration(latestUploadedBcc));

        var pitId = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId);

        // Force merge so once we relocate the search shard we start with a commit without generational files
        forceMerge(true);

        var newSearchNode = startSearchNode(nodeSettings);

        // Relocate shards to the other search node
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeA));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(newSearchNode));

        // PIT search should still work after relocation, with an updated PIT id
        var updatedPitId = new AtomicReference<BytesReference>();
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
            // We index two new documents together with the updates
            assertHitCount(resp, initialDocs + 2);
            updatedPitId.set(resp.pointInTimeId());
        });

        // Close the PIT with the updated id
        closePointInTime(updatedPitId.get());
    }

    /**
     * Lucene generation of the newest compound commit contained in the uploaded BCC.
     * {@link BatchedCompoundCommit#primaryTermAndGeneration()} is the VBCC / blob key (opening commit),
     * which can be strictly older than {@link BatchedCompoundCommit#lastCompoundCommit()} when the
     * batch holds multiple commits — waiting on the former can unblock before the search shard has applied the latest CC.
     */
    private static long lastUploadedCompoundCommitGeneration(BatchedCompoundCommit latestUploadedBcc) {
        return latestUploadedBcc.lastCompoundCommit().generation();
    }

    private static void awaitUntilSearchNodeGetsCommit(String indexName, long generation) {
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();
        var searchEngine = getShardEngine(findSearchShard(indexName), SearchEngine.class);
        var uploadedBCCRefreshedListener = new SubscribableListener<Long>();
        searchEngine.addPrimaryTermAndGenerationListener(primaryTerm, generation, uploadedBCCRefreshedListener);
        safeAwait(uploadedBCCRefreshedListener);
    }

    public void testPointInTimeRelocationHandoffBccReadFailureFallsBackToLazyReconstruction() throws Exception {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        // pin all shards on searchNodeA so it is the predictable relocation source
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeB), indexName);
        ensureGreen(indexName);

        // index docs and flush to ensure a BCC is committed to the object store
        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);

        var pitId = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId);

        // inject a read failure on the source node for all BCC blobs to prevent PIT context transfer
        setNodeRepositoryFailureStrategy(searchNodeA, true, false, Map.of(OperationPurpose.INDICES, "stateless_commit_.*"));

        // relocate shards from searchNodeA to searchNodeB
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeA), indexName);
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeB));

        // stop the source node so that its search contexts are gone
        internalCluster().stopNode(searchNodeA);

        // normal search should still work after relocation
        assertResponse(prepareSearch(), resp -> assertHitCount(resp, numDocs));

        // PIT search should also succeed: even though the BCC read failure prevented an explicit context
        // transfer during handoff, stateless search reconstructs the context on-demand.
        var updatedPitId = new AtomicReference<BytesReference>();
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
            assertHitCount(resp, numDocs);
            updatedPitId.set(resp.pointInTimeId());
        });

        closePointInTime(updatedPitId.get());
    }

    /**
     * Verifies that PITs opened at different CC positions within the same BCC are all correctly
     * transferred during shard relocation. Three rounds of indexing with refresh-only (no flush)
     * accumulate three CCs in a single VBCC. Each PIT is matched to its CC entry when building the handoff.
     */
    public void testPointInTimeRelocationPitPositionsInBcc() {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        // ensure three refresh cycles do not trigger a count-based VBCC upload before relocation
        var nodeSettings = Settings.builder()
            .put(this.nodeSettings)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 10)
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        // pin all shards on searchNodeA so it is the predictable relocation source
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeB), indexName);
        ensureGreen(indexName);

        var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var shardId = new ShardId(resolveIndex(indexName), 0);

        // Three rounds of indexing and refresh (not flush). Each refresh creates a new Lucene commit
        // (flush-by-refresh) that is added to the VBCC without uploading it, so all three CCs
        // accumulate in the same VBCC.
        int numDocsFirst = randomIntBetween(1, 50);
        indexDocs(
            indexName,
            numDocsFirst,
            bulkRequestBuilder -> bulkRequestBuilder.add(client().prepareIndex(indexName).setSource(Map.of("value", "first")))
        );
        refresh(indexName);

        var currentVirtualBCC = commitService.getCurrentVirtualBcc(shardId);
        assertNotNull(currentVirtualBCC);

        final var firstPitGeneration = currentVirtualBCC.getMaxGeneration();
        awaitUntilSearchNodeGetsCommit(indexName, firstPitGeneration);
        final var pitFirst = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();

        int numDocsMiddle = randomIntBetween(1, 50);
        indexDocs(
            indexName,
            numDocsMiddle,
            bulkRequestBuilder -> bulkRequestBuilder.add(client().prepareIndex(indexName).setSource(Map.of("value", "middle")))
        );
        refresh(indexName);

        final var pitMiddleGeneration = currentVirtualBCC.getMaxGeneration();
        awaitUntilSearchNodeGetsCommit(indexName, pitMiddleGeneration);
        final var pitMiddle = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();

        int numDocsLast = randomIntBetween(1, 50);
        indexDocs(
            indexName,
            numDocsLast,
            bulkRequestBuilder -> bulkRequestBuilder.add(client().prepareIndex(indexName).setSource(Map.of("value", "last")))
        );
        refresh(indexName);

        final var pitLastGeneration = currentVirtualBCC.getMaxGeneration();
        awaitUntilSearchNodeGetsCommit(indexName, pitLastGeneration);
        final var pitLast = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();

        flush(indexName);

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeA), indexName);
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeB));

        AtomicReference<BytesReference> updatedPitFirst = new AtomicReference<>(pitFirst);
        AtomicReference<BytesReference> updatedPitMiddle = new AtomicReference<>(pitMiddle);
        AtomicReference<BytesReference> updatedPitLast = new AtomicReference<>(pitLast);

        // each PIT should see only the docs that were indexed before it was opened
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitFirst)), resp -> {
            // To account for the marker document
            assertHitCount(resp, numDocsFirst + 1);
            updatedPitFirst.set(resp.pointInTimeId());
        });
        assertResponse(
            prepareSearch().setPointInTime(new PointInTimeBuilder(updatedPitFirst.get())).setQuery(termQuery("value", "first")),
            resp -> assertHitCount(resp, 1)
        );

        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitMiddle)).setSize(1000), resp -> {
            // To account for the marker documents
            assertHitCount(resp, numDocsFirst + numDocsMiddle + 2);
            updatedPitMiddle.set(resp.pointInTimeId());
        });
        assertResponse(
            prepareSearch().setPointInTime(new PointInTimeBuilder(updatedPitMiddle.get()))
                .setQuery(boolQuery().should(termQuery("value", "first")).should(termQuery("value", "middle"))),
            resp -> assertHitCount(resp, 2)
        );

        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitLast)).setSize(1000), resp -> {
            // To account for the marker documents
            assertHitCount(resp, numDocsFirst + numDocsMiddle + numDocsLast + 3);
            updatedPitLast.set(resp.pointInTimeId());
        });
        assertResponse(
            prepareSearch().setPointInTime(new PointInTimeBuilder(updatedPitLast.get()))
                .setQuery(
                    boolQuery().should(termQuery("value", "first")).should(termQuery("value", "middle")).should(termQuery("value", "last"))
                ),
            resp -> assertHitCount(resp, 3)
        );

        assertClosePit(updatedPitFirst.get(), 1);
        assertClosePit(updatedPitMiddle.get(), 1);
        assertClosePit(updatedPitLast.get(), 1);
    }

    private void assertClosePit(BytesReference pitId, int expectedFreedContexts) {
        ClosePointInTimeResponse closePit = closePointInTime(pitId);
        assertEquals("Closing pit should free open contexts.", expectedFreedContexts, closePit.getNumFreed());
    }

    record TestDataSetup(BytesReference pitId1, int numDocs_pit1, BytesReference pitId2, int numDocs_pit2) {}

    private TestDataSetup commonTestdataSetup(String indexName, int numberOfShards) {
        logger.info("number of shards: " + numberOfShards);

        int numDocsPit1 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsPit1);
        flushAndRefresh(indexName);

        // open a point in time
        var openPITResponse = openPointInTime(indexName, TimeValue.timeValueMinutes(1));
        BytesReference pitId1 = openPITResponse.getPointInTimeId();
        assertNotNull(pitId1);
        logger.info(
            "Original PIT id1: " + new PointInTimeBuilder(pitId1).getSearchContextId(this.writableRegistry()).toString().replace("},", "\n")
        );

        // index some more documents
        int moreDocs = randomIntBetween(1, 100);
        indexDocs(indexName, moreDocs);
        flushAndRefresh(indexName);
        int numDocs_pit2 = numDocsPit1 + moreDocs;

        // open a second point in time
        openPITResponse = openPointInTime(indexName, TimeValue.timeValueMinutes(1));
        BytesReference pitId2 = openPITResponse.getPointInTimeId();
        assertNotNull(pitId2);
        logger.info(
            "Original PIT id2: " + new PointInTimeBuilder(pitId2).getSearchContextId(this.writableRegistry()).toString().replace("},", "\n")
        );
        TestDataSetup testDataSetup = new TestDataSetup(pitId1, numDocsPit1, pitId2, numDocs_pit2);
        logger.info("TestDataSetup: " + testDataSetup);
        return testDataSetup;
    }

    private boolean isEquivalentId(BytesReference id1bytes, BytesReference id2bytes) {
        var id1 = SearchContextId.decode(this.writableRegistry(), id1bytes);
        var id2 = SearchContextId.decode(this.writableRegistry(), id2bytes);
        return id1.equals(id2);
    }

    private OpenPointInTimeResponse openPointInTime(String index, TimeValue keepAlive) {
        return client().execute(TransportOpenPointInTimeAction.TYPE, new OpenPointInTimeRequest(index).keepAlive(keepAlive)).actionGet();
    }

    private ClosePointInTimeResponse closePointInTime(BytesReference readerId) {
        return client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(readerId)).actionGet();
    }

    private static BytesReference searchAllPagesAndAssertHitCount(BytesReference pitId, int expectedNumDocs) {
        var nextPitId = new AtomicReference<>(pitId);
        var searchAfter = new AtomicReference<Object[]>();
        var totalHits = new AtomicInteger();
        var firstPage = new AtomicBoolean(true);

        do {
            var request = prepareSearch().setPointInTime(new PointInTimeBuilder(nextPitId.get()))
                .setSize(randomIntBetween(1, expectedNumDocs))
                .addSort(SortBuilders.pitTiebreaker());

            if (searchAfter.get() != null && searchAfter.get().length > 0) {
                request.searchAfter(searchAfter.get());
            }

            assertResponse(request, resp -> {
                if (firstPage.getAndSet(false)) {
                    assertHitCount(resp, expectedNumDocs);
                }
                SearchHit[] hits = resp.getHits().getHits();
                totalHits.addAndGet(hits.length);
                nextPitId.set(resp.pointInTimeId());
                if (hits.length > 0) {
                    searchAfter.set(hits[hits.length - 1].getSortValues());
                    for (SearchHit hit : hits) {
                        assertThat(hit.getSourceAsMap(), is(notNullValue()));
                    }
                } else {
                    searchAfter.set(null);
                }
            });
            assertThat(
                "pagination returned more hits than expected — likely stuck on the same page",
                totalHits.get(),
                lessThanOrEqualTo(expectedNumDocs)
            );

        } while (searchAfter.get() != null);
        assertEquals(expectedNumDocs, totalHits.get());
        return nextPitId.get();
    }
}
