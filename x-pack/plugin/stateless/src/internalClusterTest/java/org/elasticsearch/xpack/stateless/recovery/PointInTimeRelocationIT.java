/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.search.SearchService.PIT_RELOCATION_FEATURE_FLAG;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.START_HANDOFF_ACTION_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@TestIssueLogging(
    issueUrl = "https://github.com/elastic/elasticsearch-serverless/issues/5379",
    value = "org.elasticsearch.action.search:DEBUG,"
        + "org.elasticsearch.search.SearchService:DEBUG,"
        + "org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction:DEBUG,"
        + "org.elasticsearch.xpack.stateless.recovery.PITRelocationService:DEBUG"
)
public class PointInTimeRelocationIT extends AbstractStatelessPluginIntegTestCase {

    private final Settings nodeSettings = Settings.builder()
        // speed up search context keepalive checks
        .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
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
}
