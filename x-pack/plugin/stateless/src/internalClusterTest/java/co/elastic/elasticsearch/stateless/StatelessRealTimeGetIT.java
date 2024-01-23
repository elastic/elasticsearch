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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.StatelessLiveVersionMapArchive;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.TransportGetFromTranslogAction;
import org.elasticsearch.action.get.TransportShardMultiGetFomTranslogAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.get;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.getArchive;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isSafeAccessRequired;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isUnsafe;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessRealTimeGetIT extends AbstractStatelessIntegTestCase {

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    public void testGet() {
        int numOfShards = randomIntBetween(1, 3);
        int numOfReplicas = randomIntBetween(1, 2);
        startIndexNodes(numOfShards);
        startSearchNodes(numOfReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(numOfShards, numOfReplicas).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build()
        );
        ensureGreen(indexName);

        var bulkRequest = client().prepareBulk();
        int numOfIndexRequests = randomIntBetween(2, 5);
        var setDocIdInIndexingRequest = randomBoolean();
        for (int i = 0; i < numOfIndexRequests; i++) {
            var indexRequest = new IndexRequest(indexName).source("field", "value1");
            if (setDocIdInIndexingRequest) {
                indexRequest.id(String.valueOf(i));
            }
            bulkRequest.add(indexRequest);
        }
        BulkResponse response = bulkRequest.get();
        assertNoFailures(response);

        final AtomicInteger getFromTranslogActionsSent = new AtomicInteger();
        final AtomicInteger shardRefreshActionsSent = new AtomicInteger();
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportGetFromTranslogAction.NAME)) {
                    getFromTranslogActionsSent.incrementAndGet();
                } else if (action.equals(TransportShardRefreshAction.NAME)) {
                    shardRefreshActionsSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        var id = randomFrom(Arrays.stream(response.getItems()).map(BulkItemResponse::getId).toList());

        // Check that a non-real-time get would not see the doc.
        // Note that if we have explicitly set the ids, this would not work, since that would enforce a flush on an unsafe map.
        if (setDocIdInIndexingRequest == false) {
            var nonRealTimeGetResponse = client().prepareGet().setIndex(indexName).setId(id).setRealtime(false).get();
            assertFalse(nonRealTimeGetResponse.isExists());
            assertThat(getFromTranslogActionsSent.get(), equalTo(0));
            assertThat(shardRefreshActionsSent.get(), equalTo(0));
        }

        var getResponse = client().prepareGet().setIndex(indexName).setId(id).get();
        assertTrue(getResponse.isExists());
        assertThat(getFromTranslogActionsSent.get(), equalTo(1));
        assertThat(shardRefreshActionsSent.get(), equalTo(0));

        // Since we refresh, whether the get is real-time or not should not matter
        getResponse = client().prepareGet().setIndex(indexName).setId(id).setRefresh(true).setRealtime(randomBoolean()).get();
        assertTrue(getResponse.isExists());
        assertThat(getFromTranslogActionsSent.get(), equalTo(1));
        assertThat(shardRefreshActionsSent.get(), equalTo(1));

        // A non realtime get, shouldn't cause any GetFromTranslogAction
        getResponse = client().prepareGet().setIndex(indexName).setId(id).setRealtime(false).get();
        assertTrue(getResponse.isExists());
        assertThat(getFromTranslogActionsSent.get(), equalTo(1));
        assertThat(shardRefreshActionsSent.get(), equalTo(1));

        // Test with a doc that has also a newer value after refresh
        assertNoFailures(client().prepareBulk().add(new UpdateRequest().index(indexName).id(id).doc("field", "value2")).get());
        getResponse = client().prepareGet().setIndex(indexName).setId(id).get();
        assertTrue(getResponse.isExists());
        assertThat(getFromTranslogActionsSent.get(), equalTo(2));
        assertThat(shardRefreshActionsSent.get(), equalTo(1));
        assertThat(getResponse.getSource().get("field"), equalTo("value2"));
    }

    public void testMGet() {
        int numOfShards = randomIntBetween(1, 3);
        int numOfReplicas = randomIntBetween(1, 2);
        startIndexNodes(numOfShards);
        startSearchNodes(numOfReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(numOfShards, numOfReplicas).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build()
        );
        ensureGreen(indexName);

        var bulkRequest = client().prepareBulk();
        int numOfIndexRequests = randomIntBetween(2, 5);
        for (int i = 0; i < numOfIndexRequests; i++) {
            var indexRequest = new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25));
            if (randomBoolean()) {
                indexRequest.id(String.valueOf(i));
            }
            bulkRequest.add(indexRequest);
        }
        BulkResponse response = bulkRequest.get();
        assertNoFailures(response);

        final AtomicInteger getFromTranslogActionsSent = new AtomicInteger();
        final AtomicInteger shardRefreshActionsSent = new AtomicInteger();
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportShardMultiGetFomTranslogAction.NAME)) {
                    getFromTranslogActionsSent.incrementAndGet();
                } else if (action.equals(TransportShardRefreshAction.NAME)) {
                    shardRefreshActionsSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        var items = randomSubsetOf(2, IntStream.range(0, numOfIndexRequests).boxed().toList());
        var bulkResponse1 = response.getItems()[items.get(0)].getResponse();
        var bulkResponse2 = response.getItems()[items.get(1)].getResponse();
        // Depending on how many shards the chosen IDs cover, number of ShardRefreshAction and ShardMultiGetFomTranslogAction
        // sent increment by this number.
        var distinctShards = bulkResponse1.getShardId().equals(bulkResponse2.getShardId()) ? 1 : 2;

        var multiGetResponse = client().prepareMultiGet().addIds(indexName, bulkResponse1.getId(), bulkResponse2.getId()).get();
        var multiGetResponses = multiGetResponse.getResponses();
        assertThat(multiGetResponses.length, equalTo(2));
        assertTrue(Arrays.stream(multiGetResponses).map(MultiGetItemResponse::getFailure).allMatch(Objects::isNull));
        assertThat(multiGetResponses[0].getResponse().getId(), equalTo(bulkResponse1.getId()));
        assertTrue(multiGetResponses[0].getResponse().isExists());
        assertThat(multiGetResponses[0].getResponse().getVersion(), equalTo(bulkResponse1.getVersion()));
        assertThat(multiGetResponses[1].getResponse().getId(), equalTo(bulkResponse2.getId()));
        assertTrue(multiGetResponses[1].getResponse().isExists());
        assertThat(multiGetResponses[1].getResponse().getVersion(), equalTo(bulkResponse2.getVersion()));
        assertThat(getFromTranslogActionsSent.get(), equalTo(distinctShards));
        assertThat(shardRefreshActionsSent.get(), equalTo(0));

        // Since we refresh, whether the get is real-time or not should not matter
        multiGetResponse = client().prepareMultiGet()
            .addIds(indexName, bulkResponse1.getId(), bulkResponse2.getId())
            .setRefresh(true)
            .setRealtime(randomBoolean())
            .get();
        assertThat(multiGetResponse.getResponses().length, equalTo(2));
        assertTrue(Arrays.stream(multiGetResponses).map(MultiGetItemResponse::getFailure).allMatch(Objects::isNull));
        assertTrue(Arrays.stream(multiGetResponses).map(r -> r.getResponse().isExists()).allMatch(b -> b.equals(true)));
        assertThat(getFromTranslogActionsSent.get(), equalTo(distinctShards));
        assertThat(shardRefreshActionsSent.get(), equalTo(distinctShards));

        // A non realtime get, shouldn't cause any ShardMultiGetFomTranslogAction
        multiGetResponse = client().prepareMultiGet()
            .addIds(indexName, bulkResponse1.getId(), bulkResponse2.getId())
            .setRealtime(false)
            .get();
        assertThat(multiGetResponse.getResponses().length, equalTo(2));

        assertThat(getFromTranslogActionsSent.get(), equalTo(distinctShards));
        assertThat(shardRefreshActionsSent.get(), equalTo(distinctShards));

        // Test with a doc that has also a newer value after refresh
        var updateResponse = client().prepareBulk()
            .add(new UpdateRequest().index(indexName).id(bulkResponse1.getId()).doc("field", "value2"))
            .get();
        assertNoFailures(updateResponse);
        multiGetResponse = client().prepareMultiGet().addIds(indexName, bulkResponse1.getId(), bulkResponse2.getId()).get();
        multiGetResponses = multiGetResponse.getResponses();
        assertThat(multiGetResponses.length, equalTo(2));
        assertTrue(Arrays.stream(multiGetResponses).map(MultiGetItemResponse::getFailure).allMatch(Objects::isNull));
        assertThat(multiGetResponses[0].getResponse().getId(), equalTo(bulkResponse1.getId()));
        assertTrue(multiGetResponses[0].getResponse().isExists());
        assertThat(multiGetResponses[0].getResponse().getVersion(), equalTo(updateResponse.getItems()[0].getResponse().getVersion()));
        assertThat(multiGetResponses[0].getResponse().getSource().get("field"), equalTo("value2"));
        assertThat(multiGetResponses[1].getResponse().getId(), equalTo(bulkResponse2.getId()));
        assertTrue(multiGetResponses[1].getResponse().isExists());
        assertThat(multiGetResponses[1].getResponse().getVersion(), equalTo(bulkResponse2.getVersion()));
        assertThat(getFromTranslogActionsSent.get(), equalTo(distinctShards * 2));
        assertThat(shardRefreshActionsSent.get(), equalTo(distinctShards));
    }

    public void testLiveVersionMapArchive() throws Exception {
        final int numberOfShards = 1;
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName, indexSettings(numberOfShards, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)).get()
        );
        ensureGreen(indexName);
        var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        var shardId = new ShardId(resolveIndex(indexName), 0);
        var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        var indexShard = indexService.getShard(shardId.id());
        assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
        var indexEngine = ((IndexEngine) indexShard.getEngineOrNull());
        var map = indexEngine.getLiveVersionMap();
        if (randomBoolean()) {
            // Make sure the map is marked as unsafe
            indexDocs(indexName, randomIntBetween(1, 10));
            assertTrue(isUnsafe(map));
        }
        // Enforce safe access mode
        client().prepareIndex(indexName).setId(randomIdentifier()).setSource("field1", randomUnicodeOfLength(10)).get();
        assertTrue(isSafeAccessRequired(map));
        var bulkResponse = client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("k1", "v1"))
            .setRefreshPolicy("false")
            .get();
        assertNoFailures(bulkResponse);
        assertNotNull(get(map, "1"));
        assertTrue(getFromTranslog(indexShard, "1").getResult().isExists());
        // A local refresh doesn't prune the LVM archive
        indexEngine.refresh("test");
        assertNotNull(get(map, "1"));
        assertTrue(getFromTranslog(indexShard, "1").getResult().isExists());
        final long lastUnsafeGenerationForGets = indexEngine.getLastUnsafeSegmentGenerationForGets();
        // Create a new commit explicitly where once ack'ed by the unpronmotables we are sure the docs that were
        // in the archive are visible on the unpromotable shards.
        var newCommitSeen = new CountDownLatch(1);
        AtomicLong newCommitGen = new AtomicLong();
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                final var compoundCommit = ((NewCommitNotificationRequest) request).getCompoundCommit();
                assertEquals(compoundCommit.shardId().getIndexName(), indexName);
                handler.messageReceived(
                    request,
                    new TestTransportChannel(ActionListener.runAfter(new ChannelActionListener<>(channel), () -> {
                        if (compoundCommit.generation() == newCommitGen.get()) {
                            newCommitSeen.countDown();
                        }
                    })),
                    task
                );
            });
        indexDocs(indexName, randomIntBetween(1, 10));
        newCommitGen.set(indexEngine.getLastCommittedSegmentInfos().getGeneration() + 1);
        client().admin().indices().refresh(new RefreshRequest(indexName)).get();
        safeAwait(newCommitSeen);
        var getFromTranslogResponse = getFromTranslog(indexShard, "1");
        assertNull(get(map, "1"));
        assertNull(getFromTranslogResponse.getResult());
        assertEquals(getFromTranslogResponse.segmentGeneration(), lastUnsafeGenerationForGets);
    }

    private TransportGetFromTranslogAction.Response getFromTranslog(IndexShard indexShard, String id) throws Exception {
        var shardId = indexShard.shardId();
        var state = clusterService().state();
        var shardRouting = state.routingTable().shardRoutingTable(shardId).primaryShard();
        var getRequest = client().prepareGet(shardId.getIndexName(), id).request();
        var node = state.nodes().get(shardRouting.currentNodeId());
        assertNotNull(node);
        TransportGetFromTranslogAction.Request request = new TransportGetFromTranslogAction.Request(getRequest, shardId);
        var transportService = internalCluster().getInstance(TransportService.class);
        PlainActionFuture<TransportGetFromTranslogAction.Response> response = new PlainActionFuture<>();
        // We cannot use a Client since we need to directly send the request to the node where the promotable shard is.
        transportService.sendRequest(
            node,
            TransportGetFromTranslogAction.NAME,
            request,
            new ActionListenerResponseHandler<>(
                response,
                TransportGetFromTranslogAction.Response::new,
                transportService.getThreadPool().executor(ThreadPool.Names.SAME)
            )
        );
        return response.get();
    }

    public void testRealTimeGetLocalRefreshDuringUnpromotableRefresh() throws Exception {
        var indexNode = startIndexNode();
        startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        var sendUnpromotableRefreshStarted = new CountDownLatch(1);
        var continueSendUnpromotableRefresh = new CountDownLatch(1);
        var mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, indexNode);
        mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.startsWith(TransportUnpromotableShardRefreshAction.NAME)) {
                sendUnpromotableRefreshStarted.countDown();
                safeAwait(continueSendUnpromotableRefresh);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        indexDocs(indexName, randomIntBetween(5, 10));
        var refreshFuture = indicesAdmin().refresh(new RefreshRequest(indexName)); // async refresh
        var indexShard = findIndexShard(indexName);
        var indexEngine = ((IndexEngine) indexShard.getEngineOrNull());
        var map = indexEngine.getLiveVersionMap();
        assertTrue(isUnsafe(map));
        // While map is unsafe and unpromotable refresh is inflight, index more
        safeAwait(sendUnpromotableRefreshStarted);
        var id = client().prepareIndex(indexName).setSource("field", randomIdentifier()).get().getId();
        // Still unsafe and `id` is not in the previous commit
        assertTrue(isUnsafe(map));
        // Local refresh happens (sets minSafeGeneration).
        indexEngine.refresh("local");
        var archive = (StatelessLiveVersionMapArchive) getArchive(map);
        assertThat(archive.getMinSafeGeneration(), equalTo(indexEngine.getCurrentGeneration() + 1));
        // After unpromotable refresh comes back, map should still be unsafe
        continueSendUnpromotableRefresh.countDown();
        refreshFuture.get();
        assertTrue(isUnsafe(map));
        var getResponse = client().prepareGet(indexName, id).get();
        assertTrue(getResponse.isExists());
    }

    public void testDataVisibility() throws Exception {
        int numOfShards = randomIntBetween(1, 3);
        int numOfReplicas = randomIntBetween(1, 2);
        startIndexNodes(numOfShards);
        startSearchNodes(numOfReplicas);
        var indexName = "test-index";
        createIndex(indexName, indexSettings(numOfShards, numOfReplicas).build());
        ensureGreen(indexName);
        var parallelRuns = randomIntBetween(1, 3);
        Runnable singleRun = () -> {
            var docs = randomIntBetween(500, 1000);
            for (int write = 0; write < docs; write++) {
                if (randomBoolean()) {
                    // Parallel async refreshes randomly
                    indicesAdmin().prepareRefresh(indexName).execute(ActionListener.noop());
                }
                var indexResponse = client().prepareIndex(indexName)
                    .setSource("date", randomPositiveTimeValue(), "value", randomInt())
                    .get(TimeValue.timeValueSeconds(10));
                var id = indexResponse.getId();
                assertNotEquals(id, "");
                var gets = randomIntBetween(20, 50);
                for (int read = 0; read < gets; read++) {
                    var getResponse = client().prepareGet(indexName, id).setRealtime(true).get(TimeValue.timeValueSeconds(10));
                    assertTrue(Strings.format("(write %d): failed to get '%s' at read %s", write, id, read), getResponse.isExists());
                }
            }
        };
        var threads = new ArrayList<Thread>(parallelRuns);
        for (int i = 0; i < parallelRuns; i++) {
            threads.add(new Thread(singleRun));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testStress() throws Exception {
        int numOfShards = randomIntBetween(1, 3);
        int numOfReplicas = randomIntBetween(1, 2);
        startIndexNodes(numOfShards);
        startSearchNodes(numOfReplicas);
        var indexName = "test-index";
        createIndex(indexName, indexSettings(numOfShards, numOfReplicas).build());
        ensureGreen(indexName);
        var indexEngines = new ArrayList<IndexEngine>(numOfShards);
        for (int i = 0; i < numOfShards; i++) {
            var indexShard = findIndexShard(resolveIndex(indexName), i);
            indexEngines.add((IndexEngine) indexShard.getEngineOrNull());
        }
        AtomicBoolean run = new AtomicBoolean(true);
        Runnable localRefresher = () -> {
            while (run.get()) {
                safeSleep(randomLongBetween(0, 100));
                for (var engine : indexEngines) {
                    if (randomBoolean()) {
                        engine.refresh("local");
                    }
                }
            }
        };
        Runnable flusher = () -> {
            while (run.get()) {
                safeSleep(randomLongBetween(0, 100));
                if (randomBoolean()) {
                    indicesAdmin().prepareRefresh(indexName).execute();
                }
            }
        };
        BlockingQueue<String> ids = new LinkedBlockingQueue<>();
        var writersCount = randomIntBetween(1, 5);
        var writersBusy = new CountDownLatch(writersCount);
        var readersCount = randomIntBetween(4, 5);
        var threads = new LinkedList<Thread>();
        Runnable writer = () -> {
            try {
                var bulks = randomIntBetween(100, 200);
                for (int bulk = 0; bulk < bulks; bulk++) {
                    var docs = randomIntBetween(1, 100);
                    var bulkRequest = client().prepareBulk();
                    for (int i = 0; i < docs; i++) {
                        bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
                    }
                    var response = bulkRequest.get(TimeValue.timeValueSeconds(30));
                    assertNoFailures(response);
                    ids.add(randomFrom(Arrays.stream(response.getItems()).map(BulkItemResponse::getId).toList()));
                    safeSleep(randomLongBetween(0, 100));
                }
            } finally {
                writersBusy.countDown();
            }
        };

        Runnable reader = () -> {
            // Run until told to stop AND ids left to GET
            while (run.get() || ids.isEmpty() == false) {
                try {
                    var id = randomBoolean() ? ids.poll(1, TimeUnit.SECONDS) : ids.peek();
                    if (id != null) {
                        var getResponse = client().prepareGet(indexName, id).get(TimeValue.timeValueSeconds(30));
                        assertTrue(Strings.format("could not GET id '%s'", id), getResponse.isExists());
                        safeSleep(randomLongBetween(1, 100));
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        };
        threads.add(new Thread(flusher));
        threads.add(new Thread(localRefresher));
        for (int i = 0; i < writersCount; i++) {
            threads.add(new Thread(writer));
        }
        for (int i = 0; i < readersCount; i++) {
            threads.add(new Thread(reader));
        }
        threads.forEach(Thread::start);
        // wait for writers
        writersBusy.await();
        run.set(false);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testLiveVersionMapMemoryBytesUsed() throws Exception {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        // Enforce safe access mode
        client().prepareIndex(indexName).setId("non-existing").setSource("field1", randomUnicodeOfLength(10)).get();
        IndicesStatsResponse statsBeforeIndexing = indicesAdmin().prepareStats().setSegments(true).get();
        var sizeBeforeIndexing = statsBeforeIndexing.getTotal().getSegments().getVersionMapMemoryInBytes();
        indexDocs(indexName, randomIntBetween(10, 1000));
        IndicesStatsResponse statsAfterIndexing = indicesAdmin().prepareStats().setSegments(true).get();
        var sizeAfterIndexing = statsAfterIndexing.getTotal().getSegments().getVersionMapMemoryInBytes();
        assertThat(sizeAfterIndexing, greaterThan(sizeBeforeIndexing));
        var sendingUnpromotableShardRefresh = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.startsWith(TransportUnpromotableShardRefreshAction.NAME)) {
                sendingUnpromotableShardRefresh.countDown();
            }
            connection.sendRequest(requestId, action, request, options);
        });
        // Commit the indexed docs and make sure after flushing but before unpromotable refresh
        // the map size still reflects the indexed docs.
        var refreshFuture = indicesAdmin().prepareRefresh(indexName).execute();
        safeAwait(sendingUnpromotableShardRefresh);
        IndicesStatsResponse statsAfterFlush = indicesAdmin().prepareStats().setSegments(true).get();
        var sizeAfterFlush = statsAfterFlush.getTotal().getSegments().getVersionMapMemoryInBytes();
        assertEquals(sizeAfterFlush, sizeAfterIndexing, sizeAfterIndexing / 10);
        ElasticsearchAssertions.assertNoFailures(refreshFuture.get());
        // We need a second commit to clear previous entries from the LVM Archive.
        indexDocs(indexName, 1);
        refresh(indexName);
        assertBusy(() -> {
            IndicesStatsResponse statsAfterUnpromotableRefresh = indicesAdmin().prepareStats().setSegments(true).get();
            var sizeAfterUnpromotableRefresh = statsAfterUnpromotableRefresh.getTotal().getSegments().getVersionMapMemoryInBytes();
            assertEquals(sizeAfterUnpromotableRefresh, sizeBeforeIndexing, sizeBeforeIndexing);
        });
    }

    public void testLiveVersionMapMemoryUsageTriggersFlush() throws Exception {
        var indexNode = startMasterAndIndexNode(
            Settings.builder().put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(1)).build()
        );
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        // Enforce safe access mode
        client().prepareIndex(indexName).setId("non-existing").setSource("field1", randomUnicodeOfLength(10)).get();
        var sentNewCommitNotificationsForIndex = new AtomicInteger();
        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.startsWith(TransportNewCommitNotificationAction.NAME)) {
                final var newNotificationRequest = (NewCommitNotificationRequest) request;
                if (newNotificationRequest.getCompoundCommit().shardId().getIndexName().equals(indexName)) {
                    sentNewCommitNotificationsForIndex.incrementAndGet();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        indexDocs(indexName, 100);
        assertBusy(() -> assertThat(sentNewCommitNotificationsForIndex.get(), greaterThan(0)));
    }
}
