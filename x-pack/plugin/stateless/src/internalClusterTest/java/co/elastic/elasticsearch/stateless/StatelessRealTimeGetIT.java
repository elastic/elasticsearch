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

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.LiveVersionMap;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
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
                    .get(timeValueSeconds(10));
                var id = indexResponse.getId();
                assertNotEquals(id, "");
                var gets = randomIntBetween(20, 50);
                for (int read = 0; read < gets; read++) {
                    var getResponse = client().prepareGet(indexName, id).setRealtime(true).get(timeValueSeconds(10));
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
        AtomicBoolean runReader = new AtomicBoolean(true);
        AtomicBoolean runFlusherRefresher = new AtomicBoolean(true);
        Runnable localRefresher = () -> {
            while (runFlusherRefresher.get()) {
                safeSleep(randomLongBetween(0, 100));
                for (var engine : indexEngines) {
                    if (randomBoolean()) {
                        engine.refresh("local");
                    }
                }
            }
            logger.info("Local refresher thread has finished");
        };
        Runnable flusher = () -> {
            while (runFlusherRefresher.get()) {
                safeSleep(randomLongBetween(0, 100));
                if (randomBoolean()) {
                    indicesAdmin().prepareRefresh(indexName).execute();
                }
            }
            logger.info("Flusher thread has finished");
        };
        BlockingQueue<String> ids = new LinkedBlockingQueue<>();
        var writersCount = randomIntBetween(1, 5);
        var writersBusy = new CountDownLatch(writersCount);
        var readersCount = randomIntBetween(4, 5);
        var readersBusy = new CountDownLatch(readersCount);
        var threads = new LinkedList<Thread>();
        Runnable writer = () -> {
            try {
                var bulks = randomIntBetween(10, 100);
                for (int bulk = 0; bulk < bulks; bulk++) {
                    var docs = randomIntBetween(1, 100);
                    var bulkRequest = client().prepareBulk();
                    for (int i = 0; i < docs; i++) {
                        bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
                    }
                    BulkResponse response;
                    try {
                        response = bulkRequest.get(timeValueSeconds(30));
                    } catch (ElasticsearchTimeoutException e) {
                        var message =
                            "Unable to bulk insert %d documents. Aborted due to a timeout. Iteration number: %d, Number of shards: %d";
                        throw new AssertionError(Strings.format(message, docs, bulk, numOfShards), e);
                    }
                    assertNoFailures(response);
                    ids.add(randomFrom(Arrays.stream(response.getItems()).map(BulkItemResponse::getId).toList()));
                    safeSleep(randomLongBetween(0, 100));
                }
            } finally {
                writersBusy.countDown();
                logger.info("Writer thread has finished");
            }
        };

        Runnable reader = () -> {
            try {
                // Run until told to stop AND ids left to GET
                while (runReader.get() || ids.isEmpty() == false) {
                    String id = null;
                    try {
                        id = randomBoolean() ? ids.poll(1, TimeUnit.SECONDS) : ids.peek();
                        if (id != null) {
                            var getResponse = client().prepareGet(indexName, id).get(timeValueSeconds(30));
                            assertTrue(Strings.format("could not GET id '%s'", id), getResponse.isExists());
                            safeSleep(randomLongBetween(1, 100));
                        }
                    } catch (ElasticsearchTimeoutException e) {
                        throw new AssertionError(Strings.format("Unable to GET id '%s' due to timeout", id), e);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            } finally {
                readersBusy.countDown();
                logger.info("Reader thread has finished");
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
        runReader.set(false);
        // Keep the flusher and refresher threads running along with the readers
        readersBusy.await();
        runFlusherRefresher.set(false);
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

    // Tests that if the shard is unassigned the RTG request fails since there is no available shard to route the request to.
    public void testRealTimeGetAndMGetFailsWhenShardIsUnassigned() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                .build()
        );
        ensureGreen(indexName);
        var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        var indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        var indexShard = indexService.getShard(0);
        indexShard.failShard("test", new Exception("test"));
        // Wait until the search node sees the state where the shard is unassigned, otherwise we get a NodeNotConnectedException.
        assertBusy(
            () -> assertTrue(
                internalCluster().clusterService(searchNode)
                    .state()
                    .routingTable()
                    .shardRoutingTable(indexName, 0)
                    .primaryShard()
                    .unassigned()
            )
        );
        // The expected NoShardAvailableActionException happens on the search node
        if (randomBoolean()) {
            var response = client().prepareMultiGet().addIds(indexName, "non-existing").setRealtime(true).get(timeValueSeconds(10));
            assertEquals(response.getResponses().length, 1);
            var shardMGetResponse = response.getResponses()[0];
            assertNull(shardMGetResponse.getResponse());
            assertThat(shardMGetResponse.getFailure().getFailure(), instanceOf(NoShardAvailableActionException.class));
        } else {
            expectThrows(NoShardAvailableActionException.class, client().prepareGet(indexName, "non-existing").setRealtime(true));
        }
    }

    public void testRealTimeGetAndMGetRetryFailsWhenIndexDeletedOrUnassigned() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                .build()
        );
        ensureGreen(indexName);
        String actionName = randomBoolean() ? TransportGetFromTranslogAction.NAME : TransportShardMultiGetFomTranslogAction.NAME;
        final var shardId = findIndexShard(indexName).shardId();
        final var getFromTranslogCount = new AtomicInteger();
        final var getFromTranslogHandled = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode).addRequestHandlingBehavior(actionName, (handler, request, channel, task) -> {
            getFromTranslogCount.incrementAndGet();
            // Remove the index or make the promotable shard unassigned and cause a retry
            if (randomBoolean()) {
                assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)));
            } else {
                var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
                var indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
                var indexShard = indexService.getShard(shardId.id());
                indexShard.failShard("test", new Exception("test"));
            }
            channel.sendResponse(randomFrom(new IndexNotFoundException(indexName), new ShardNotFoundException(shardId)));
            getFromTranslogHandled.countDown();
        });
        if (actionName.equals(TransportGetFromTranslogAction.NAME)) {
            final var getFuture = client().prepareGet(indexName, "non-existing").setRealtime(true).execute();
            safeAwait(getFromTranslogHandled);
            // The deletion/unassignment of the index/shard should cause a retry
            expectThrows(NoShardAvailableActionException.class, getFuture);
        } else {
            final var mgetFuture = client().prepareMultiGet().addIds(indexName, "non-existing").setRealtime(true).execute();
            safeAwait(getFromTranslogHandled);
            // The deletion/unassignment of the index/shard should cause a retry
            var mgetResponse = mgetFuture.get(10, TimeUnit.SECONDS);
            assertEquals(mgetResponse.getResponses().length, 1);
            var responseItem = mgetResponse.getResponses()[0];
            assertThat(responseItem.getFailure().getFailure(), instanceOf(NoShardAvailableActionException.class));
        }
        assertEquals(1, getFromTranslogCount.get());
    }

    public void testGetFromTranslogDuringRelocation() throws ExecutionException, InterruptedException, TimeoutException {
        startMasterOnlyNode();
        var indexNodeA = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        if (randomBoolean()) {
            // Enforce safe access mode
            client().prepareIndex(indexName).setId("unique-id").setSource("field1", randomUnicodeOfLength(10)).get();
            var map = getLiveVersionMap(indexNodeA, indexName, 0);
            assertTrue(isSafeAccessRequired(map));
        }
        var bulkResponse = indexDocs(indexName, randomIntBetween(10, 50));
        var id = randomFrom(Arrays.stream(bulkResponse.getItems()).map(BulkItemResponse::getId).collect(Collectors.toSet()));
        var indexNodeB = startIndexNode();
        CountDownLatch receivedGetFromTranslog = new CountDownLatch(1);
        CountDownLatch continueGetFromTranslog = new CountDownLatch(1);
        AtomicLong getFromTranslogCountOnNodeA = new AtomicLong();
        AtomicLong getFromTranslogCountOnNodeB = new AtomicLong();
        MockTransportService.getInstance(indexNodeA)
            .addRequestHandlingBehavior(TransportGetFromTranslogAction.NAME, (handler, request, channel, task) -> {
                receivedGetFromTranslog.countDown();
                getFromTranslogCountOnNodeA.incrementAndGet();
                safeAwait(continueGetFromTranslog);
                handler.messageReceived(request, channel, task);
            });
        MockTransportService.getInstance(indexNodeB)
            .addRequestHandlingBehavior(TransportGetFromTranslogAction.NAME, (handler, request, channel, task) -> {
                getFromTranslogCountOnNodeB.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });
        // Trigger a GetFromTranslog and relocate the shard before indexNodeA gets to process the request.
        final var getNonExistingId = randomBoolean();
        var getFuture = client().prepareGet(indexName, getNonExistingId ? "non-existing" : id).setRealtime(true).execute();
        safeAwait(receivedGetFromTranslog);
        logger.info("--> starting promotable shard relocation");
        clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, indexNodeA, indexNodeB)).execute().actionGet();
        ensureGreen(indexName);
        logger.info("--> relocation finished");
        continueGetFromTranslog.countDown();
        var getResponse = getFuture.get(10, TimeUnit.SECONDS);
        assertEquals(1L, getFromTranslogCountOnNodeA.get());
        assertEquals(1L, getFromTranslogCountOnNodeB.get());
        assertThat(getResponse.isExists(), equalTo(getNonExistingId ? false : true));
    }

    public void testGetFromTranslogRetries() throws ExecutionException, InterruptedException, TimeoutException {
        startMasterOnlyNode();
        var indexNodeA = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        final var shardId = findIndexShard(indexName).shardId();
        final var failCount = randomIntBetween(1, 3);
        final var getFromTranslogSeen = new AtomicInteger();
        MockTransportService.getInstance(indexNodeA)
            .addRequestHandlingBehavior(TransportGetFromTranslogAction.NAME, (handler, request, channel, task) -> {
                if (getFromTranslogSeen.incrementAndGet() <= failCount) {
                    channel.sendResponse(randomFrom(new IndexNotFoundException(indexName), new ShardNotFoundException(shardId)));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        final var getFuture = client().prepareGet(indexName, "non-existing").setRealtime(true).execute();
        // Trigger enough cluster state updates to see the reties succeed.
        for (int i = 0; i < failCount; i++) {
            indicesAdmin().preparePutMapping(indexName).setSource("field" + i, "type=keyword").get();
        }
        assertFalse(getFuture.get(10, TimeUnit.SECONDS).isExists());
        assertEquals(failCount + 1, getFromTranslogSeen.get());
    }

    public void testShardMultiGetFromTranslogDuringRelocation() throws Exception {
        startMasterOnlyNode();
        var indexNodeA = startIndexNode();
        var indexNodeB = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        // If we use more than 2 shards and the mget request fans out to all the shards, we end up blocking more than two
        // threads from the GET threadpool and leaving no further threads available and cause a timeout. Therefore, use max 2 shards.
        final var noOfShards = randomIntBetween(1, 2);
        createIndex(
            indexName,
            indexSettings(noOfShards, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put("index.routing.allocation.exclude._name", indexNodeB)
                .build()
        );
        ensureGreen(indexName);
        if (randomBoolean() && noOfShards == 1) {
            // Enforce safe access mode
            client().prepareIndex(indexName).setId("unique-id").setSource("field1", randomUnicodeOfLength(10)).get();
            var map = getLiveVersionMap(indexNodeA, indexName, 0);
            assertTrue(isSafeAccessRequired(map));
        }
        var noOfDocs = randomIntBetween(10, 50);
        var bulkResponse = indexDocs(indexName, noOfDocs);
        assertNoFailures(bulkResponse);
        Collection<DocWriteResponse> itemResponses = randomSubsetOf(
            randomIntBetween(1, noOfDocs),
            Arrays.stream(bulkResponse.getItems()).map(BulkItemResponse::<DocWriteResponse>getResponse).collect(Collectors.toSet())
        );
        final var getNonExistingId = randomBoolean();
        var ids = getNonExistingId
            ? List.of("non-existing")
            : itemResponses.stream().map(DocWriteResponse::getId).collect(Collectors.toSet());
        // We need to know how many shards are hit by the mget request to be able to correctly count the expected number of
        // ShardMultiGetFromTranslog requests that the indexing tier receives.
        int shardsTargeted = getNonExistingId
            ? 1
            : itemResponses.stream().map(DocWriteResponse::getShardId).collect(Collectors.toSet()).size();
        CountDownLatch receivedGetFromTranslog = new CountDownLatch(shardsTargeted);
        CountDownLatch continueGetFromTranslog = new CountDownLatch(1);
        AtomicLong getFromTranslogCountOnNodeA = new AtomicLong();
        AtomicLong getFromTranslogCountOnNodeB = new AtomicLong();
        MockTransportService.getInstance(indexNodeA)
            .addRequestHandlingBehavior(TransportShardMultiGetFomTranslogAction.NAME, (handler, request, channel, task) -> {
                getFromTranslogCountOnNodeA.incrementAndGet();
                receivedGetFromTranslog.countDown();
                assertTrue("timed out waiting for relocation", continueGetFromTranslog.await(30, TimeUnit.SECONDS));
                handler.messageReceived(request, channel, task);
            });
        MockTransportService.getInstance(indexNodeB)
            .addRequestHandlingBehavior(TransportShardMultiGetFomTranslogAction.NAME, (handler, request, channel, task) -> {
                getFromTranslogCountOnNodeB.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });
        // Trigger a ShardMultiGetFromTranslog and force shard relocations before indexNodeA gets to process the request.
        var getFuture = client().prepareMultiGet().addIds(indexName, ids).setRealtime(true).execute();
        assertTrue("timed out waiting for mget_from_translog requests", receivedGetFromTranslog.await(30, TimeUnit.SECONDS));
        logger.info("--> starting promotable shard relocation");
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        ensureGreen(indexName);
        logger.info("--> relocation finished");
        continueGetFromTranslog.countDown();
        if (getNonExistingId) {
            assertFalse(getFuture.get(10, TimeUnit.SECONDS).getResponses()[0].getResponse().isExists());
        } else {
            assertTrue(Arrays.stream(getFuture.get(10, TimeUnit.SECONDS).getResponses()).allMatch(r -> r.getResponse().isExists()));
        }
        assertEquals(shardsTargeted, getFromTranslogCountOnNodeA.get());
        assertEquals(shardsTargeted, getFromTranslogCountOnNodeB.get());
    }

    public void testShardMultiGetFromTranslogRetries() throws Exception {
        startMasterOnlyNode();
        var indexNodeA = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        final var shardId = findIndexShard(indexName).shardId();
        final var failCount = randomIntBetween(1, 3);
        final var getFromTranslogSeen = new AtomicInteger();
        MockTransportService.getInstance(indexNodeA)
            .addRequestHandlingBehavior(TransportShardMultiGetFomTranslogAction.NAME, (handler, request, channel, task) -> {
                if (getFromTranslogSeen.incrementAndGet() <= failCount) {
                    channel.sendResponse(randomFrom(new IndexNotFoundException(indexName), new ShardNotFoundException(shardId)));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        final var getFuture = client().prepareMultiGet().addIds(indexName, "non-existing").setRealtime(true).execute();
        // Trigger enough cluster state updates to see the reties succeed.
        for (int i = 0; i < failCount; i++) {
            indicesAdmin().preparePutMapping(indexName).setSource("field" + i, "type=keyword").get();
        }
        assertFalse(getFuture.get(10, TimeUnit.SECONDS).getResponses()[0].getResponse().isExists());
        assertEquals(failCount + 1, getFromTranslogSeen.get());
    }

    private LiveVersionMap getLiveVersionMap(String indexNodeName, String indexName, int shardId) {
        var indicesService = internalCluster().getInstance(IndicesService.class, indexNodeName);
        var indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        var indexShard = indexService.getShard(shardId);
        assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
        var indexEngine = ((IndexEngine) indexShard.getEngineOrNull());
        return indexEngine.getLiveVersionMap();
    }
}
