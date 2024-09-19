/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.ingest.IngestClientIT;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class IncrementalBulkIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(IngestClientIT.ExtendedIngestTestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexingPressure.SPLIT_BULK_THRESHOLD.getKey(), "512B")
            .build();
    }

    public void testSingleBulkRequest() {
        String index = "test";
        createIndex(index);

        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
        IndexRequest indexRequest = indexRequest(index);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        handler.lastItems(List.of(indexRequest), refCounted::decRef, future);

        BulkResponse bulkResponse = future.actionGet();
        assertNoFailures(bulkResponse);

        refresh(index);

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) 1));
        });

        assertFalse(refCounted.hasReferences());
    }

    public void testBufferedResourcesReleasedOnClose() {
        String index = "test";
        createIndex(index);

        String nodeName = internalCluster().getRandomNodeName();
        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, nodeName);
        IndexingPressure indexingPressure = internalCluster().getInstance(IndexingPressure.class, nodeName);

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
        IndexRequest indexRequest = indexRequest(index);

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        handler.addItems(List.of(indexRequest), refCounted::decRef, () -> {});

        assertTrue(refCounted.hasReferences());
        assertThat(indexingPressure.stats().getCurrentCoordinatingBytes(), greaterThan(0L));

        handler.close();

        assertFalse(refCounted.hasReferences());
        assertThat(indexingPressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    public void testIndexingPressureRejection() {
        String index = "test";
        createIndex(index);

        String nodeName = internalCluster().getRandomNodeName();
        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, nodeName);
        IndexingPressure indexingPressure = internalCluster().getInstance(IndexingPressure.class, nodeName);

        try (Releasable r = indexingPressure.markCoordinatingOperationStarted(1, indexingPressure.stats().getMemoryLimit(), true)) {
            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
            AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});

            if (randomBoolean()) {
                AtomicBoolean nextPage = new AtomicBoolean(false);
                refCounted.incRef();
                handler.addItems(List.of(indexRequest(index)), refCounted::decRef, () -> nextPage.set(true));
                assertTrue(nextPage.get());
            }

            PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
            handler.lastItems(List.of(indexRequest(index)), refCounted::decRef, future);

            expectThrows(EsRejectedExecutionException.class, future::actionGet);
            assertFalse(refCounted.hasReferences());
        }
    }

    public void testIncrementalBulkRequestMemoryBackOff() throws Exception {
        String index = "test";
        createIndex(index);

        String nodeName = internalCluster().getRandomNodeName();
        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, nodeName);
        IndexingPressure indexingPressure = internalCluster().getInstance(IndexingPressure.class, nodeName);

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        AtomicBoolean nextPage = new AtomicBoolean(false);

        IndexRequest indexRequest = indexRequest(index);
        long total = indexRequest.ramBytesUsed();
        while (total < 512) {
            refCounted.incRef();
            handler.addItems(List.of(indexRequest), refCounted::decRef, () -> nextPage.set(true));
            assertTrue(nextPage.get());
            nextPage.set(false);
            indexRequest = indexRequest(index);
            total += indexRequest.ramBytesUsed();
        }

        assertThat(indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), greaterThan(0L));
        refCounted.incRef();
        handler.addItems(List.of(indexRequest(index)), refCounted::decRef, () -> nextPage.set(true));

        assertBusy(() -> assertThat(indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), equalTo(0L)));

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        handler.lastItems(List.of(indexRequest), refCounted::decRef, future);

        BulkResponse bulkResponse = future.actionGet();
        assertNoFailures(bulkResponse);
        assertFalse(refCounted.hasReferences());
    }

    public void testMultipleBulkPartsWithBackoff() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        try (Releasable ignored = executorService::shutdown;) {
            String index = "test";
            createIndex(index);

            IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);
            long docs = randomIntBetween(200, 400);

            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();

            BulkResponse bulkResponse = executeBulk(docs, index, handler, executorService);
            assertNoFailures(bulkResponse);

            refresh(index);

            assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(docs));
            });
        }
    }

    public void testGlobalBulkFailure() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        CountDownLatch blockingLatch = new CountDownLatch(1);

        try (Releasable ignored = executorService::shutdown; Releasable ignored2 = blockingLatch::countDown) {
            String index = "test";
            createIndex(index);

            String randomNodeName = internalCluster().getRandomNodeName();
            IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, randomNodeName);
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, randomNodeName);

            int threadCount = threadPool.info(ThreadPool.Names.WRITE).getMax();
            long queueSize = threadPool.info(ThreadPool.Names.WRITE).getQueueSize().singles();
            blockWritePool(threadCount, threadPool, blockingLatch);

            Runnable runnable = () -> {};
            for (int i = 0; i < queueSize; i++) {
                threadPool.executor(ThreadPool.Names.WRITE).execute(runnable);
            }

            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
            if (randomBoolean()) {
                expectThrows(
                    EsRejectedExecutionException.class,
                    () -> executeBulk(randomIntBetween(200, 400), index, handler, executorService)
                );
            } else {
                PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
                AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
                handler.lastItems(List.of(indexRequest(index)), refCounted::decRef, future);
                assertFalse(refCounted.hasReferences());
                expectThrows(EsRejectedExecutionException.class, future::actionGet);
            }
        }
    }

    public void testBulkLevelBulkFailureAfterFirstIncrementalRequest() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        try (Releasable ignored = executorService::shutdown) {
            String index = "test";
            createIndex(index);

            String randomNodeName = internalCluster().getRandomNodeName();
            IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, randomNodeName);
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, randomNodeName);
            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
            AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
            PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();

            int threadCount = threadPool.info(ThreadPool.Names.WRITE).getMax();
            long queueSize = threadPool.info(ThreadPool.Names.WRITE).getQueueSize().singles();

            CountDownLatch blockingLatch1 = new CountDownLatch(1);

            AtomicBoolean nextRequested = new AtomicBoolean(true);
            AtomicLong hits = new AtomicLong(0);
            try (Releasable ignored2 = blockingLatch1::countDown;) {
                blockWritePool(threadCount, threadPool, blockingLatch1);
                while (nextRequested.get()) {
                    nextRequested.set(false);
                    refCounted.incRef();
                    handler.addItems(List.of(indexRequest(index)), refCounted::decRef, () -> nextRequested.set(true));
                    hits.incrementAndGet();
                }
            }
            assertBusy(() -> assertTrue(nextRequested.get()));

            CountDownLatch blockingLatch2 = new CountDownLatch(1);

            try (Releasable ignored3 = blockingLatch2::countDown;) {
                blockWritePool(threadCount, threadPool, blockingLatch2);
                Runnable runnable = () -> {};
                // Fill Queue
                for (int i = 0; i < queueSize; i++) {
                    threadPool.executor(ThreadPool.Names.WRITE).execute(runnable);
                }

                handler.lastItems(List.of(indexRequest(index)), refCounted::decRef, future);
            }

            // Should not throw because some succeeded
            BulkResponse bulkResponse = future.actionGet();

            assertTrue(bulkResponse.hasFailures());
            BulkItemResponse[] items = bulkResponse.getItems();
            assertThat(Arrays.stream(items).filter(r -> r.isFailed() == false).count(), equalTo(hits.get()));
            assertThat(items[items.length - 1].getFailure().getCause(), instanceOf(EsRejectedExecutionException.class));

            refresh(index);

            assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(hits.get()));
            });
        }
    }

    public void testShortCircuitShardLevelFailure() throws Exception {
        String index = "test";
        createIndex(index, 2, 0);

        String coordinatingOnlyNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, coordinatingOnlyNode);
        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();

        AtomicBoolean nextRequested = new AtomicBoolean(true);
        AtomicLong hits = new AtomicLong(0);
        while (nextRequested.get()) {
            nextRequested.set(false);
            refCounted.incRef();
            handler.addItems(List.of(indexRequest(index)), refCounted::decRef, () -> nextRequested.set(true));
            hits.incrementAndGet();
        }

        assertBusy(() -> assertTrue(nextRequested.get()));

        String node = findShard(resolveIndex(index), 0);
        String secondShardNode = findShard(resolveIndex(index), 1);
        IndexingPressure primaryPressure = internalCluster().getInstance(IndexingPressure.class, node);
        long memoryLimit = primaryPressure.stats().getMemoryLimit();
        long primaryRejections = primaryPressure.stats().getPrimaryRejections();
        try (Releasable releasable = primaryPressure.markPrimaryOperationStarted(10, memoryLimit, false)) {
            while (primaryPressure.stats().getPrimaryRejections() == primaryRejections) {
                while (nextRequested.get()) {
                    nextRequested.set(false);
                    refCounted.incRef();
                    List<DocWriteRequest<?>> requests = new ArrayList<>();
                    for (int i = 0; i < 20; ++i) {
                        requests.add(indexRequest(index));
                    }
                    handler.addItems(requests, refCounted::decRef, () -> nextRequested.set(true));
                }
                assertBusy(() -> assertTrue(nextRequested.get()));
            }
        }

        while (nextRequested.get()) {
            nextRequested.set(false);
            refCounted.incRef();
            handler.addItems(List.of(indexRequest(index)), refCounted::decRef, () -> nextRequested.set(true));
        }

        assertBusy(() -> assertTrue(nextRequested.get()));

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        handler.lastItems(List.of(indexRequest(index)), refCounted::decRef, future);

        BulkResponse bulkResponse = future.actionGet();
        assertTrue(bulkResponse.hasFailures());
        for (int i = 0; i < hits.get(); ++i) {
            assertFalse(bulkResponse.getItems()[i].isFailed());
        }

        boolean shardsOnDifferentNodes = node.equals(secondShardNode) == false;
        for (int i = (int) hits.get(); i < bulkResponse.getItems().length; ++i) {
            BulkItemResponse item = bulkResponse.getItems()[i];
            if (item.getResponse() != null && item.getResponse().getShardId().id() == 1 && shardsOnDifferentNodes) {
                assertFalse(item.isFailed());
            } else {
                assertTrue(item.isFailed());
                assertThat(item.getFailure().getCause().getCause(), instanceOf(EsRejectedExecutionException.class));
            }
        }
    }

    public void testShortCircuitShardLevelFailureWithIngestNodeHop() throws Exception {
        String dataOnlyNode = internalCluster().startDataOnlyNode();
        String index = "test1";

        // We ensure that the index is assigned to a non-ingest node to ensure that indexing pressure does not reject at the coordinating
        // level.
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.require._name", dataOnlyNode)
                .build()
        );

        String pipelineId = "pipeline_id";
        BytesReference pipelineSource = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        putJsonPipeline(pipelineId, pipelineSource);

        // By adding an ingest pipeline and sending the request to a coordinating node without the ingest role, we ensure that we are
        // testing the serialization of shard level requests over the wire. This is because the transport bulk action will be dispatched to
        // a node with the ingest role.
        String coordinatingOnlyNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, coordinatingOnlyNode);
        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();

        AtomicBoolean nextRequested = new AtomicBoolean(true);
        AtomicLong hits = new AtomicLong(0);
        while (nextRequested.get()) {
            nextRequested.set(false);
            refCounted.incRef();
            handler.addItems(List.of(indexRequest(index).setPipeline(pipelineId)), refCounted::decRef, () -> nextRequested.set(true));
            hits.incrementAndGet();
        }

        assertBusy(() -> assertTrue(nextRequested.get()));

        String node = findShard(resolveIndex(index), 0);
        assertThat(node, equalTo(dataOnlyNode));
        IndexingPressure primaryPressure = internalCluster().getInstance(IndexingPressure.class, node);
        long memoryLimit = primaryPressure.stats().getMemoryLimit();
        try (Releasable releasable = primaryPressure.markPrimaryOperationStarted(10, memoryLimit, false)) {
            while (nextRequested.get()) {
                nextRequested.set(false);
                refCounted.incRef();
                handler.addItems(List.of(indexRequest(index).setPipeline(pipelineId)), refCounted::decRef, () -> nextRequested.set(true));
            }

            assertBusy(() -> assertTrue(nextRequested.get()));
        }

        while (nextRequested.get()) {
            nextRequested.set(false);
            refCounted.incRef();
            handler.addItems(List.of(indexRequest(index).setPipeline(pipelineId)), refCounted::decRef, () -> nextRequested.set(true));
        }

        assertBusy(() -> assertTrue(nextRequested.get()));

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        handler.lastItems(List.of(indexRequest(index)), refCounted::decRef, future);

        BulkResponse bulkResponse = future.actionGet();
        assertTrue(bulkResponse.hasFailures());
        for (int i = 0; i < hits.get(); ++i) {
            assertFalse(bulkResponse.getItems()[i].isFailed());
        }

        for (int i = (int) hits.get(); i < bulkResponse.getItems().length; ++i) {
            BulkItemResponse item = bulkResponse.getItems()[i];
            assertTrue(item.isFailed());
            assertThat(item.getFailure().getCause().getCause(), instanceOf(EsRejectedExecutionException.class));
        }
    }

    private static void blockWritePool(int threadCount, ThreadPool threadPool, CountDownLatch blockingLatch) throws InterruptedException {
        CountDownLatch startedLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(() -> {
                startedLatch.countDown();
                try {
                    blockingLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        startedLatch.await();
    }

    private BulkResponse executeBulk(long docs, String index, IncrementalBulkService.Handler handler, ExecutorService executorService) {
        ConcurrentLinkedQueue<DocWriteRequest<?>> queue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < docs; i++) {
            IndexRequest indexRequest = indexRequest(index);
            queue.add(indexRequest);
        }

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        Runnable r = new Runnable() {

            @Override
            public void run() {
                int toRemove = Math.min(randomIntBetween(5, 10), queue.size());
                ArrayList<DocWriteRequest<?>> docs = new ArrayList<>();
                for (int i = 0; i < toRemove; i++) {
                    docs.add(queue.poll());
                }

                if (queue.isEmpty()) {
                    handler.lastItems(docs, refCounted::decRef, future);
                } else {
                    refCounted.incRef();
                    handler.addItems(docs, refCounted::decRef, () -> executorService.execute(this));
                }
            }
        };

        executorService.execute(r);

        BulkResponse bulkResponse = future.actionGet();
        assertFalse(refCounted.hasReferences());
        return bulkResponse;
    }

    private static IndexRequest indexRequest(String index) {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.source(Map.of("field", randomAlphaOfLength(10)));
        return indexRequest;
    }

    protected static String findShard(Index index, int shardId) {
        for (String node : internalCluster().getNodeNames()) {
            var indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                IndexShard shard = indexService.getShardOrNull(shardId);
                if (shard != null && shard.isActive() && shard.routingEntry().primary()) {
                    return node;
                }
            }
        }
        throw new AssertionError("IndexShard instance not found for shard " + new ShardId(index, shardId));
    }
}
