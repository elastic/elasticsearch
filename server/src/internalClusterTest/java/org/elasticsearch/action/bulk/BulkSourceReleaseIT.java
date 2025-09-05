/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.ingest.IngestClientIT;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class BulkSourceReleaseIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(IngestClientIT.ExtendedIngestTestPlugin.class);
    }

    public void testBulkSourceReleaseWhenIngestReplacesSource() throws Exception {
        String index = "test1";
        createIndex(index);

        String pipelineId = "pipeline_id";
        putPipeline(pipelineId);

        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);

        ReleasableBytesReference originalBytes = new ReleasableBytesReference(new BytesArray("{\"field\": \"value\"}"), () -> {});

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.sourceContext().source(originalBytes, XContentType.JSON);
        indexRequest.setPipeline(pipelineId);

        CountDownLatch blockLatch = new CountDownLatch(1);
        blockWritePool(internalCluster().getDataNodeInstance(ThreadPool.class), blockLatch);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();

        try {
            handler.lastItems(List.of(indexRequest), future);
            assertBusy(() -> assertFalse(originalBytes.hasReferences()));
        } finally {
            blockLatch.countDown();
        }

        BulkResponse bulkResponse = safeGet(future);
        assertNoFailures(bulkResponse);
    }

    public void testBytesReferencedByTwoSourcesNotReleasedIfOnlyOneIngestPipeline() throws Exception {
        String index = "test1";
        createIndex(index);

        String pipelineId = "pipeline_id";
        putPipeline(pipelineId);

        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);

        ReleasableBytesReference originalBytes = new ReleasableBytesReference(
            new BytesArray("{\"field\": \"value1\"}{\"field\": \"value2\"}"),
            () -> {}
        );
        int splitPoint = originalBytes.indexOf((byte) '}', 0) + 1;

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.sourceContext().source(originalBytes.retainedSlice(0, splitPoint), XContentType.JSON);
        indexRequest.setPipeline(pipelineId);

        IndexRequest indexRequestNoIngest = new IndexRequest();
        indexRequestNoIngest.index(index);
        indexRequestNoIngest.sourceContext()
            .source(originalBytes.retainedSlice(splitPoint, originalBytes.length() - splitPoint), XContentType.JSON);

        originalBytes.decRef();
        assertTrue(originalBytes.hasReferences());

        CountDownLatch blockLatch = new CountDownLatch(1);
        blockWritePool(internalCluster().getDataNodeInstance(ThreadPool.class), blockLatch);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        try {
            handler.lastItems(List.of(indexRequest, indexRequestNoIngest), future);

            // Pause briefly to allow bytes to theoretically be released after ingest processing
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));

            assertTrue(originalBytes.hasReferences());
        } finally {
            blockLatch.countDown();
        }

        blockLatch.countDown();

        BulkResponse bulkResponse = safeGet(future);
        assertNoFailures(bulkResponse);
    }

    public void testSomeReferencesCanBeReleasedWhileOthersRetained() throws Exception {
        String index = "test1";
        createIndex(index);

        String pipelineId = "pipeline_id";
        putPipeline(pipelineId);

        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);

        ReleasableBytesReference releasedBytes = new ReleasableBytesReference(new BytesArray("{\"field\": \"value1\"}"), () -> {});
        ReleasableBytesReference retainedBytes = new ReleasableBytesReference(
            new BytesArray("{\"field\": \"value2\"}{\"field\": \"value3\"}"),
            () -> {}
        );
        int splitPoint = retainedBytes.indexOf((byte) '}', 0) + 1;

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
        IndexRequest indexRequest1 = new IndexRequest();
        indexRequest1.index(index);
        indexRequest1.sourceContext().source(releasedBytes, XContentType.JSON);
        indexRequest1.setPipeline(pipelineId);

        IndexRequest indexRequest2 = new IndexRequest();
        indexRequest2.index(index);
        indexRequest2.sourceContext().source(retainedBytes.retainedSlice(0, splitPoint), XContentType.JSON);
        indexRequest2.setPipeline(pipelineId);

        IndexRequest indexRequestNoIngest = new IndexRequest();
        indexRequestNoIngest.index(index);
        indexRequestNoIngest.sourceContext()
            .source(retainedBytes.retainedSlice(splitPoint, retainedBytes.length() - splitPoint), XContentType.JSON);

        retainedBytes.decRef();
        assertTrue(retainedBytes.hasReferences());

        CountDownLatch blockLatch = new CountDownLatch(1);
        blockWritePool(internalCluster().getDataNodeInstance(ThreadPool.class), blockLatch);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        try {
            handler.lastItems(List.of(indexRequest2, indexRequest1, indexRequestNoIngest), future);

            assertBusy(() -> assertFalse(releasedBytes.hasReferences()));

            assertTrue(retainedBytes.hasReferences());
        } finally {
            blockLatch.countDown();
        }

        blockLatch.countDown();

        BulkResponse bulkResponse = safeGet(future);
        assertNoFailures(bulkResponse);
    }

    private static void putPipeline(String pipelineId) throws IOException {
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
    }

    private static void blockWritePool(ThreadPool threadPool, CountDownLatch finishLatch) {
        final var threadCount = threadPool.info(ThreadPool.Names.WRITE).getMax();
        final var startBarrier = new CyclicBarrier(threadCount + 1);
        final var blockingTask = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            protected void doRun() {
                safeAwait(startBarrier);
                safeAwait(finishLatch);
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        };
        for (int i = 0; i < threadCount; i++) {
            threadPool.executor(ThreadPool.Names.WRITE_COORDINATION).execute(blockingTask);
        }
        safeAwait(startBarrier);
    }
}
