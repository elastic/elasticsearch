/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Unit tests for {@link FetchPhaseResponseStream}.
 */
public class FetchPhaseResponseStreamTests extends ESTestCase {

    private static final int SHARD_INDEX = 0;
    private static final ShardId TEST_SHARD_ID = new ShardId(new Index("test-index", "test-uuid"), 0);

    public void testEmptyStream() {
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 0, new NoopCircuitBreaker("test"));
        try {
            FetchSearchResult result = buildFinalResult(stream);
            try {
                assertThat(result.hits().getHits().length, equalTo(0));
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testSingleHit() throws IOException {
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 1, new NoopCircuitBreaker("test"));

        try {
            writeChunk(stream, createChunk(0, 1, 0));

            FetchSearchResult result = buildFinalResult(stream);

            try {
                SearchHit[] hits = result.hits().getHits();
                assertThat(hits.length, equalTo(1));
                assertThat(getIdFromSource(hits[0]), equalTo(0));
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testChunksArriveInOrder() throws IOException {
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 15, new NoopCircuitBreaker("test"));

        try {
            // Send 3 chunks in order: sequence 0-4, 5-9, 10-14
            writeChunk(stream, createChunk(0, 5, 0));   // hits 0-4, sequence starts at 0
            writeChunk(stream, createChunk(5, 5, 5));   // hits 5-9, sequence starts at 5
            writeChunk(stream, createChunk(10, 5, 10)); // hits 10-14, sequence starts at 10

            FetchSearchResult result = buildFinalResult(stream);

            try {
                SearchHit[] hits = result.hits().getHits();
                assertThat(hits.length, equalTo(15));

                for (int i = 0; i < 15; i++) {
                    assertThat("Hit at position " + i + " should have correct id in source", getIdFromSource(hits[i]), equalTo(i));
                }
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testChunksArriveRandomOrder() throws IOException {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        int numChunks = 10;
        int hitsPerChunk = 5;
        int totalHits = numChunks * hitsPerChunk;

        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, totalHits, breaker);

        try {
            // Create chunks and shuffle them
            List<FetchPhaseResponseChunk> chunks = new ArrayList<>();
            for (int i = 0; i < numChunks; i++) {
                int startId = i * hitsPerChunk;
                long sequenceStart = i * hitsPerChunk;
                chunks.add(createChunk(startId, hitsPerChunk, sequenceStart));
            }
            Collections.shuffle(chunks, random());

            // Write in shuffled order
            for (FetchPhaseResponseChunk chunk : chunks) {
                writeChunk(stream, chunk);
            }

            FetchSearchResult result = buildFinalResult(stream);

            try {
                SearchHit[] hits = result.hits().getHits();
                assertThat(hits.length, equalTo(totalHits));

                for (int i = 0; i < totalHits; i++) {
                    assertThat("Hit at position " + i + " should have correct id in source", getIdFromSource(hits[i]), equalTo(i));
                }
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testAddHitWithSequence() {
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, new NoopCircuitBreaker("test"));

        try {
            stream.addHitWithSequence(createHit(3), 3);
            stream.addHitWithSequence(createHit(1), 1);
            stream.addHitWithSequence(createHit(4), 4);
            stream.addHitWithSequence(createHit(0), 0);
            stream.addHitWithSequence(createHit(2), 2);

            FetchSearchResult result = buildFinalResult(stream);

            try {
                SearchHit[] hits = result.hits().getHits();
                assertThat(hits.length, equalTo(5));

                for (int i = 0; i < 5; i++) {
                    assertThat(getIdFromSource(hits[i]), equalTo(i));
                }
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testMixedChunkAndSingleHitAddition() throws IOException {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 10, breaker);

        try {
            // Add a chunk (sequence 0-4)
            writeChunk(stream, createChunk(0, 5, 0));

            // Add individual hits for sequence 5-9 in random order
            stream.addHitWithSequence(createHit(7), 7);
            stream.addHitWithSequence(createHit(5), 5);
            stream.addHitWithSequence(createHit(9), 9);
            stream.addHitWithSequence(createHit(6), 6);
            stream.addHitWithSequence(createHit(8), 8);

            FetchSearchResult result = buildFinalResult(stream);

            try {
                SearchHit[] hits = result.hits().getHits();
                assertThat(hits.length, equalTo(10));

                for (int i = 0; i < 10; i++) {
                    assertThat(getIdFromSource(hits[i]), equalTo(i));
                }
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testNonContiguousSequenceNumbers() throws IOException {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 6, breaker);

        try {
            // Chunks with gaps in sequence
            writeChunk(stream, createChunkWithSequence(0, 2, 0));   // id 0,1 -> seq 0, 1
            writeChunk(stream, createChunkWithSequence(2, 2, 10));  // id 2,3 -> seq 10, 11
            writeChunk(stream, createChunkWithSequence(4, 2, 5));   // id 4,5 -> seq 5, 6

            FetchSearchResult result = buildFinalResult(stream);

            try {
                SearchHit[] hits = result.hits().getHits();
                assertThat(hits.length, equalTo(6));

                // source ids: 0, 1, 4, 5, 2, 3
                assertThat(getIdFromSource(hits[0]), equalTo(0)); // seq 0
                assertThat(getIdFromSource(hits[1]), equalTo(1)); // seq 1
                assertThat(getIdFromSource(hits[2]), equalTo(4)); // seq 5
                assertThat(getIdFromSource(hits[3]), equalTo(5)); // seq 6
                assertThat(getIdFromSource(hits[4]), equalTo(2)); // seq 10
                assertThat(getIdFromSource(hits[5]), equalTo(3)); // seq 11
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    // ==================== Circuit Breaker Tests ====================

    public void testCircuitBreakerBytesTracked() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 10, breaker);

        try {
            long bytesBefore = breaker.getUsed();
            assertThat(bytesBefore, equalTo(0L));

            FetchPhaseResponseChunk chunk1 = createChunkWithSourceSize(0, 5, 0, 1024);
            long chunk1Bytes = chunk1.getBytesLength();
            writeChunk(stream, chunk1);

            long bytesAfterChunk1 = breaker.getUsed();
            assertThat("Circuit breaker should track chunk1 bytes", bytesAfterChunk1, equalTo(chunk1Bytes));

            FetchPhaseResponseChunk chunk2 = createChunkWithSourceSize(5, 5, 5, 1024);
            long chunk2Bytes = chunk2.getBytesLength();
            writeChunk(stream, chunk2);

            long bytesAfterChunk2 = breaker.getUsed();
            assertThat("Circuit breaker should track both chunks' bytes", bytesAfterChunk2, equalTo(chunk1Bytes + chunk2Bytes));
        } finally {
            stream.decRef();
        }
    }

    public void testCircuitBreakerBytesReleasedOnClose() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 10, breaker);

        FetchPhaseResponseChunk chunk1 = createChunkWithSourceSize(0, 5, 0, 1024);
        FetchPhaseResponseChunk chunk2 = createChunkWithSourceSize(5, 5, 5, 1024);
        long expectedBytes = chunk1.getBytesLength() + chunk2.getBytesLength();

        writeChunk(stream, chunk1);
        writeChunk(stream, chunk2);

        long bytesBeforeClose = breaker.getUsed();
        assertThat("Should have bytes tracked", bytesBeforeClose, equalTo(expectedBytes));

        stream.decRef();

        long bytesAfterClose = breaker.getUsed();
        assertThat("All breaker bytes should be released", bytesAfterClose, equalTo(0L));
    }

    public void testCircuitBreakerTrips() throws IOException {
        FetchPhaseResponseChunk testChunk = createChunkWithSourceSize(0, 5, 0, 2048);
        long chunkSize = testChunk.getBytesLength();

        // Set limit smaller than chunk size
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(chunkSize - 1));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 10, breaker);

        try {
            FetchPhaseResponseChunk chunk = createChunkWithSourceSize(0, 5, 0, 2048);
            expectThrows(CircuitBreakingException.class, () -> writeChunk(stream, chunk));
        } finally {
            stream.decRef();
        }
    }

    public void testCircuitBreakerTripsOnSecondChunk() throws IOException {
        FetchPhaseResponseChunk chunk1 = createChunkWithSourceSize(0, 5, 0, 1024);
        FetchPhaseResponseChunk chunk2 = createChunkWithSourceSize(5, 5, 5, 1024);
        long chunk1Size = chunk1.getBytesLength();
        long chunk2Size = chunk2.getBytesLength();

        // Set limit to allow first chunk but not second
        long limit = chunk1Size + (chunk2Size / 2);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(limit));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 10, breaker);

        try {
            writeChunk(stream, createChunkWithSourceSize(0, 5, 0, 1024));
            assertThat("First chunk should be tracked", breaker.getUsed(), greaterThan(0L));

            expectThrows(CircuitBreakingException.class, () -> { writeChunk(stream, createChunkWithSourceSize(5, 5, 5, 1024)); });
        } finally {
            stream.decRef();
        }
    }

    public void testCircuitBreakerReleasedOnCloseWithoutBuildingResult() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 10, breaker);

        // Write chunks but don't call buildFinalResult
        writeChunk(stream, createChunkWithSourceSize(0, 5, 0, 1024));
        writeChunk(stream, createChunkWithSourceSize(5, 5, 5, 1024));

        long bytesBeforeClose = breaker.getUsed();
        assertThat("Should have bytes tracked", bytesBeforeClose, greaterThan(0L));

        stream.decRef();

        assertThat("All breaker bytes should be released", breaker.getUsed(), equalTo(0L));
    }

    // ==================== Reference Counting Tests ====================

    public void testHitOwnershipTransferredToQueueOnWrite() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, breaker);

        FetchPhaseResponseChunk chunk = createChunk(0, 5, 0);
        try {
            stream.writeChunk(chunk, () -> {});

            for (SearchHit hit : chunk.getHits()) {
                assertNull("Chunk should have released its reference to the hit after consumeHits", hit);
            }
        } finally {
            chunk.close();
        }

        FetchSearchResult result = buildFinalResult(stream);
        for (SearchHit hit : result.hits().getHits()) {
            assertTrue("Hit should have references", hit.hasReferences());
        }
        result.decRef();

        stream.decRef();
        assertThat("Breaker bytes should be released after stream close", breaker.getUsed(), equalTo(0L));
    }

    public void testHitsReleasedWhenStreamClosedWithoutBuildFinalResult() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, breaker);

        FetchPhaseResponseChunk chunk = createChunk(0, 5, 0);
        try {
            stream.writeChunk(chunk, () -> {});
        } finally {
            chunk.close();
        }

        assertThat("Breaker should account for the accumulated chunk bytes", breaker.getUsed(), greaterThan(0L));

        stream.decRef();

        assertThat("All breaker bytes should be released", breaker.getUsed(), equalTo(0L));
    }

    // ==================== Score Handling Tests ====================

    public void testMaxScoreCalculation() throws IOException {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, breaker);

        try {
            float[] scores = { 1.5f, 3.2f, 2.1f, 4.8f, 0.9f };
            FetchPhaseResponseChunk chunk = createChunkWithScores(0, scores, 0);
            writeChunk(stream, chunk);

            FetchSearchResult result = buildFinalResult(stream);

            try {
                assertThat(result.hits().getMaxScore(), equalTo(4.8f));
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testMaxScoreWithNaN() throws IOException {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 3, breaker);

        try {
            float[] scores = { Float.NaN, Float.NaN, Float.NaN };
            FetchPhaseResponseChunk chunk = createChunkWithScores(0, scores, 0);
            writeChunk(stream, chunk);

            FetchSearchResult result = stream.buildFinalResult(
                new ShardSearchContextId("test", 1),
                new SearchShardTarget("node1", TEST_SHARD_ID, null),
                null
            );

            try {
                assertTrue(Float.isNaN(result.hits().getMaxScore()));
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testMaxScoreWithMixedNaNAndValid() throws IOException {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 4, breaker);

        try {
            float[] scores = { Float.NaN, 2.5f, Float.NaN, 1.8f };
            FetchPhaseResponseChunk chunk = createChunkWithScores(0, scores, 0);
            writeChunk(stream, chunk);

            FetchSearchResult result = buildFinalResult(stream);

            try {
                assertThat(result.hits().getMaxScore(), equalTo(2.5f));
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    /**
     * Test concurrent chunk writes from multiple threads.
     * Verifies thread-safety of the ConcurrentLinkedQueue usage.
     * Simulates shards
     */
    public void testConcurrentChunkWrites() throws Exception {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        int numThreads = 10;
        int hitsPerThread = 10;
        int totalHits = numThreads * hitsPerThread;

        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, totalHits, breaker);

        try {
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            CountDownLatch done = new CountDownLatch(numThreads);
            AtomicBoolean error = new AtomicBoolean(false);

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                new Thread(() -> {
                    try {
                        barrier.await();
                        // Each thread writes its own chunk with distinct sequence range
                        int startId = threadId * hitsPerThread;
                        long sequenceStart = threadId * hitsPerThread;
                        FetchPhaseResponseChunk chunk = createChunk(startId, hitsPerThread, sequenceStart);
                        writeChunk(stream, chunk);
                    } catch (Exception e) {
                        error.set(true);
                    } finally {
                        done.countDown();
                    }
                }).start();
            }

            assertTrue("All threads should complete", done.await(10, TimeUnit.SECONDS));
            assertFalse("No errors should occur", error.get());

            FetchSearchResult result = buildFinalResult(stream);

            try {
                SearchHit[] hits = result.hits().getHits();
                assertThat(hits.length, equalTo(totalHits));

                for (int i = 0; i < totalHits; i++) {
                    assertThat("Hit at position " + i + " should have correct id in source", getIdFromSource(hits[i]), equalTo(i));
                }
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testReleasableClosedOnSuccess() throws IOException {
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, new NoopCircuitBreaker("test"));

        try {
            AtomicBoolean releasableClosed = new AtomicBoolean(false);
            Releasable releasable = () -> releasableClosed.set(true);

            FetchPhaseResponseChunk chunk = createChunk(0, 5, 0);
            try {
                stream.writeChunk(chunk, releasable);
            } finally {
                chunk.close();
            }

            assertTrue("Releasable should be closed after successful write", releasableClosed.get());
        } finally {
            stream.decRef();
        }
    }

    public void testReleasableNotClosedOnFailure() throws IOException {
        FetchPhaseResponseChunk testChunk = createChunkWithSourceSize(0, 5, 0, 10000);
        long chunkSize = testChunk.getBytesLength();

        // Set limit smaller than chunk size to guarantee trip
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(chunkSize / 2));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, breaker);

        try {
            AtomicBoolean releasableClosed = new AtomicBoolean(false);
            Releasable releasable = () -> releasableClosed.set(true);

            FetchPhaseResponseChunk chunk = createChunkWithSourceSize(0, 5, 0, 10000);
            try {
                expectThrows(CircuitBreakingException.class, () -> stream.writeChunk(chunk, releasable));
            } finally {
                chunk.close();
            }

            assertFalse("Releasable should not be closed on failure", releasableClosed.get());
        } finally {
            stream.decRef();
        }
    }

    public void testWriteChunkWithCircuitBreakerTripPreservesAccountingAndPropagates() throws IOException {
        FetchPhaseResponseChunk chunk = createChunkWithSourceSize(0, 5, 0, 4096);
        long chunkSize = chunk.getBytesLength();

        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(chunkSize - 1));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, breaker);
        AtomicBoolean releasableClosed = new AtomicBoolean(false);

        try {
            try {
                expectThrows(CircuitBreakingException.class, () -> stream.writeChunk(chunk, () -> releasableClosed.set(true)));
            } finally {
                chunk.close();
            }

            assertFalse("Releasable should not be closed on failure", releasableClosed.get());
            assertThat("No bytes should be tracked on breaker trip", breaker.getUsed(), equalTo(0L));

            FetchSearchResult result = buildFinalResult(stream);
            try {
                assertThat("No hits should be accumulated after breaker trip", result.hits().getHits().length, equalTo(0));
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
            assertThat("No breaker bytes should remain after close", breaker.getUsed(), equalTo(0L));
        }
    }

    public void testConcurrentWriteChunkAndBuildFinalResultNoHitLeaks() throws Exception {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        int numThreads = 8;
        int hitsPerThread = 8;
        int totalHits = numThreads * hitsPerThread;

        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, totalHits, breaker);
        CountDownLatch startSignal = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        SearchHit[] resultHits = null;
        FetchSearchResult result = null;
        try {
            List<CompletableFuture<Void>> writerFutures = IntStream.range(0, numThreads)
                .mapToObj(threadId -> CompletableFuture.runAsync(() -> {
                    try {
                        assertTrue("Writer should be released to start", startSignal.await(5, TimeUnit.SECONDS));
                        int startId = threadId * hitsPerThread;
                        long sequenceStart = threadId * hitsPerThread;
                        FetchPhaseResponseChunk chunk = createChunk(startId, hitsPerThread, sequenceStart);
                        try {
                            writeChunk(stream, chunk);
                        } finally {
                            chunk.close();
                        }
                    } catch (Exception e) {
                        throw new AssertionError("Writer failed", e);
                    }
                }, executor))
                .toList();
            startSignal.countDown();

            CompletableFuture.allOf(writerFutures.toArray(new CompletableFuture<?>[0])).get(10, TimeUnit.SECONDS);

            result = buildFinalResult(stream);
            resultHits = result.hits().getHits().clone();
            assertThat(resultHits.length, equalTo(totalHits));

            for (int i = 0; i < totalHits; i++) {
                assertThat(getIdFromSource(resultHits[i]), equalTo(i));
            }
        } finally {
            executor.shutdown();
            assertTrue("Executor should terminate", executor.awaitTermination(10, TimeUnit.SECONDS));
            if (result != null) {
                result.decRef();
            }
            stream.decRef();
        }

        assertNotNull(resultHits);
        assertThat("All breaker bytes should be released after stream close", breaker.getUsed(), equalTo(0L));
    }

    public void testChunkMetadata() throws IOException {
        SearchHit hit = createHit(0);
        try {
            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(new SearchHit[] { hit }, 0), 1, 10, 0);

            assertThat(chunk.shardId(), equalTo(TEST_SHARD_ID));
            assertThat(chunk.hitCount(), equalTo(1));
            assertThat(chunk.expectedTotalDocs(), equalTo(10));
            assertThat(chunk.sequenceStart(), equalTo(0L));
            assertThat(chunk.getBytesLength(), greaterThan(0L));

            chunk.close();
        } finally {
            hit.decRef();
        }
    }

    private FetchSearchResult buildFinalResult(FetchPhaseResponseStream stream) {
        return stream.buildFinalResult(new ShardSearchContextId("test", 1), new SearchShardTarget("node1", TEST_SHARD_ID, null), null);
    }

    /**
     * Extracts the "id" field from a hit's source JSON.
     */
    private int getIdFromSource(SearchHit hit) {
        Number id = (Number) XContentHelper.convertToMap(hit.getSourceRef(), false, XContentType.JSON).v2().get("id");
        return id.intValue();
    }

    private FetchPhaseResponseChunk createChunk(int startId, int hitCount, long sequenceStart) throws IOException {
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            hits[i] = createHit(startId + i);
        }
        try {
            return new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(hits, sequenceStart), hitCount, 100, sequenceStart);
        } finally {
            decRefSearchHits(hits);
        }
    }

    private FetchPhaseResponseChunk createChunkWithSequence(int startId, int hitCount, long sequenceStart) throws IOException {
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            hits[i] = createHit(startId + i);
        }
        try {
            return new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(hits, sequenceStart), hitCount, 100, sequenceStart);
        } finally {
            decRefSearchHits(hits);
        }
    }

    private FetchPhaseResponseChunk createChunkWithSourceSize(int startId, int hitCount, long sequenceStart, int sourceSize)
        throws IOException {
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            hits[i] = createHitWithSourceSize(startId + i, sourceSize);
        }
        try {
            return new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(hits, sequenceStart), hitCount, 100, sequenceStart);
        } finally {
            decRefSearchHits(hits);
        }
    }

    private FetchPhaseResponseChunk createChunkWithScores(int startId, float[] scores, long sequenceStart) throws IOException {
        SearchHit[] hits = new SearchHit[scores.length];
        for (int i = 0; i < scores.length; i++) {
            hits[i] = createHitWithScore(startId + i, scores[i]);
        }
        try {
            return new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(hits, sequenceStart), scores.length, 100, sequenceStart);
        } finally {
            decRefSearchHits(hits);
        }
    }

    private void decRefSearchHits(SearchHit[] hits) {
        for (SearchHit hit : hits) {
            hit.decRef();
        }
    }

    private SearchHit createHit(int id) {
        SearchHit hit = new SearchHit(id);
        hit.sourceRef(new BytesArray("{\"id\":" + id + "}"));
        return hit;
    }

    private SearchHit createHitWithSourceSize(int id, int sourceSize) {
        SearchHit hit = new SearchHit(id);
        StringBuilder sb = new StringBuilder();
        sb.append("{\"id\":").append(id).append(",\"data\":\"");
        int dataSize = Math.max(0, sourceSize - 20);
        for (int i = 0; i < dataSize; i++) {
            sb.append('x');
        }
        sb.append("\"}");
        hit.sourceRef(new BytesArray(sb.toString()));
        return hit;
    }

    private SearchHit createHitWithScore(int id, float score) {
        SearchHit hit = new SearchHit(id);
        hit.sourceRef(new BytesArray("{\"id\":" + id + "}"));
        hit.score(score);
        return hit;
    }

    private BytesReference serializeHits(SearchHit[] hits, long sequenceStart) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            for (int i = 0; i < hits.length; i++) {
                out.writeVInt((int) (sequenceStart + i));
                hits[i].writeTo(out);
            }
            return out.bytes();
        }
    }

    private void writeChunk(FetchPhaseResponseStream stream, FetchPhaseResponseChunk chunk) throws IOException {
        try {
            stream.writeChunk(chunk, () -> {});
        } finally {
            chunk.close();
        }
    }

}
