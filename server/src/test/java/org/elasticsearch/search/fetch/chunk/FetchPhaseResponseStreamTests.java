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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
        TestCircuitBreaker breaker = new TestCircuitBreaker("test", Long.MAX_VALUE);
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
        TestCircuitBreaker breaker = new TestCircuitBreaker("test", Long.MAX_VALUE);
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
        TestCircuitBreaker breaker = new TestCircuitBreaker("test", chunkSize - 1);
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
        TestCircuitBreaker breaker = new TestCircuitBreaker("test", limit);
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
        TestCircuitBreaker breaker = new TestCircuitBreaker("test", Long.MAX_VALUE);
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

    public void testHitsIncRefOnWrite() throws IOException {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, breaker);

        try {
            FetchPhaseResponseChunk chunk = createChunk(0, 5, 0);
            writeChunk(stream, chunk);

            FetchSearchResult result = buildFinalResult(stream);

            try {
                // Hits should still have references after writeChunk
                for (SearchHit hit : result.hits().getHits()) {
                    assertTrue("Hit should have references", hit.hasReferences());
                }
            } finally {
                result.decRef();
            }
        } finally {
            stream.decRef();
        }
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

            stream.writeChunk(createChunk(0, 5, 0), releasable);

            assertTrue("Releasable should be closed after successful write", releasableClosed.get());
        } finally {
            stream.decRef();
        }
    }

    public void testReleasableNotClosedOnFailure() throws IOException {
        FetchPhaseResponseChunk testChunk = createChunkWithSourceSize(0, 5, 0, 10000);
        long chunkSize = testChunk.getBytesLength();

        // Set limit smaller than chunk size to guarantee trip
        TestCircuitBreaker breaker = new TestCircuitBreaker("test", chunkSize / 2);
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(SHARD_INDEX, 5, breaker);

        try {
            AtomicBoolean releasableClosed = new AtomicBoolean(false);
            Releasable releasable = () -> releasableClosed.set(true);

            expectThrows(
                CircuitBreakingException.class,
                () -> { stream.writeChunk(createChunkWithSourceSize(0, 5, 0, 10000), releasable); }
            );

            assertFalse("Releasable should not be closed on failure", releasableClosed.get());
        } finally {
            stream.decRef();
        }
    }

    public void testChunkMetadata() throws IOException {
        long timestamp = System.currentTimeMillis();
        SearchHit hit = createHit(0);
        try {
            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(
                timestamp,
                FetchPhaseResponseChunk.Type.HITS,
                TEST_SHARD_ID,
                serializeHits(hit),
                1,
                0,
                10,
                0
            );

            assertThat(chunk.type(), equalTo(FetchPhaseResponseChunk.Type.HITS));
            assertThat(chunk.shardId(), equalTo(TEST_SHARD_ID));
            assertThat(chunk.hitCount(), equalTo(1));
            assertThat(chunk.from(), equalTo(0));
            assertThat(chunk.expectedDocs(), equalTo(10));
            assertThat(chunk.sequenceStart(), equalTo(0L));
            assertThat(chunk.getBytesLength(), greaterThan(0L));

            chunk.close();
        } finally {
            hit.decRef();
        }
    }

    public void testChunkInvalidShardId() {
        ShardId invalidShardId = new ShardId(new Index("test", "uuid"), -2);

        expectThrows(
            IllegalArgumentException.class,
            () -> new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                invalidShardId,
                BytesArray.EMPTY,
                0,
                0,
                0,
                0
            )
        );
    }

    private FetchSearchResult buildFinalResult(FetchPhaseResponseStream stream) {
        return stream.buildFinalResult(new ShardSearchContextId("test", 1), new SearchShardTarget("node1", TEST_SHARD_ID, null), null);
    }

    /**
     * Extracts the "id" field from a hit's source JSON.
     */
    private int getIdFromSource(SearchHit hit) {
        String source = hit.getSourceAsString();
        int start = source.indexOf("\"id\":") + 5;
        int end = source.indexOf(",", start);
        if (end == -1) {
            end = source.indexOf("}", start);
        }
        return Integer.parseInt(source.substring(start, end));
    }

    private FetchPhaseResponseChunk createChunk(int startId, int hitCount, long sequenceStart) throws IOException {
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            hits[i] = createHit(startId + i);
        }
        try {
            return new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                TEST_SHARD_ID,
                serializeHits(hits),
                hitCount,
                startId,
                100,
                sequenceStart
            );
        } finally {
            for (SearchHit hit : hits) {
                hit.decRef();
            }
        }
    }

    private FetchPhaseResponseChunk createChunkWithSequence(int startId, int hitCount, long sequenceStart) throws IOException {
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            hits[i] = createHit(startId + i);
        }
        try {
            return new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                TEST_SHARD_ID,
                serializeHits(hits),
                hitCount,
                startId,
                100,
                sequenceStart
            );
        } finally {
            for (SearchHit hit : hits) {
                hit.decRef();
            }
        }
    }

    private FetchPhaseResponseChunk createChunkWithSourceSize(int startId, int hitCount, long sequenceStart, int sourceSize)
        throws IOException {
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            hits[i] = createHitWithSourceSize(startId + i, sourceSize);
        }
        try {
            return new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                TEST_SHARD_ID,
                serializeHits(hits),
                hitCount,
                startId,
                100,
                sequenceStart
            );
        } finally {
            for (SearchHit hit : hits) {
                hit.decRef();
            }
        }
    }

    private FetchPhaseResponseChunk createChunkWithScores(int startId, float[] scores, long sequenceStart) throws IOException {
        SearchHit[] hits = new SearchHit[scores.length];
        for (int i = 0; i < scores.length; i++) {
            hits[i] = createHitWithScore(startId + i, scores[i]);
        }
        try {
            return new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                TEST_SHARD_ID,
                serializeHits(hits),
                scores.length,
                startId,
                100,
                sequenceStart
            );
        } finally {
            for (SearchHit hit : hits) {
                hit.decRef();
            }
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

    private BytesReference serializeHits(SearchHit... hits) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            for (SearchHit hit : hits) {
                hit.writeTo(out);
            }
            return out.bytes();
        }
    }

    private void writeChunk(FetchPhaseResponseStream stream, FetchPhaseResponseChunk chunk) throws IOException {
        stream.writeChunk(chunk, () -> {});
    }

    private static class TestCircuitBreaker implements CircuitBreaker {
        private final String name;
        private final long limit;
        private final AtomicLong used = new AtomicLong(0);
        private final AtomicLong tripped = new AtomicLong(0);

        TestCircuitBreaker(String name, long limit) {
            this.name = name;
            this.limit = limit;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            tripped.incrementAndGet();
            throw new CircuitBreakingException(
                "Data too large, data for [" + fieldName + "] would be [" + bytesNeeded + "] which exceeds limit of [" + limit + "]",
                bytesNeeded,
                limit,
                Durability.TRANSIENT
            );
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            long newUsed = used.addAndGet(bytes);
            if (newUsed > limit) {
                used.addAndGet(-bytes);
                circuitBreak(label, newUsed);
            }
            // return newUsed;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }

        @Override
        public long getLimit() {
            return limit;
        }

        @Override
        public double getOverhead() {
            return 1.0;
        }

        @Override
        public long getTrippedCount() {
            return tripped.get();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            // Not implemented for test
        }
    }
}
