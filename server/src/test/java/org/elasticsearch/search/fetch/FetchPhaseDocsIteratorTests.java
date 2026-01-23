/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchPhaseDocsIterator.IterateResult;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FetchPhaseDocsIteratorTests extends ESTestCase {

    // ==================== Synchronous iterate() tests ====================

    public void testInOrderIteration() throws IOException {
        int docCount = random().nextInt(300) + 100;
        Directory directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
        for (int i = 0; i < docCount; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "foo", Field.Store.NO));
            writer.addDocument(doc);
            if (i % 50 == 0) {
                writer.commit();
            }
        }
        writer.commit();
        IndexReader reader = writer.getReader();
        writer.close();

        int[] docs = randomDocIds(docCount - 1);
        FetchPhaseDocsIterator it = new FetchPhaseDocsIterator() {

            LeafReaderContext ctx = null;
            int[] docsInLeaf = null;
            int index = 0;

            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {
                this.ctx = ctx;
                this.docsInLeaf = docsInLeaf;
                for (int i = 0; i < docsInLeaf.length; i++) {
                    if (i > 0) {
                        assertThat(docsInLeaf[i], greaterThan(docsInLeaf[i - 1]));
                    }
                    assertThat(docsInLeaf[i], lessThan(ctx.reader().maxDoc()));
                }
                this.index = 0;
            }

            @Override
            protected SearchHit nextDoc(int doc) {
                assertThat(doc, equalTo(this.docsInLeaf[this.index] + this.ctx.docBase));
                index++;
                return new SearchHit(doc);
            }
        };

        SearchHit[] hits = it.iterate(null, reader, docs, randomBoolean(), new QuerySearchResult()).hits;

        assertThat(hits.length, equalTo(docs.length));
        for (int i = 0; i < hits.length; i++) {
            assertThat(hits[i].docId(), equalTo(docs[i]));
            hits[i].decRef();
        }

        reader.close();
        directory.close();
    }

    public void testExceptions() throws IOException {
        int docCount = randomIntBetween(300, 400);
        Directory directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
        for (int i = 0; i < docCount; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "foo", Field.Store.NO));
            writer.addDocument(doc);
            if (i % 50 == 0) {
                writer.commit();
            }
        }
        writer.commit();
        IndexReader reader = writer.getReader();
        writer.close();

        int[] docs = randomDocIds(docCount - 1);
        int badDoc = docs[randomInt(docs.length - 1)];

        FetchPhaseDocsIterator it = new FetchPhaseDocsIterator() {
            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {}

            @Override
            protected SearchHit nextDoc(int doc) {
                if (doc == badDoc) {
                    throw new IllegalArgumentException("Error processing doc");
                }
                return new SearchHit(doc);
            }
        };

        Exception e = expectThrows(
            FetchPhaseExecutionException.class,
            () -> it.iterate(null, reader, docs, randomBoolean(), new QuerySearchResult())
        );
        assertThat(e.getMessage(), containsString("Error running fetch phase for doc [" + badDoc + "]"));
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));

        reader.close();
        directory.close();
    }

    // ==================== Asynchronous iterateAsync() tests ====================

    public void testIterateAsyncNullOrEmptyDocIds() throws Exception {
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        TestChunkWriter chunkWriter = new TestChunkWriter();
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        FetchPhaseDocsIterator it = createIterator();

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        it.iterateAsync(
            createShardTarget(),
            null,
            randomBoolean() ? null : new int[0],
            chunkWriter,
            1024,
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        assertThat(result.hits, notNullValue());
        assertThat(result.hits.length, equalTo(0));
        assertThat(result.lastChunkBytes, nullValue());
        assertThat(circuitBreaker.getUsed(), equalTo(0L));
        result.close();
    }

    public void testIterateAsyncSingleDocument() throws Exception {
        LuceneDocs docs = createDocs(1);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        TestChunkWriter chunkWriter = new TestChunkWriter();
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            new int[] { 0 },
            chunkWriter,
            1024,
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        // Single doc becomes the last chunk
        assertThat(result.hits, nullValue());
        assertThat(result.lastChunkBytes, notNullValue());
        assertThat(result.lastChunkHitCount, equalTo(1));
        assertThat(result.lastChunkByteSize, greaterThan(0L));

        // No intermediate chunks sent
        assertThat(chunkWriter.getSentChunks().size(), equalTo(0));

        // Circuit breaker has the last chunk reserved
        assertThat(circuitBreaker.getUsed(), equalTo(result.lastChunkByteSize));

        result.close();
        assertThat(circuitBreaker.getUsed(), equalTo(0L));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncAllDocsInSingleChunk() throws Exception {
        LuceneDocs docs = createDocs(5);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        TestChunkWriter chunkWriter = new TestChunkWriter();
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            1024 * 1024,  // Large chunk size
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        // No intermediate chunks sent - all in last chunk
        assertThat(chunkWriter.getSentChunks().size(), equalTo(0));
        assertThat(result.lastChunkBytes, notNullValue());
        assertThat(result.lastChunkHitCount, equalTo(5));

        result.close();
        assertThat(circuitBreaker.getUsed(), equalTo(0L));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncMultipleChunks() throws Exception {
        LuceneDocs docs = createDocs(100);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        TestChunkWriter chunkWriter = new TestChunkWriter();
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,  // Small chunk size to force multiple chunks
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        // Verify chunks are in order by from index
        List<SentChunkInfo> chunks = chunkWriter.getSentChunks();
        int expectedFrom = 0;
        for (SentChunkInfo chunk : chunks) {
            assertThat(chunk.from, equalTo(expectedFrom));
            expectedFrom += chunk.hitCount;
        }
        assertThat(result.lastChunkSequenceStart, equalTo((long) expectedFrom));

        // Should have multiple chunks sent + last chunk held back
        assertThat(chunkWriter.getSentChunks().size(), greaterThan(0));
        assertThat(result.lastChunkBytes, notNullValue());

        // Total hits across all chunks should equal docCount
        int totalHits = chunkWriter.getSentChunks().stream().mapToInt(c -> c.hitCount).sum() + result.lastChunkHitCount;
        assertThat(totalHits, equalTo(100));

        // Only last chunk's bytes should be reserved
        assertThat(circuitBreaker.getUsed(), equalTo(result.lastChunkByteSize));

        result.close();
        // Last chunk's bytes released after the listener (future for the test) is closed
        assertThat(circuitBreaker.getUsed(), equalTo(0L));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncCircuitBreakerTrips() throws Exception {
        LuceneDocs docs = createDocs(100);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker(100);
        TestChunkWriter chunkWriter = new TestChunkWriter(true);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );
        chunkWriter.ackAll();

        Exception e = expectThrows(Exception.class, () -> future.get(10, TimeUnit.SECONDS));
        Throwable actual = e instanceof ExecutionException ? e.getCause() : e;
        assertThat(actual, instanceOf(CircuitBreakingException.class));

        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncCancellationBeforeFetchStart() throws Exception {
        LuceneDocs docs = createDocs(100);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        TestChunkWriter chunkWriter = new TestChunkWriter();
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(true);  // Already cancelled

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        Exception e = expectThrows(Exception.class, () -> future.get(10, TimeUnit.SECONDS));
        assertTrue(
            "Expected cancellation but got: " + e,
            e.getCause() instanceof TaskCancelledException || e.getMessage().contains("cancelled")
        );

        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncCancellationDuringDocProduction() throws Exception {
        LuceneDocs docs = createDocs(1000);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        TestChunkWriter chunkWriter = new TestChunkWriter();
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        // Iterator that cancels after processing some docs
        AtomicInteger processedDocs = new AtomicInteger(0);
        FetchPhaseDocsIterator it = new FetchPhaseDocsIterator() {
            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {}

            @Override
            protected SearchHit nextDoc(int doc) {
                if (processedDocs.incrementAndGet() == 100) {
                    cancelled.set(true);
                }
                return new SearchHit(doc);
            }
        };

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        it.iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        Exception e = expectThrows(Exception.class, () -> future.get(10, TimeUnit.SECONDS));
        assertTrue(
            "Expected TaskCancelledException but got: " + e,
            e.getCause() instanceof TaskCancelledException || e.getMessage().contains("cancelled")
        );

        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncDocProducerException() throws Exception {
        LuceneDocs docs = createDocs(100);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        TestChunkWriter chunkWriter = new TestChunkWriter();
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        // Iterator that throws after processing some docs
        FetchPhaseDocsIterator it = new FetchPhaseDocsIterator() {
            private int count = 0;

            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {}

            @Override
            protected SearchHit nextDoc(int doc) {
                if (++count > 50) {
                    throw new RuntimeException("Simulated producer failure");
                }
                return new SearchHit(doc);
            }
        };

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        it.iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        Exception e = expectThrows(Exception.class, () -> future.get(10, TimeUnit.SECONDS));
        assertThat(e.getCause().getMessage(), containsString("Simulated producer failure"));

        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncPreExistingSendFailure() throws Exception {
        LuceneDocs docs = createDocs(100);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        TestChunkWriter chunkWriter = new TestChunkWriter();
        AtomicReference<Throwable> sendFailure = new AtomicReference<>(new IOException("Pre-existing failure")); // Send Failure
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        Exception e = expectThrows(Exception.class, () -> future.get(10, TimeUnit.SECONDS));
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertThat(e.getCause().getMessage(), containsString("Pre-existing failure"));

        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncSendFailure() throws Exception {
        LuceneDocs docs = createDocs(100);
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        // Chunk writer that fails after first chunk
        AtomicInteger chunkCount = new AtomicInteger(0);
        TestChunkWriter chunkWriter = new TestChunkWriter() {
            @Override
            public void writeResponseChunk(FetchPhaseResponseChunk chunk, ActionListener<Void> listener) {
                if (chunkCount.incrementAndGet() > 1) {
                    chunk.close();
                    listener.onFailure(new IOException("Simulated send failure"));
                } else {
                    super.writeResponseChunk(chunk, listener);
                }
            }
        };
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            circuitBreaker,
            sendFailure,
            cancelled::get,
            future
        );

        Exception e = expectThrows(Exception.class, () -> future.get(10, TimeUnit.SECONDS));
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertThat(e.getCause().getMessage(), containsString("Simulated send failure"));

        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));

        docs.reader.close();
        docs.directory.close();
    }

    private static int[] randomDocIds(int maxDoc) {
        List<Integer> integers = new ArrayList<>();
        int v = 0;
        for (int i = 0; i < 10; i++) {
            v = v + randomInt(maxDoc / 15) + 1;
            if (v >= maxDoc) {
                break;
            }
            integers.add(v);
        }
        Collections.shuffle(integers, random());
        return integers.stream().mapToInt(i -> i).toArray();
    }

    private static SearchShardTarget createShardTarget() {
        return new SearchShardTarget("node1", new ShardId(new Index("test", "uuid"), 0), null);
    }

    private static FetchPhaseDocsIterator createIterator() {
        return new FetchPhaseDocsIterator() {
            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {}

            @Override
            protected SearchHit nextDoc(int doc) {
                return new SearchHit(doc);
            }
        };
    }

    private LuceneDocs createDocs(int numDocs) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "value" + i, Field.Store.NO));
            writer.addDocument(doc);
            if (i % 30 == 0) {
                writer.commit();  // Create multiple segments
            }
        }
        writer.commit();
        IndexReader reader = writer.getReader();
        writer.close();

        int[] docIds = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docIds[i] = i;
        }

        return new LuceneDocs(directory, reader, docIds);
    }

    private record LuceneDocs(Directory directory, IndexReader reader, int[] docIds) {}

    /**
     * Simple record to track sent chunk info
     */
    private record SentChunkInfo(int hitCount, int from, int expectedDocs) {}

    /**
     * Test circuit breaker that tracks memory usage.
     */
    private static class TestCircuitBreaker implements CircuitBreaker {
        private final AtomicLong used = new AtomicLong(0);
        private final long limit;

        TestCircuitBreaker() {
            this(Long.MAX_VALUE);
        }

        TestCircuitBreaker(long limit) {
            this.limit = limit;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            throw new CircuitBreakingException("Circuit breaker tripped", bytesNeeded, limit, Durability.TRANSIENT);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            long newUsed = used.addAndGet(bytes);
            if (newUsed > limit) {
                used.addAndGet(-bytes);
                throw new CircuitBreakingException(
                    "Circuit breaker [" + label + "] tripped, used=" + newUsed + ", limit=" + limit,
                    bytes,
                    limit,
                    Durability.TRANSIENT
                );
            }
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
            return 0;
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {}
    }

    private static class TestChunkWriter implements FetchPhaseResponseChunk.Writer {

        // This is for testing, to track chunks sent over the network
        protected final List<SentChunkInfo> sentChunks = new CopyOnWriteArrayList<>();
        private final List<ActionListener<Void>> pendingAcks = new CopyOnWriteArrayList<>();
        private final boolean delayAcks;

        private final PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);

        TestChunkWriter() {
            this(false);
        }

        TestChunkWriter(boolean delayAcks) {
            this.delayAcks = delayAcks;
        }

        @Override
        public void writeResponseChunk(FetchPhaseResponseChunk chunk, ActionListener<Void> listener) {
            sentChunks.add(new SentChunkInfo(chunk.hitCount(), chunk.from(), chunk.expectedDocs()));
            if (delayAcks) {
                pendingAcks.add(listener);
            } else {
                listener.onResponse(null);
            }
        }

        public void ackAll() {
            for (ActionListener<Void> ack : pendingAcks) {
                ack.onResponse(null);
            }
            pendingAcks.clear();
        }

        @Override
        public RecyclerBytesStreamOutput newNetworkBytesStream() {
            return new RecyclerBytesStreamOutput(new BytesRefRecycler(recycler));
        }

        public List<SentChunkInfo> getSentChunks() {
            return sentChunks;
        }
    }
}
