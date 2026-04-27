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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchPhaseDocsIterator.IterateResult;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FetchPhaseDocsIteratorTests extends ESTestCase {

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

    public void testIterateAsyncNullOrEmptyDocIds() throws Exception {
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        StreamingFetchPhaseDocsIterator it = createStreamingIterator();

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
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
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

    public void testFetchPhaseMaxInFlightChunksSettingIsReadCorrectly() {
        Settings customSettings = Settings.builder().put(SearchService.FETCH_PHASE_MAX_IN_FLIGHT_CHUNKS.getKey(), 7).build();
        assertThat(SearchService.FETCH_PHASE_MAX_IN_FLIGHT_CHUNKS.get(customSettings), equalTo(7));
        assertThat(SearchService.FETCH_PHASE_MAX_IN_FLIGHT_CHUNKS.get(Settings.EMPTY), equalTo(3));
    }

    public void testIterateAsyncSingleDocument() throws Exception {
        LuceneDocs docs = createDocs(1, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createStreamingIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            new int[] { 0 },
            chunkWriter,
            1024,
            refs,
            4,
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        // Single doc becomes the last chunk
        assertThat(result.hits, nullValue());
        assertThat(result.lastChunkBytes, notNullValue());
        assertThat(result.lastChunkHitCount, equalTo(1));

        // No intermediate chunks sent
        assertThat(chunkWriter.getSentChunks().size(), equalTo(0));

        // Pages for the last chunk are reserved on the CB
        assertThat(circuitBreaker.getUsed(), greaterThan(0L));

        result.close();
        assertThat(circuitBreaker.getUsed(), equalTo(0L));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncAllDocsInSingleChunk() throws Exception {
        LuceneDocs docs = createDocs(5, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createStreamingIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            1024 * 1024,  // Large chunk size
            refs,
            4,
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
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
        LuceneDocs docs = createDocs(100, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createStreamingIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,  // Small chunk size to force multiple chunks
            refs,
            4,
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        // Verify chunks are in order by from index
        List<SentChunkInfo> chunks = chunkWriter.getSentChunks();
        long expectedSequenceStart = 0L;
        for (SentChunkInfo chunk : chunks) {
            assertThat(chunk.sequenceStart, equalTo(expectedSequenceStart));
            expectedSequenceStart += chunk.hitCount;
        }
        assertThat(result.lastChunkSequenceStart, equalTo(expectedSequenceStart));

        // Should have multiple chunks sent + last chunk held back
        assertThat(chunkWriter.getSentChunks().size(), greaterThan(0));
        assertThat(result.lastChunkBytes, notNullValue());

        // Total hits across all chunks should equal docCount
        int totalHits = chunkWriter.getSentChunks().stream().mapToInt(c -> c.hitCount).sum() + result.lastChunkHitCount;
        assertThat(totalHits, equalTo(100));

        // Only last chunk's pages should remain reserved
        assertThat(circuitBreaker.getUsed(), greaterThan(0L));

        result.close();
        assertThat(circuitBreaker.getUsed(), equalTo(0L));

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncRespectsMaxInFlightWithDelayedAcks() throws Exception {
        for (int maxInFlightChunks : List.of(1, 2, 3)) {
            LuceneDocs docs = createDocs(100, false);
            CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
            InFlightTrackingChunkWriter chunkWriter = new InFlightTrackingChunkWriter(circuitBreaker);
            AtomicReference<Throwable> sendFailure = new AtomicReference<>();
            AtomicBoolean cancelled = new AtomicBoolean(false);

            PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
            CountDownLatch refsComplete = new CountDownLatch(1);
            RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

            createStreamingIterator().iterateAsync(
                createShardTarget(),
                docs.reader,
                docs.docIds,
                chunkWriter,
                1,
                refs,
                maxInFlightChunks,
                sendFailure,
                cancelled::get,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                future
            );

            assertThat(chunkWriter.getSentChunks().size(), equalTo(maxInFlightChunks));
            assertThat(chunkWriter.getMaxInFlight(), equalTo(maxInFlightChunks));
            assertFalse(future.isDone());

            assertBusy(() -> {
                chunkWriter.ackAll();
                assertTrue(future.isDone());
            }, 10, TimeUnit.SECONDS);

            IterateResult result = future.get(10, TimeUnit.SECONDS);

            refs.close();
            assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

            assertThat(chunkWriter.getMaxInFlight(), lessThanOrEqualTo(maxInFlightChunks));
            assertThat(result.lastChunkBytes, notNullValue());

            int totalHits = chunkWriter.getSentChunks().stream().mapToInt(c -> c.hitCount).sum() + result.lastChunkHitCount;
            assertThat(totalHits, equalTo(100));

            result.close();
            assertThat(circuitBreaker.getUsed(), equalTo(0L));

            docs.reader.close();
            docs.directory.close();
        }
    }

    public void testIterateAsyncCircuitBreakerTrips() throws Exception {
        LuceneDocs docs = createDocs(100, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(100L));
        TestChunkWriter chunkWriter = new TestChunkWriter(true, circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createStreamingIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
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
        LuceneDocs docs = createDocs(100, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(true);  // Already cancelled

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createStreamingIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
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
        LuceneDocs docs = createDocs(1000, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        // Iterator that cancels after processing some docs
        AtomicInteger processedDocs = new AtomicInteger(0);
        StreamingFetchPhaseDocsIterator it = new StreamingFetchPhaseDocsIterator() {
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
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
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
        LuceneDocs docs = createDocs(100, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        // Iterator that throws after processing some docs
        StreamingFetchPhaseDocsIterator it = new StreamingFetchPhaseDocsIterator() {
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
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
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
        LuceneDocs docs = createDocs(100, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>(new IOException("Pre-existing failure")); // Send Failure
        AtomicBoolean cancelled = new AtomicBoolean(false);

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        createStreamingIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
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
        LuceneDocs docs = createDocs(100, false);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        // Chunk writer that fails after first chunk
        AtomicInteger chunkCount = new AtomicInteger(0);
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker) {
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

        createStreamingIterator().iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50,
            refs,
            4,
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
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

    public void testIterateAsyncVisitsLeavesInDocIdOrder() throws Exception {
        int docCount = 200;
        Directory directory = newDirectory();
        // Use plain IndexWriter (not RandomIndexWriter) so that segments are produced exactly at our
        // explicit commit() points. RandomIndexWriter performs random extra flushes that, combined
        // with NoMergePolicy, can produce many tiny single-doc segments and cause the assertion
        // "at least one leaf should have multiple docs" to flake.
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
        for (int i = 0; i < docCount; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "value" + i, Field.Store.NO));
            writer.addDocument(doc);
            if (i % 30 == 0) {
                writer.commit();
            }
        }
        writer.commit();
        IndexReader reader = DirectoryReader.open(writer);
        writer.close();

        assertTrue("Need multiple leaves to test leaf ordering", reader.leaves().size() > 1);

        int[] docIds = new int[] { 150, 10, 180, 50, 30, 120, 70, 160, 90, 140 };

        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        List<Integer> setNextReaderLeafOrdinals = new CopyOnWriteArrayList<>();
        List<int[]> setNextReaderDocsInLeaf = new CopyOnWriteArrayList<>();
        List<Integer> nextDocCalls = new CopyOnWriteArrayList<>();

        StreamingFetchPhaseDocsIterator it = new StreamingFetchPhaseDocsIterator() {
            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {
                setNextReaderLeafOrdinals.add(ctx.ord);
                setNextReaderDocsInLeaf.add(docsInLeaf.clone());
            }

            @Override
            protected SearchHit nextDoc(int doc) {
                nextDocCalls.add(doc);
                return new SearchHit(doc);
            }
        };

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        it.iterateAsync(
            createShardTarget(),
            reader,
            docIds,
            chunkWriter,
            1024 * 1024,
            refs,
            4,
            sendFailure,
            cancelled::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        for (int i = 1; i < setNextReaderLeafOrdinals.size(); i++) {
            assertThat(
                "Leaf ordinals must increase: " + setNextReaderLeafOrdinals,
                setNextReaderLeafOrdinals.get(i),
                greaterThan(setNextReaderLeafOrdinals.get(i - 1))
            );
        }

        boolean anyMultiDoc = setNextReaderDocsInLeaf.stream().anyMatch(arr -> arr.length > 1);
        assertTrue("At least one leaf should have multiple docs", anyMultiDoc);

        for (int i = 1; i < nextDocCalls.size(); i++) {
            assertThat("nextDoc calls must be in doc-ID order", nextDocCalls.get(i), greaterThan(nextDocCalls.get(i - 1)));
        }

        assertThat(result.lastChunkBytes, notNullValue());
        assertThat(result.lastChunkHitCount, equalTo(docIds.length));

        try (var in = result.lastChunkBytes.streamInput()) {
            int[] positions = new int[result.lastChunkHitCount];
            for (int i = 0; i < result.lastChunkHitCount; i++) {
                positions[i] = in.readVInt();
                SearchHit hit = SearchHit.readFrom(in, false);
                hit.decRef();
            }

            boolean[] seen = new boolean[docIds.length];
            for (int pos : positions) {
                assertThat("Position must be in range", pos, greaterThanOrEqualTo(0));
                assertThat("Position must be in range", pos, lessThan(docIds.length));
                assertFalse("Each position must be unique", seen[pos]);
                seen[pos] = true;
            }
        }

        result.close();
        assertThat(circuitBreaker.getUsed(), equalTo(0L));

        reader.close();
        directory.close();
    }

    public void testTimeoutReturnsCompactPartialResults() throws IOException {
        int docCount = 400;
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

        ContextIndexSearcher searcher = new ContextIndexSearcher(reader, null, null, TrivialQueryCachingPolicy.NEVER, randomBoolean());

        // deliberately unsorted doc ids so that the doc-id-sorted iteration order
        // differs from the original order
        int[] docs = new int[] { 250, 10, 150, 50, 300, 100, 200, 350 };
        // in doc-id order: 10, 50, 100, 150, 200, ... timeout at doc 200
        final int timeoutAfterDocId = 200;

        FetchPhaseDocsIterator it = new FetchPhaseDocsIterator() {
            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {}

            @Override
            protected SearchHit nextDoc(int doc) {
                if (doc == timeoutAfterDocId) {
                    searcher.throwTimeExceededException();
                }
                return new SearchHit(doc);
            }
        };

        IterateResult result = it.iterate(null, reader, docs, true, new QuerySearchResult());

        // the returned array is compact — no null entries, shorter than input
        assertThat(result.hits.length, greaterThan(0));
        assertThat(result.hits.length, lessThan(docs.length));
        for (SearchHit hit : result.hits) {
            assertNotNull(hit);
            assertThat(hit.docId(), greaterThanOrEqualTo(0));
            hit.decRef();
        }

        reader.close();
        directory.close();
    }

    public void testSameLeafSameThreadReusesLeafSetup() throws Exception {
        LuceneDocs docs = createDocs(100, true);
        assertThat("need one leaf", docs.reader.leaves().size(), equalTo(1));

        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        RecordingStreamingIterator it = new RecordingStreamingIterator();

        PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
        CountDownLatch refsComplete = new CountDownLatch(1);
        RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

        it.iterateAsync(
            createShardTarget(),
            docs.reader,
            docs.docIds,
            chunkWriter,
            50, // tiny chunk size -> many chunks
            refs,
            4,
            new AtomicReference<>(),
            new AtomicBoolean(false)::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        assertThat("producer should have emitted multiple chunks", chunkWriter.getSentChunks().size(), greaterThan(1));
        assertThat(
            "setNextReader must be called exactly once when all chunks stay on one thread within a single leaf",
            it.setupCount(),
            equalTo(1)
        );
        assertThat("leaf setup must be recorded from a single thread", it.distinctThreads().size(), equalTo(1));

        int totalHits = chunkWriter.getSentChunks().stream().mapToInt(c -> c.hitCount).sum() + result.lastChunkHitCount;
        assertThat(totalHits, equalTo(docs.docIds.length));

        result.close();
        assertThat(circuitBreaker.getUsed(), equalTo(0L));

        docs.reader.close();
        docs.directory.close();
    }

    public void testCrossLeafOnSameThreadRebuilds() throws Exception {
        LuceneDocs docs = createDocs(200, false);
        assertThat("need multiple leaves to exercise cross-leaf rebuild", docs.reader.leaves().size(), greaterThan(1));

        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        TestChunkWriter chunkWriter = new TestChunkWriter(circuitBreaker);
        RecordingStreamingIterator it = new RecordingStreamingIterator();

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
            new AtomicReference<>(),
            new AtomicBoolean(false)::get,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            future
        );

        IterateResult result = future.get(10, TimeUnit.SECONDS);
        refs.close();
        assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

        int expectedLeaves = docs.reader.leaves().size();
        assertThat(
            "setNextReader must be called exactly once per leaf when all chunks stay on one thread",
            it.setupCount(),
            equalTo(expectedLeaves)
        );
        List<Integer> ordinals = it.leafOrdinals();
        for (int i = 1; i < ordinals.size(); i++) {
            assertThat("leaf ordinals must strictly increase: " + ordinals, ordinals.get(i), greaterThan(ordinals.get(i - 1)));
        }
        assertThat("all cross-leaf rebuilds stay on a single thread here", it.distinctThreads().size(), equalTo(1));

        int totalHits = chunkWriter.getSentChunks().stream().mapToInt(c -> c.hitCount).sum() + result.lastChunkHitCount;
        assertThat(totalHits, equalTo(docs.docIds.length));

        result.close();
        assertThat(circuitBreaker.getUsed(), equalTo(0L));

        docs.reader.close();
        docs.directory.close();
    }

    public void testCrossThreadWithinSameLeafRebuildsSetup() throws Exception {
        LuceneDocs docs = createDocs(150, true);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        InFlightTrackingChunkWriter chunkWriter = new InFlightTrackingChunkWriter(circuitBreaker);
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        RecordingStreamingIterator it = new RecordingStreamingIterator();

        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
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
                1, // every ACK dispatches a continuation on the executor
                sendFailure,
                new AtomicBoolean(false)::get,
                threadPool.generic(),
                future
            );

            Thread testThread = Thread.currentThread();
            // Drain all chunks
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (future.isDone() == false && System.nanoTime() < deadlineNanos) {
                chunkWriter.ackAll();
                Thread.yield();
            }

            IterateResult result = future.get(10, TimeUnit.SECONDS);
            refs.close();
            assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

            assertThat("no send failure", sendFailure.get(), nullValue());

            Set<String> threadsSeen = it.distinctThreads();
            assertThat("leaf setup must have been recorded on at least two distinct threads", threadsSeen.size(), greaterThanOrEqualTo(2));
            assertTrue("first leaf setup must come from the caller thread", threadsSeen.contains(testThread.getName()));
            assertThat(
                "every recorded setup must come from the thread running that produceNext",
                it.setupCount(),
                greaterThanOrEqualTo(threadsSeen.size())
            );

            int totalHits = chunkWriter.getSentChunks().stream().mapToInt(c -> c.hitCount).sum() + result.lastChunkHitCount;
            assertThat(totalHits, equalTo(docs.docIds.length));
            assertThat(result.lastChunkBytes, notNullValue());

            result.close();
            assertThat(circuitBreaker.getUsed(), equalTo(0L));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }

        docs.reader.close();
        docs.directory.close();
    }

    public void testIterateAsyncConcurrentLeafSetupStress() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        Executor continuationExecutor = threadPool.generic();
        try {
            int iterations = 20;
            for (int i = 0; i < iterations; i++) {
                final int iter = i;
                int docCount = randomIntBetween(100, 500);
                boolean singleLeaf = randomBoolean();
                LuceneDocs docs = singleLeaf ? createDocs(docCount, true) : createDocs(docCount, false);
                try {
                    int maxInFlight = randomIntBetween(1, 4);
                    int chunkBytes = randomFrom(1, 20, 50, 200);

                    final CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
                    InFlightTrackingChunkWriter chunkWriter = new InFlightTrackingChunkWriter(circuitBreaker);
                    AtomicReference<Throwable> sendFailure = new AtomicReference<>();
                    RecordingStreamingIterator it = new RecordingStreamingIterator();

                    PlainActionFuture<IterateResult> future = new PlainActionFuture<>();
                    CountDownLatch refsComplete = new CountDownLatch(1);
                    RefCountingListener refs = new RefCountingListener(ActionListener.running(refsComplete::countDown));

                    it.iterateAsync(
                        createShardTarget(),
                        docs.reader,
                        docs.docIds,
                        chunkWriter,
                        chunkBytes,
                        refs,
                        maxInFlight,
                        sendFailure,
                        new AtomicBoolean(false)::get,
                        continuationExecutor,
                        future
                    );

                    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (future.isDone() == false && System.nanoTime() < deadlineNanos) {
                        if (randomBoolean()) {
                            chunkWriter.ackAll();
                        } else {
                            threadPool.generic().execute(chunkWriter::ackAll);
                        }
                        Thread.yield();
                    }

                    IterateResult result = future.get(10, TimeUnit.SECONDS);
                    refs.close();
                    assertTrue(refsComplete.await(10, TimeUnit.SECONDS));

                    assertThat("no send failure on iter=" + iter, sendFailure.get(), nullValue());
                    int totalHits = chunkWriter.getSentChunks().stream().mapToInt(c -> c.hitCount).sum() + result.lastChunkHitCount;
                    assertThat("hit count matches on iter=" + iter, totalHits, equalTo(docs.docIds.length));

                    result.close();
                    assertBusy(() -> assertThat("circuit breaker released on iter=" + iter, circuitBreaker.getUsed(), equalTo(0L)));
                } finally {
                    docs.reader.close();
                    docs.directory.close();
                }
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
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

    private static StreamingFetchPhaseDocsIterator createStreamingIterator() {
        return new StreamingFetchPhaseDocsIterator() {
            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {}

            @Override
            protected SearchHit nextDoc(int doc) {
                return new SearchHit(doc);
            }
        };
    }

    private LuceneDocs createDocs(int numDocs, boolean singleLeaf) throws IOException {
        Directory directory = newDirectory();
        IndexReader reader;
        if (singleLeaf) {
            RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("field", "value" + i, Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();
            reader = writer.getReader();
            writer.close();
        } else {
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("field", "value" + i, Field.Store.NO));
                writer.addDocument(doc);
                if (i % 30 == 0) {
                    writer.commit();
                }
            }
            writer.commit();
            reader = DirectoryReader.open(writer);
            writer.close();
        }

        int[] docIds = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docIds[i] = i;
        }
        return new LuceneDocs(directory, reader, docIds);
    }

    private record LuceneDocs(Directory directory, IndexReader reader, int[] docIds) {}

    private static class RecordingStreamingIterator extends StreamingFetchPhaseDocsIterator {
        private final List<SetupEvent> setups = new CopyOnWriteArrayList<>();

        @Override
        protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {
            setups.add(new SetupEvent(Thread.currentThread().getName(), ctx.ord, docsInLeaf.clone()));
        }

        @Override
        protected SearchHit nextDoc(int doc) {
            return new SearchHit(doc);
        }

        int setupCount() {
            return setups.size();
        }

        Set<String> distinctThreads() {
            Set<String> names = new HashSet<>();
            for (SetupEvent e : setups) {
                names.add(e.threadName);
            }
            return names;
        }

        List<Integer> leafOrdinals() {
            List<Integer> ords = new ArrayList<>(setups.size());
            for (SetupEvent e : setups) {
                ords.add(e.leafOrd);
            }
            return ords;
        }

        private record SetupEvent(String threadName, int leafOrd, int[] docsInLeaf) {}
    }

    /**
     * Simple record to track sent chunk info
     */
    private record SentChunkInfo(int hitCount, long sequenceStart, int expectedTotalDocs) {}

    private static class TestChunkWriter implements FetchPhaseResponseChunk.Writer {

        protected final List<SentChunkInfo> sentChunks = new CopyOnWriteArrayList<>();
        private final List<ActionListener<Void>> pendingAcks = new CopyOnWriteArrayList<>();
        private final boolean delayAcks;
        private final CircuitBreaker circuitBreaker;

        private final PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);

        TestChunkWriter(CircuitBreaker circuitBreaker) {
            this(false, circuitBreaker);
        }

        TestChunkWriter(boolean delayAcks, CircuitBreaker circuitBreaker) {
            this.delayAcks = delayAcks;
            this.circuitBreaker = circuitBreaker;
        }

        @Override
        public void writeResponseChunk(FetchPhaseResponseChunk chunk, ActionListener<Void> listener) {
            sentChunks.add(new SentChunkInfo(chunk.hitCount(), chunk.sequenceStart(), chunk.expectedTotalDocs()));
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
            return new RecyclerBytesStreamOutput(new BytesRefRecycler(recycler), circuitBreaker);
        }

        public List<SentChunkInfo> getSentChunks() {
            return sentChunks;
        }
    }

    private static class InFlightTrackingChunkWriter extends TestChunkWriter {
        private final ConcurrentLinkedQueue<ActionListener<Void>> pendingAcks = new ConcurrentLinkedQueue<>();
        private final AtomicInteger inFlightChunks = new AtomicInteger();
        private final AtomicInteger maxInFlightChunks = new AtomicInteger();

        InFlightTrackingChunkWriter(CircuitBreaker circuitBreaker) {
            super(true, circuitBreaker);
        }

        @Override
        public void writeResponseChunk(FetchPhaseResponseChunk chunk, ActionListener<Void> listener) {
            sentChunks.add(new SentChunkInfo(chunk.hitCount(), chunk.sequenceStart(), chunk.expectedTotalDocs()));
            int currentInFlight = inFlightChunks.incrementAndGet();
            maxInFlightChunks.accumulateAndGet(currentInFlight, Math::max);
            pendingAcks.add(ActionListener.wrap(v -> {
                inFlightChunks.decrementAndGet();
                listener.onResponse(v);
            }, e -> {
                inFlightChunks.decrementAndGet();
                listener.onFailure(e);
            }));
        }

        @Override
        public void ackAll() {
            ActionListener<Void> ack;
            while ((ack = pendingAcks.poll()) != null) {
                ack.onResponse(null);
            }
        }

        int getMaxInFlight() {
            return maxInFlightChunks.get();
        }
    }
}
