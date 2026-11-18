/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ZeroBytesReference;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Feeds batches of hits to the check the way the fetch phase does. Random bytes don't compress, so their stored size is larger than
 * their heap size once written as base64. Zeros compress to almost nothing.
 */
public class StoredResponseSizeCheckTests extends ESTestCase {

    private static final long LIMIT = 32 * 1024;
    private static final int HITS_PER_BATCH = 4;
    // Each batch is over the limit by itself, so every batch passes the next multiple of the limit and is sampled.
    private static final int SOURCE_SIZE = 10 * 1024;

    public void testHitsUnderTheLimitPass() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> hits = hits(1, SOURCE_SIZE, true);
        check.onHits(hits, heapBytes(hits));
        check.onHits(hits, heapBytes(hits));
    }

    public void testLimitOfZeroOrLessIsLeftToTheStore() {
        long limit = randomFrom(0L, -1L);
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(hits, heapBytes(hits));
    }

    /**
     * Stands in for an empty chunk that arrives after a multiple went unsampled because another caller held the sampler. Charging
     * the limit with no hits reaches that state without a second thread.
     */
    public void testCallWithoutHitsLeavesTheSampleToTheNext() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        check.onHits(List.of(), LIMIT);
        List<SearchHit> incompressible = hits(1, SOURCE_SIZE, true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(incompressible, heapBytes(incompressible)));
    }

    public void testIncompressibleHitsOverTheLimitFail() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        assertThat(heapBytes(hits), greaterThanOrEqualTo(LIMIT));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check.onHits(hits, heapBytes(hits)));
        assertThat(e.getMessage(), containsString("Can't store an async search response larger than [" + LIMIT + "] bytes"));
        assertThat(e.getMessage(), containsString(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()));
    }

    public void testCompressibleHitsFarOverTheLimitPass() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        for (int i = 0; i < 10; i++) {
            List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, false);
            check.onHits(hits, heapBytes(hits));
        }
    }

    public void testHitsThatCompressWorseThanTheFirstSampleFail() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> compressible = hits(HITS_PER_BATCH, SOURCE_SIZE, false);
        check.onHits(compressible, heapBytes(compressible));
        List<SearchHit> incompressible = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(incompressible, heapBytes(incompressible)));
    }

    public void testProjectionWithinTheMarginPasses() {
        List<SearchHit> hits = partlyCompressibleHits();
        long projected = projectedStoredBytes(hits);
        long limit = (long) (projected / 1.2);
        assertThat(heapBytes(hits), greaterThanOrEqualTo(limit));
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    public void testProjectionOverTheMarginFails() {
        List<SearchHit> hits = partlyCompressibleHits();
        long projected = projectedStoredBytes(hits);
        long limit = (long) (projected / 1.3);
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(hits, heapBytes(hits)));
    }

    public void testSampleStopsAtSampleBytes() {
        // The zeros alone fill the sample, so the random bytes after them don't count toward the ratio.
        List<SearchHit> hits = new ArrayList<>(hits(3, 100 * 1024, false));
        assertThat(heapBytes(hits), greaterThanOrEqualTo(StoredResponseSizeCheck.SAMPLE_BYTES));
        hits.addAll(hits(8, 100 * 1024, true));
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        check.onHits(hits, heapBytes(hits));
    }

    public void testHitsBeforeTheNextMultipleOfTheLimitAreNotSampled() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        // Charged as the whole limit, so the sample of this one hit sets a ratio of almost zero.
        check.onHits(hits(1, SOURCE_SIZE, false), LIMIT);

        // Sampling these would project them well over the limit, but they stop short of twice the limit.
        List<SearchHit> incompressible = hits(2, SOURCE_SIZE, true);
        assertThat(LIMIT + heapBytes(incompressible), lessThan(2 * LIMIT));
        check.onHits(incompressible, heapBytes(incompressible));
    }

    public void testCallsWhileAnotherSamplesProceedWithoutSampling() throws Exception {
        CountDownLatch sampling = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // Holds the sampling caller inside the compression until released.
        BytesReference blockingSource = new ZeroBytesReference(SOURCE_SIZE) {
            @Override
            public void writeTo(OutputStream os) throws IOException {
                sampling.countDown();
                safeAwait(release);
                super.writeTo(os);
            }
        };
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        PlainActionFuture<Void> sampler = new PlainActionFuture<>();
        Thread samplerThread = new Thread(() -> ActionListener.completeWith(sampler, () -> {
            check.onHits(List.of(SearchHit.unpooled(0, "id-0").sourceRef(blockingSource)), LIMIT);
            return null;
        }));
        PlainActionFuture<Void> other = new PlainActionFuture<>();
        Thread otherThread = new Thread(() -> ActionListener.completeWith(other, () -> {
            // There is no ratio yet, so this passes unless it samples these hits itself, which would project them over the limit.
            List<SearchHit> incompressible = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
            check.onHits(incompressible, heapBytes(incompressible));
            return null;
        }));
        samplerThread.start();
        try {
            safeAwait(sampling);
            otherThread.start();
            safeGet(other);
        } finally {
            release.countDown();
            samplerThread.join();
            otherThread.join();
        }
        safeGet(sampler);
    }

    public void testEvenlyMixedHitsFailOnlyOnceOverTheLimit() {
        assertMixedHitsFailOnlyOnceOverTheLimit(0.5);
    }

    /**
     * The hits a sample takes are more often random than the rest, so the projection is high, by less than the margin.
     */
    public void testHitsWhoseSamplesCompressWorseFailOnlyOnceOverTheLimit() {
        List<SearchHit> chunk = mixedChunk(0, 0.58);
        StoredResponseSizeCheck.Sample sample = StoredResponseSizeCheck.Sample.of(chunk);
        double skew = ((double) sample.storedBytes() / sample.hitsBytes()) / ((double) storedBytes(chunk) / heapBytes(chunk));
        // Above 1.1, so a margin of 1.1 would fail this search before the hits reach the limit.
        assertThat(skew, both(greaterThan(1.1)).and(lessThan(StoredResponseSizeCheck.MARGIN)));
        assertMixedHitsFailOnlyOnceOverTheLimit(0.58);
    }

    /**
     * Feeds eight chunks, each four sample-sizes long and half random letters, with {@code randomInSample} of the sample random. The
     * limit is the stored size of the first six, so those pass and the last two fail.
     */
    private static void assertMixedHitsFailOnlyOnceOverTheLimit(double randomInSample) {
        List<List<SearchHit>> chunks = new ArrayList<>();
        long limitBytes = 0L;
        for (int c = 0; c < 8; c++) {
            List<SearchHit> chunk = mixedChunk(c * 1000, randomInSample);
            chunks.add(chunk);
            if (c < 6) {
                limitBytes += storedBytes(chunk);
            }
        }
        final long limit = limitBytes;
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        for (List<SearchHit> chunk : chunks.subList(0, 6)) {
            check.onHits(chunk, heapBytes(chunk));
        }
        expectThrows(IllegalArgumentException.class, () -> {
            for (List<SearchHit> chunk : chunks.subList(6, 8)) {
                check.onHits(chunk, heapBytes(chunk));
            }
        });
    }

    /**
     * A chunk of hits that are each either random letters or repeated text, with {@code randomInSample} of those up to
     * {@link StoredResponseSizeCheck#SAMPLE_BYTES} random and half of the whole chunk random.
     */
    private static List<SearchHit> mixedChunk(int firstId, double randomInSample) {
        List<SearchHit> chunk = new ArrayList<>();
        long chunkBytes = 0L;
        int random = 0;
        while (chunkBytes < 4 * StoredResponseSizeCheck.SAMPLE_BYTES) {
            double randomFraction = chunkBytes < StoredResponseSizeCheck.SAMPLE_BYTES ? randomInSample : 0.5;
            boolean isRandom = random < randomFraction * (chunk.size() + 1);
            String source = isRandom ? randomAlphaOfLength(4800) : "lorem ipsum ".repeat(400);
            int id = firstId + chunk.size();
            SearchHit hit = SearchHit.unpooled(id, "id-" + id).sourceRef(new BytesArray(source));
            chunk.add(hit);
            chunkBytes += hit.ramBytesUsed();
            random += isRandom ? 1 : 0;
        }
        return chunk;
    }

    public void testHitsAreCheckedOnceTheResponseIsStored() {
        AtomicBoolean stored = new AtomicBoolean();
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, stored::get);
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(hits, heapBytes(hits));

        // The search ran past wait_for_completion_timeout, so the response is now stored.
        stored.set(true);
        List<SearchHit> more = hits(1, SOURCE_SIZE, true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(more, heapBytes(more)));
    }

    public void testLimitIsReadOnEachCall() {
        AtomicLong limit = new AtomicLong(LIMIT * 100);
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(limit::get, () -> true);
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(hits, heapBytes(hits));

        // The setting is dynamic.
        limit.set(LIMIT);
        List<SearchHit> more = hits(1, SOURCE_SIZE, true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(more, heapBytes(more)));
    }

    private static List<SearchHit> hits(int count, int sourceSize, boolean incompressible) {
        List<SearchHit> hits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] source = incompressible ? randomByteArrayOfLength(sourceSize) : new byte[sourceSize];
            hits.add(SearchHit.unpooled(i, "id-" + i).sourceRef(new BytesArray(source)));
        }
        return hits;
    }

    /**
     * Hits whose source is 30% random bytes and the rest zeros, so they are stored in less than their heap size.
     */
    private static List<SearchHit> partlyCompressibleHits() {
        List<SearchHit> hits = new ArrayList<>(HITS_PER_BATCH);
        for (int i = 0; i < HITS_PER_BATCH; i++) {
            byte[] source = new byte[SOURCE_SIZE];
            System.arraycopy(randomByteArrayOfLength(SOURCE_SIZE * 3 / 10), 0, source, 0, SOURCE_SIZE * 3 / 10);
            hits.add(SearchHit.unpooled(i, "id-" + i).sourceRef(new BytesArray(source)));
        }
        return hits;
    }

    /**
     * What the check projects for {@code hits} when they are all it has seen. They fit in one sample, so it is their stored size.
     */
    private static long projectedStoredBytes(List<SearchHit> hits) {
        StoredResponseSizeCheck.Sample sample = StoredResponseSizeCheck.Sample.of(hits);
        assertThat(sample.hitsBytes(), equalTo(heapBytes(hits)));
        assertThat(sample.storedBytes(), lessThan(sample.hitsBytes()));
        return sample.storedBytes();
    }

    /**
     * The stored size of all of {@code hits}, measured a sample at a time.
     */
    private static long storedBytes(List<SearchHit> hits) {
        long stored = 0L;
        for (int from = 0; from < hits.size();) {
            StoredResponseSizeCheck.Sample sample = StoredResponseSizeCheck.Sample.of(hits.subList(from, hits.size()));
            stored += sample.storedBytes();
            long sampledBytes = 0L;
            while (from < hits.size() && sampledBytes < sample.hitsBytes()) {
                sampledBytes += hits.get(from++).ramBytesUsed();
            }
        }
        return stored;
    }

    private static long heapBytes(List<SearchHit> hits) {
        long bytes = 0L;
        for (SearchHit hit : hits) {
            bytes += hit.ramBytesUsed();
        }
        return bytes;
    }
}
