/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ZeroBytesReference;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    private static final long LIMIT = 128 * 1024;
    // A batch is over the minimum sample, and one incompressible batch fits under the limit with the margin while two don't.
    private static final int HITS_PER_BATCH = 8;
    private static final int SOURCE_SIZE = 10 * 1024;

    public void testCallsUnderTheLimitAreNotCompressed() {
        AtomicLong compressedBytes = new AtomicLong();
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> hits = countingHits(HITS_PER_BATCH, compressedBytes);
        assertThat(heapBytes(hits), greaterThanOrEqualTo(StoredResponseSizeCheck.MIN_SAMPLE_BYTES));
        check.onHits(hits, heapBytes(hits));
        List<SearchHit> small = countingHits(2, compressedBytes);
        assertThat(heapBytes(hits) + heapBytes(small), lessThan(LIMIT));
        check.onHits(small, heapBytes(small));
        assertThat(compressedBytes.get(), equalTo(0L));
    }

    /**
     * A call under the minimum sample takes the hits to the limit, which samples the call remembered before it.
     */
    public void testSmallCallThatReachesTheLimitStartsSampling() {
        AtomicLong compressedBytes = new AtomicLong();
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> hits = countingHits(HITS_PER_BATCH, compressedBytes);
        check.onHits(hits, heapBytes(hits));
        long total = heapBytes(hits);
        while (total < LIMIT) {
            assertThat(compressedBytes.get(), equalTo(0L));
            List<SearchHit> small = countingHits(2, compressedBytes);
            assertThat(heapBytes(small), lessThan(StoredResponseSizeCheck.MIN_SAMPLE_BYTES));
            check.onHits(small, heapBytes(small));
            total += heapBytes(small);
        }
        assertThat(compressedBytes.get(), equalTo(sampledSourceBytes()));
    }

    /**
     * The call that takes the hits to the limit samples only some of the remembered calls, and the calls after it sample the rest.
     */
    public void testCallSamplesRememberedCallsOnlyUpToTheCap() {
        final long sampledOfACall = sampledSourceBytes();
        final long callBytes = heapBytes(countingHits(HITS_PER_BATCH, new AtomicLong()));
        final int remembered = 40;
        assertThat(remembered * sampledOfACall, greaterThan(2 * StoredResponseSizeCheck.MAX_SAMPLED_BYTES_PER_CALL));
        AtomicLong compressedBytes = new AtomicLong();
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> remembered * callBytes + 1, () -> true);
        for (int i = 0; i < remembered; i++) {
            List<SearchHit> hits = countingHits(HITS_PER_BATCH, compressedBytes);
            check.onHits(hits, callBytes);
        }
        assertThat(compressedBytes.get(), equalTo(0L));

        int calls = remembered;
        long compressedBefore = 0L;
        while (compressedBytes.get() < calls * sampledOfACall) {
            assertThat("the remembered calls should run out", calls, lessThan(remembered + 100));
            List<SearchHit> hits = countingHits(HITS_PER_BATCH, compressedBytes);
            check.onHits(hits, callBytes);
            calls++;
            long compressedByCall = compressedBytes.get() - compressedBefore;
            compressedBefore = compressedBytes.get();
            assertThat(
                compressedByCall,
                both(greaterThanOrEqualTo(sampledOfACall)).and(
                    lessThan(StoredResponseSizeCheck.MAX_SAMPLED_BYTES_PER_CALL + StoredResponseSizeCheck.MAX_SAMPLE_BYTES)
                )
            );
        }
        // Each call was sampled once, and it took more than the call that reached the limit.
        assertThat(compressedBytes.get(), equalTo(calls * sampledOfACall));
        assertThat(calls, greaterThan(remembered + 1));
    }

    public void testConcurrentCallsAreEachSampledOnce() {
        final long sampledOfACall = sampledSourceBytes();
        final int threads = between(2, 8);
        final int callsPerThread = between(5, 20);
        AtomicLong compressedBytes = new AtomicLong();
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        startInParallel(threads, thread -> {
            for (int i = 0; i < callsPerThread; i++) {
                List<SearchHit> hits = countingHits(HITS_PER_BATCH, compressedBytes);
                check.onHits(hits, heapBytes(hits));
            }
        });
        assertThat(compressedBytes.get(), equalTo(threads * callsPerThread * sampledOfACall));
    }

    /**
     * Small calls from several threads share samples, and each of their hits and bytes is counted once across them.
     */
    public void testConcurrentSmallCallsAreEachCountedOnce() {
        final int threads = between(2, 8);
        final int callsPerThread = between(50, 200);
        StoredResponseSizeCheck.SmallCalls smallCalls = new StoredResponseSizeCheck.SmallCalls();
        List<StoredResponseSizeCheck.Call> calls = new ArrayList<>();
        for (int i = 0; i < threads * callsPerThread; i++) {
            List<SearchHit> hits = hits(between(1, 5), 1024, false);
            calls.add(new StoredResponseSizeCheck.Call(hits, heapBytes(hits)));
        }
        Queue<StoredResponseSizeCheck.SharedSample> samples = new ConcurrentLinkedQueue<>();
        startInParallel(threads, thread -> {
            for (int i = 0; i < callsPerThread; i++) {
                StoredResponseSizeCheck.SharedSample shared = smallCalls.add(calls.get(thread * callsPerThread + i));
                if (shared != null) {
                    samples.add(shared);
                }
            }
        });
        // One of any eight hits in a row is sampled, and this one alone completes the sample.
        List<SearchHit> last = hits(StoredResponseSizeCheck.SAMPLE_SHARE, Math.toIntExact(StoredResponseSizeCheck.MIN_SAMPLE_BYTES), false);
        calls.add(new StoredResponseSizeCheck.Call(last, heapBytes(last)));
        StoredResponseSizeCheck.SharedSample shared = smallCalls.add(calls.getLast());
        assertNotNull(shared);
        samples.add(shared);

        long hitCount = calls.stream().mapToLong(call -> call.hits().size()).sum();
        long sampledHits = samples.stream().mapToLong(sample -> sample.hits().size()).sum();
        assertThat(sampledHits, equalTo((hitCount + StoredResponseSizeCheck.SAMPLE_SHARE - 1) / StoredResponseSizeCheck.SAMPLE_SHARE));
        assertThat(
            samples.stream().mapToLong(StoredResponseSizeCheck.SharedSample::callsBytes).sum(),
            equalTo(calls.stream().mapToLong(StoredResponseSizeCheck.Call::bytes).sum())
        );
    }

    public void testEmptyCallsBeforeAndAfterTheLimitPass() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        check.onHits(List.of(), 0L);
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(hits, heapBytes(hits));
        // Takes the hits to the limit, so the empty call is sampled with the others, and fits.
        List<SearchHit> compressible = hits(HITS_PER_BATCH, SOURCE_SIZE, false);
        check.onHits(compressible, heapBytes(compressible));
        check.onHits(List.of(), 0L);

        // The empty calls didn't change the projection, so this call takes it over the limit.
        List<SearchHit> more = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(more, heapBytes(more)));
    }

    public void testLimitOfZeroOrLessIsLeftToTheStore() {
        long limit = randomFrom(0L, -1L);
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        // Incompressible hits that would fail the search with any positive limit this small.
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(hits, heapBytes(hits));
    }

    public void testIncompressibleHitsOverTheLimitFail() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        assertThat(heapBytes(hits), lessThan(LIMIT));
        check.onHits(hits, heapBytes(hits));
        // Takes the hits to the limit, so both calls are sampled, and together they are projected over it.
        List<SearchHit> more = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> check.onHits(more, heapBytes(more)));
        assertThat(e.getMessage(), containsString("Can't store an async search response larger than [" + LIMIT + "] bytes"));
        assertThat(e.getMessage(), containsString(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()));
    }

    public void testCompressibleHitsFarOverTheLimitPass() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        for (int i = 0; i < 20; i++) {
            List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, false);
            check.onHits(hits, heapBytes(hits));
        }
    }

    public void testSmallIncompressibleCallsOverTheLimitFail() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        expectThrows(IllegalArgumentException.class, () -> {
            for (int i = 0; i < 40; i++) {
                List<SearchHit> hits = hits(2, SOURCE_SIZE, true);
                assertThat(heapBytes(hits), lessThan(StoredResponseSizeCheck.MIN_SAMPLE_BYTES));
                check.onHits(hits, heapBytes(hits));
            }
        });
    }

    public void testSmallCompressibleCallsFarOverTheLimitPass() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        for (int i = 0; i < 100; i++) {
            List<SearchHit> hits = hits(2, SOURCE_SIZE, false);
            check.onHits(hits, heapBytes(hits));
        }
    }

    public void testSmallCallsShareASampleOfOneHitInEight() {
        StoredResponseSizeCheck.SmallCalls smallCalls = new StoredResponseSizeCheck.SmallCalls();
        List<SearchHit> allHits = new ArrayList<>();
        int sampledHits = 0;
        for (int sample = 0; sample < 2; sample++) {
            long callsBytes = 0L;
            StoredResponseSizeCheck.SharedSample shared = null;
            while (shared == null) {
                List<SearchHit> hits = hits(between(1, 5), 2 * 1024, randomBoolean());
                allHits.addAll(hits);
                callsBytes += heapBytes(hits);
                shared = smallCalls.add(new StoredResponseSizeCheck.Call(hits, heapBytes(hits)));
            }
            // It stands for the calls since the sample before it.
            assertThat(shared.callsBytes(), equalTo(callsBytes));
            assertThat(heapBytes(shared.hits()), greaterThanOrEqualTo(StoredResponseSizeCheck.MIN_SAMPLE_BYTES));
            for (SearchHit hit : shared.hits()) {
                assertSame(allHits.get(sampledHits++ * StoredResponseSizeCheck.SAMPLE_SHARE), hit);
            }
            assertThat(sampledHits, equalTo((allHits.size() - 1) / StoredResponseSizeCheck.SAMPLE_SHARE + 1));
        }
    }

    /**
     * A shard of compressible hits under the limit, then a shard of incompressible hits that takes them to it and fits. Projecting the
     * first shard with the second one's ratio would fail the search.
     */
    public void testHitsOfOneShardDoNotSkewAnother() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> compressible = hits(HITS_PER_BATCH, SOURCE_SIZE, false);
        check.onHits(compressible, heapBytes(compressible));
        List<SearchHit> incompressible = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        assertThat(heapBytes(compressible) + heapBytes(incompressible), greaterThanOrEqualTo(LIMIT));
        check.onHits(incompressible, heapBytes(incompressible));
    }

    public void testHitsReleasedBeforeTheLimitAreSkipped() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> released = pooledHits(HITS_PER_BATCH);
        check.onHits(released, heapBytes(released));
        // Their shard failed, or the search ended.
        released.forEach(SearchHit::decRef);

        // Takes the hits to the limit, and fits once the released ones are left out.
        List<SearchHit> incompressible = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(incompressible, heapBytes(incompressible));
    }

    public void testCallWithSomeHitsReleasedIsSkipped() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> hits = pooledHits(HITS_PER_BATCH);
        check.onHits(hits, heapBytes(hits));
        // A hit within the sample is released.
        SearchHit released = hits.get(between(0, 4));
        released.decRef();

        try {
            // Takes the hits to the limit, and fits once the call with a released hit is left out.
            List<SearchHit> incompressible = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
            check.onHits(incompressible, heapBytes(incompressible));
        } finally {
            hits.stream().filter(hit -> hit != released).forEach(SearchHit::decRef);
        }
    }

    /**
     * A small call whose hits are released is in a shared sample with live small calls, so the whole sample is left out.
     */
    public void testSharedSampleWithAReleasedHitIsSkipped() {
        List<StoredResponseSizeCheck.Call> calls = new ArrayList<>();
        List<SearchHit> released = pooledHits(2);
        calls.add(new StoredResponseSizeCheck.Call(released, heapBytes(released)));
        // The calls that complete the first and the second shared sample, the first of which has the released call's first hit.
        StoredResponseSizeCheck.SmallCalls smallCalls = new StoredResponseSizeCheck.SmallCalls();
        List<Integer> completing = new ArrayList<>();
        assertNull(smallCalls.add(calls.getFirst()));
        while (completing.size() < 2) {
            List<SearchHit> hits = hits(2, SOURCE_SIZE, true);
            calls.add(new StoredResponseSizeCheck.Call(hits, heapBytes(hits)));
            if (smallCalls.add(calls.getLast()) != null) {
                completing.add(calls.size() - 1);
            }
        }
        assertThat(heapBytes(released), lessThan(StoredResponseSizeCheck.MIN_SAMPLE_BYTES));

        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        check.onHits(released, heapBytes(released));
        released.forEach(SearchHit::decRef);
        // Each shared sample is projected far over the limit, unless it has a released hit.
        for (int i = 1; i < completing.get(1); i++) {
            check.onHits(calls.get(i).hits(), calls.get(i).bytes());
        }
        StoredResponseSizeCheck.Call last = calls.get(completing.get(1));
        expectThrows(IllegalArgumentException.class, () -> check.onHits(last.hits(), last.bytes()));
    }

    public void testHitsThatCompressWorseThanEarlierOnesFail() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> compressible = hits(HITS_PER_BATCH, SOURCE_SIZE, false);
        check.onHits(compressible, heapBytes(compressible));
        List<SearchHit> incompressible = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(incompressible, heapBytes(incompressible));
        List<SearchHit> more = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(more, heapBytes(more)));
    }

    public void testProjectionWithinTheMarginPasses() {
        List<SearchHit> hits = partlyCompressibleHits();
        long limit = (long) (projectedStoredBytes(hits) / 1.2);
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    public void testProjectionOverTheMarginFails() {
        List<SearchHit> hits = partlyCompressibleHits();
        long limit = (long) (projectedStoredBytes(hits) / 1.3);
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(hits, heapBytes(hits)));
    }

    public void testSampleStopsAtItsShareOfTheHits() {
        // The first hits have sources of zeros and take up more than an eighth of the hits, so the sample ends before the random hits
        // after them. Sampling those would fail the search.
        List<SearchHit> hits = new ArrayList<>(hits(2, 100 * 1024, false));
        hits.addAll(hits(9, 100 * 1024, true));
        long sampleBytes = StoredResponseSizeCheck.sampleBytes(heapBytes(hits));
        assertThat(sampleBytes, both(greaterThan(StoredResponseSizeCheck.MIN_SAMPLE_BYTES)).and(lessThan(heapBytes(hits.subList(0, 2)))));
        new StoredResponseSizeCheck(() -> LIMIT, () -> true).onHits(hits, heapBytes(hits));
    }

    public void testSampleStopsAtItsMaximum() {
        // An eighth of the hits is more than the maximum sample. The first hits have sources of zeros and take up more than the maximum,
        // so the sample ends before the random hits after them. Sampling those would fail the search.
        List<SearchHit> hits = new ArrayList<>(hits(3, 100 * 1024, false));
        hits.addAll(hits(40, 100 * 1024, true));
        assertThat(heapBytes(hits) / StoredResponseSizeCheck.SAMPLE_SHARE, greaterThan(heapBytes(hits.subList(0, 4))));
        assertThat(StoredResponseSizeCheck.MAX_SAMPLE_BYTES, lessThan(heapBytes(hits.subList(0, 3))));
        new StoredResponseSizeCheck(() -> LIMIT, () -> true).onHits(hits, heapBytes(hits));
    }

    public void testSampleTakesAtLeastItsMinimum() {
        // The first hits have sources of zeros and take up more than an eighth of the hits, but less than the minimum sample, so the
        // sample reaches the random hits after them and the search fails.
        List<SearchHit> hits = new ArrayList<>(hits(11, 4 * 1024, false));
        hits.addAll(hits(69, 4 * 1024, true));
        long zerosBytes = heapBytes(hits.subList(0, 11));
        assertThat(heapBytes(hits) / StoredResponseSizeCheck.SAMPLE_SHARE, lessThan(zerosBytes));
        assertThat(StoredResponseSizeCheck.MIN_SAMPLE_BYTES, greaterThan(zerosBytes));
        long limit = 64 * 1024;
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(hits, heapBytes(hits)));
    }

    public void testEvenlyMixedHitsFailOnlyOnceOverTheLimit() {
        assertMixedHitsFailOnlyOnceOverTheLimit(0.5);
    }

    /**
     * The hits a sample takes are more often random than the rest, so the projection is high, by less than the margin.
     */
    public void testHitsWhoseSamplesCompressWorseFailOnlyOnceOverTheLimit() {
        List<SearchHit> chunk = mixedChunk(0, 0.58);
        StoredResponseSizeCheck.Sample sample = StoredResponseSizeCheck.Sample.of(
            chunk,
            StoredResponseSizeCheck.sampleBytes(heapBytes(chunk))
        );
        double skew = ((double) sample.storedBytes() / sample.hitsBytes()) / ((double) storedBytes(chunk) / heapBytes(chunk));
        // Above 1.1, so a margin of 1.1 would fail this search before the hits reach the limit.
        assertThat(skew, both(greaterThan(1.1)).and(lessThan(StoredResponseSizeCheck.MARGIN)));
        assertMixedHitsFailOnlyOnceOverTheLimit(0.58);
    }

    /**
     * Feeds eight 1 MB chunks that are each half random letters, with {@code randomInSample} of the sample random. The limit is the
     * stored size of the first six, so those pass and the search fails within the last two.
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
     * A 1 MB chunk of hits that are each random letters or repeated text. Its first eighth, which is what a sample takes, has
     * {@code randomInSample} random hits, and the rest is half random.
     */
    private static List<SearchHit> mixedChunk(int firstId, double randomInSample) {
        final long chunkSize = 1024 * 1024;
        List<SearchHit> chunk = new ArrayList<>();
        long chunkBytes = 0L;
        int random = 0;
        while (chunkBytes < chunkSize) {
            double randomFraction = chunkBytes < chunkSize / StoredResponseSizeCheck.SAMPLE_SHARE ? randomInSample : 0.5;
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

    public void testHitsBeforeTheResponseIsStoredAreNotCounted() {
        AtomicBoolean stored = new AtomicBoolean();
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, stored::get);
        for (int i = 0; i < 3; i++) {
            List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
            check.onHits(hits, heapBytes(hits));
        }

        // The search ran past wait_for_completion_timeout, so the response is now stored.
        stored.set(true);
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(hits, heapBytes(hits));
        List<SearchHit> more = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(more, heapBytes(more)));
    }

    public void testLimitIsReadOnEachCall() {
        AtomicLong limit = new AtomicLong(LIMIT * 100);
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(limit::get, () -> true);
        List<SearchHit> hits = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        check.onHits(hits, heapBytes(hits));

        // The setting is dynamic.
        limit.set(LIMIT);
        List<SearchHit> more = hits(HITS_PER_BATCH, SOURCE_SIZE, true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(more, heapBytes(more)));
    }

    /**
     * Hits of zeros that add the size of their source to {@code compressedBytes} each time one of them is compressed.
     */
    private static List<SearchHit> countingHits(int count, AtomicLong compressedBytes) {
        BytesReference source = new ZeroBytesReference(SOURCE_SIZE) {
            @Override
            public void writeTo(OutputStream os) throws IOException {
                compressedBytes.addAndGet(length());
                super.writeTo(os);
            }
        };
        List<SearchHit> hits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            hits.add(SearchHit.unpooled(i, "id-" + i).sourceRef(source));
        }
        return hits;
    }

    /**
     * The size of the sources that are compressed when a call of {@link #HITS_PER_BATCH} counting hits is sampled.
     */
    private static long sampledSourceBytes() {
        AtomicLong compressedBytes = new AtomicLong();
        List<SearchHit> hits = countingHits(HITS_PER_BATCH, compressedBytes);
        StoredResponseSizeCheck.Sample.of(hits, StoredResponseSizeCheck.sampleBytes(heapBytes(hits)));
        assertThat(compressedBytes.get(), both(greaterThan(0L)).and(lessThan((long) HITS_PER_BATCH * SOURCE_SIZE)));
        return compressedBytes.get();
    }

    /**
     * Incompressible hits that are released like the hits a coordinator reads off the wire. The caller must release them.
     */
    private static List<SearchHit> pooledHits(int count) {
        List<SearchHit> hits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            hits.add(new SearchHit(i, "id-" + i).sourceRef(new BytesArray(randomByteArrayOfLength(SOURCE_SIZE))));
        }
        return hits;
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
     * What the check projects for {@code hits} when they are all it has seen.
     */
    private static long projectedStoredBytes(List<SearchHit> hits) {
        long heapBytes = heapBytes(hits);
        StoredResponseSizeCheck.Sample sample = StoredResponseSizeCheck.Sample.of(hits, StoredResponseSizeCheck.sampleBytes(heapBytes));
        assertThat(sample.storedBytes(), lessThan(sample.hitsBytes()));
        return heapBytes * sample.storedBytes() / sample.hitsBytes();
    }

    /**
     * The stored size of all of {@code hits}, measured a maximum sample at a time.
     */
    private static long storedBytes(List<SearchHit> hits) {
        long stored = 0L;
        for (int from = 0; from < hits.size();) {
            List<SearchHit> rest = hits.subList(from, hits.size());
            StoredResponseSizeCheck.Sample sample = StoredResponseSizeCheck.Sample.of(rest, StoredResponseSizeCheck.MAX_SAMPLE_BYTES);
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
