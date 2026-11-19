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
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
                    lessThan(StoredResponseSizeCheck.MAX_SAMPLED_BYTES_PER_CALL + 2 * StoredResponseSizeCheck.MAX_SAMPLE_BYTES)
                )
            );
        }
        // Each call was sampled once, and the call that reached the limit didn't sample them all.
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
        // Over 3 MB, so it gives at least four runs of 64 KB and 48 single hits of 8 KB, which alone fill both samples.
        List<SearchHit> last = hits(384, 8 * 1024, false);
        calls.add(new StoredResponseSizeCheck.Call(last, heapBytes(last)));
        StoredResponseSizeCheck.SharedSample shared = smallCalls.add(calls.getLast());
        assertNotNull(shared);
        samples.add(shared);

        // All the hits before the last call are as large, so which of them are sampled doesn't depend on the order of the calls.
        long hitCount = 0L;
        long seenBytes = 0L;
        long expectedInRuns = 0L;
        for (StoredResponseSizeCheck.Call call : calls) {
            for (SearchHit hit : call.hits()) {
                hitCount++;
                expectedInRuns += isInRun(seenBytes) ? 1 : 0;
                seenBytes += hit.ramBytesUsed();
            }
        }
        long spread = samples.stream().mapToLong(sample -> sample.spread().size()).sum();
        assertThat(spread, equalTo((hitCount + StoredResponseSizeCheck.SAMPLE_SHARE / 2 - 1) / StoredResponseSizeCheck.SAMPLE_SHARE));
        assertThat(samples.stream().mapToLong(sample -> sample.runs().size()).sum(), equalTo(expectedInRuns));
        assertThat(
            samples.stream().mapToLong(StoredResponseSizeCheck.SharedSample::callsBytes).sum(),
            equalTo(calls.stream().mapToLong(StoredResponseSizeCheck.Call::bytes).sum())
        );
    }

    public void testEmptyCallsDoNotChangeTheProjection() {
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
        assertThat(e.getMessage(), containsString("estimated to need ["));
        assertThat(e.getMessage(), containsString(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()));
    }

    /**
     * A call of exactly the smallest sample is sampled alone, and one just under it is left to the shared samples.
     */
    public void testCallOfTheSmallestSampleIsSampledAlone() {
        // The calls are about 85 KB on the heap, but are given as sizes on either side of the boundary to land on it exactly.
        final long bytes = StoredResponseSizeCheck.MIN_SAMPLE_BYTES;
        AtomicLong shared = new AtomicLong();
        new StoredResponseSizeCheck(() -> bytes - 1, () -> true).onHits(countingHits(HITS_PER_BATCH, shared), bytes - 1);
        assertThat(shared.get(), equalTo(0L));
        AtomicLong alone = new AtomicLong();
        new StoredResponseSizeCheck(() -> bytes, () -> true).onHits(countingHits(HITS_PER_BATCH, alone), bytes);
        assertThat(alone.get(), greaterThan(0L));
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
            for (int i = 0; i < 200; i++) {
                List<SearchHit> hits = hits(2, SOURCE_SIZE, true);
                assertThat(heapBytes(hits), lessThan(StoredResponseSizeCheck.MIN_SAMPLE_BYTES));
                check.onHits(hits, heapBytes(hits));
            }
        });
    }

    /**
     * Small calls that don't fill a shared sample are never projected, so the store decides, even when they don't fit.
     */
    public void testSmallCallsThatDoNotFillASharedSampleAreLeftToTheStore() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        List<SearchHit> allHits = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            List<SearchHit> hits = hits(2, SOURCE_SIZE, true);
            allHits.addAll(hits);
            check.onHits(hits, heapBytes(hits));
        }
        assertThat(heapBytes(allHits), lessThan(StoredResponseSizeCheck.SAMPLE_SHARE * StoredResponseSizeCheck.MAX_SAMPLE_BYTES));
        assertThat(storedBytes(allHits), greaterThan((long) (LIMIT * StoredResponseSizeCheck.MARGIN)));
    }

    public void testSmallCompressibleCallsFarOverTheLimitPass() {
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> LIMIT, () -> true);
        for (int i = 0; i < 300; i++) {
            List<SearchHit> hits = hits(2, SOURCE_SIZE, false);
            check.onHits(hits, heapBytes(hits));
        }
    }

    /**
     * Small calls share two samples. One takes every eighth hit across the calls, starting with the fifth, and the other the hits that
     * start in the first {@link StoredResponseSizeCheck#MIN_SAMPLE_BYTES} of every 512 KB.
     */
    public void testSmallCallsShareSamplesOfTheirHits() {
        StoredResponseSizeCheck.SmallCalls smallCalls = new StoredResponseSizeCheck.SmallCalls();
        List<SearchHit> allHits = new ArrayList<>();
        int spreadHits = 0;
        int position = 0;
        long seenBytes = 0L;
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
            assertThat(heapBytes(shared.spread()), greaterThanOrEqualTo(StoredResponseSizeCheck.MAX_SAMPLE_BYTES));
            assertThat(heapBytes(shared.runs()), greaterThanOrEqualTo(StoredResponseSizeCheck.MAX_SAMPLE_BYTES));
            for (SearchHit hit : shared.spread()) {
                assertSame(allHits.get(spreadHits++ * 8 + 4), hit);
            }
            assertThat(spreadHits, equalTo((allHits.size() + 3) / 8));
            List<SearchHit> expectedRuns = new ArrayList<>();
            for (; position < allHits.size(); position++) {
                if (isInRun(seenBytes)) {
                    expectedRuns.add(allHits.get(position));
                }
                seenBytes += allHits.get(position).ramBytesUsed();
            }
            assertThat(shared.runs().size(), equalTo(expectedRuns.size()));
            for (int i = 0; i < expectedRuns.size(); i++) {
                assertSame(expectedRuns.get(i), shared.runs().get(i));
            }
        }
    }

    /**
     * Small calls of a shard of random hits and a shard of zeros, in a pattern that repeats. A limit of their stored size passes, and
     * one of an eighth of it fails. The samples of some patterns take more zeros than the calls have, and the calls after the last full
     * sample aren't projected, so the projection can be about a third of what the calls need.
     */
    public void testSmallCallsOfShardsThatCompressDifferently() {
        List<List<SearchHit>> calls = new ArrayList<>();
        List<SearchHit> allHits = new ArrayList<>();
        int period = between(2, 13);
        long callsBytes = 0L;
        for (int c = 0; callsBytes < 8 * 1024 * 1024; c++) {
            List<SearchHit> call = hits(between(1, 5), 2048, c % period == 0);
            calls.add(call);
            allHits.addAll(call);
            callsBytes += heapBytes(call);
        }
        long stored = storedBytes(allHits);
        StoredResponseSizeCheck fits = new StoredResponseSizeCheck(() -> stored, () -> true);
        for (List<SearchHit> call : calls) {
            fits.onHits(call, heapBytes(call));
        }
        StoredResponseSizeCheck tooLarge = new StoredResponseSizeCheck(() -> stored / 8, () -> true);
        expectThrows(IllegalArgumentException.class, () -> {
            for (List<SearchHit> call : calls) {
                tooLarge.onHits(call, heapBytes(call));
            }
        });
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
        // The single hits at even steps take every hit of a call this small, so the released hit is in a sample.
        SearchHit released = hits.get(between(0, HITS_PER_BATCH - 1));
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

    /**
     * The reverse of {@link #testHitsOfOneShardDoNotSkewAnother}. Incompressible hits after compressible ones are projected on their own
     * and fail the search.
     */
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
        // Just under the margin, so this and the test below pin it.
        long limit = (long) (projectedStoredBytes(hits) / 1.4);
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    public void testProjectionOverTheMarginFails() {
        List<SearchHit> hits = partlyCompressibleHits();
        // Just over the margin.
        long limit = (long) (projectedStoredBytes(hits) / 1.6);
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(hits, heapBytes(hits)));
    }

    /**
     * Calls of about 90 KB, 1.3 MB and 9 MB of hits of 8 KB. The first is sampled whole, the second one hit in eight, and the third about
     * one hit in 34, so the sample is capped. The second and third start half a step in.
     */
    public void testSpreadSampleTakesSingleHitsAtEvenSteps() {
        assertThat(sampledPositions(10, i -> 8 * 1024, StoredResponseSizeCheck.Sample::spread), equalTo(range(0, 10, 1)));
        assertThat(sampledPositions(150, i -> 8 * 1024, StoredResponseSizeCheck.Sample::spread), equalTo(range(4, 150, 8)));
        List<Integer> positions = sampledPositions(1000, i -> 8 * 1024, StoredResponseSizeCheck.Sample::spread);
        int step = positions.get(1) - positions.get(0);
        assertThat(step, both(greaterThanOrEqualTo(30)).and(lessThanOrEqualTo(36)));
        assertThat(positions, equalTo(range(step / 2, 1000, step)));
    }

    /**
     * The same calls. A run is eight hits, and the calls are sampled in one, two and four runs, at the start of as many equal parts.
     */
    public void testRunsSampleTakesRunsOfNeighboringHits() {
        assertThat(sampledPositions(10, i -> 8 * 1024, StoredResponseSizeCheck.Sample::runs), equalTo(range(0, 8, 1)));
        assertThat(
            sampledPositions(150, i -> 8 * 1024, StoredResponseSizeCheck.Sample::runs),
            equalTo(IntStream.of(0, 75).flatMap(start -> IntStream.range(start, start + 8)).boxed().toList())
        );
        assertThat(
            sampledPositions(1000, i -> 8 * 1024, StoredResponseSizeCheck.Sample::runs),
            equalTo(IntStream.of(0, 250, 500, 750).flatMap(start -> IntStream.range(start, start + 8)).boxed().toList())
        );
    }

    /**
     * Calls of about 1 MB in two hits of 500 KB and in four of 250 KB have a step of eight, so half a step is past their hits. The
     * spread sample takes the last hit instead.
     */
    public void testSpreadSampleOfFewerHitsThanHalfAStepTakesTheLast() {
        assertThat(sampledPositions(2, i -> 500 * 1024, StoredResponseSizeCheck.Sample::spread), equalTo(List.of(1)));
        assertThat(sampledPositions(4, i -> 250 * 1024, StoredResponseSizeCheck.Sample::spread), equalTo(List.of(3)));
    }

    /**
     * A chunk of four incompressible hits of 250 KB has fewer hits than half a step of the spread sample, and is still projected over
     * the limit.
     */
    public void testChunkOfFewLargeHitsFails() {
        List<SearchHit> hits = hits(4, 250 * 1024, true);
        long limit = 512 * 1024;
        assertThat((double) storedBytes(hits), greaterThan(limit * StoredResponseSizeCheck.MARGIN));
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(hits, heapBytes(hits)));
    }

    /**
     * A hit of 600 KB among hits of 8 KB, at the first position of the spread sample and inside the first run, would take either sample
     * past twice what it aims for, so both leave it out and take the hits after it instead.
     */
    public void testSamplesSkipMuchLargerHits() {
        IntUnaryOperator sourceSize = i -> i == 4 ? 600 * 1024 : 8 * 1024;
        assertThat(sampledPositions(150, sourceSize, StoredResponseSizeCheck.Sample::spread), equalTo(range(12, 150, 8)));
        assertThat(
            sampledPositions(150, sourceSize, StoredResponseSizeCheck.Sample::runs),
            equalTo(
                IntStream.concat(
                    IntStream.of(0, 1, 2, 3, 5, 6, 7, 8),
                    IntStream.of(50, 100).flatMap(start -> IntStream.range(start, start + 8))
                ).boxed().toList()
            )
        );
    }

    /**
     * Given as a call of the smallest sample, each sample aims for 64 KB and skips hits past 128 KB, which every 300 KB hit is over. So
     * each takes the first hit alone.
     */
    public void testSamplesThatSkipEveryHitTakeTheFirst() {
        List<Integer> spread = new ArrayList<>();
        StoredResponseSizeCheck.Sample.spread(recordingHits(10, i -> 300 * 1024, spread), StoredResponseSizeCheck.MIN_SAMPLE_BYTES);
        assertThat(spread, equalTo(List.of(0)));
        List<Integer> runs = new ArrayList<>();
        StoredResponseSizeCheck.Sample.runs(recordingHits(10, i -> 300 * 1024, runs), StoredResponseSizeCheck.MIN_SAMPLE_BYTES);
        assertThat(runs, equalTo(List.of(0)));
    }

    /**
     * A call of four parts of ten hits, of 300 KB in the first two and 8 KB in the others. The first run is a single hit of 300 KB, and
     * the next one would take the sample past twice its aim of 256 KB, so it stops there rather than going on to the smaller hits.
     */
    public void testRunsStopAtAHitThatWouldTakeThemPastTwiceTheirAim() {
        assertThat(sampledPositions(40, i -> i < 20 ? 300 * 1024 : 8 * 1024, StoredResponseSizeCheck.Sample::runs), equalTo(List.of(0)));
    }

    /**
     * Hits of 600 KB are over twice the largest aim, so samples that skip all of them are empty rather than taking one.
     */
    public void testSamplesOfHitsOverTwiceTheLargestAimAreEmpty() {
        List<Integer> written = new ArrayList<>();
        List<SearchHit> hits = recordingHits(10, i -> 600 * 1024, written);
        assertThat(StoredResponseSizeCheck.Sample.spread(hits, heapBytes(hits)).hitsBytes(), equalTo(0L));
        assertThat(StoredResponseSizeCheck.Sample.runs(hits, heapBytes(hits)).hitsBytes(), equalTo(0L));
        assertThat(written, equalTo(List.of()));
    }

    /**
     * A call of incompressible 600 KB hits stores in more than the margin over the limit, but its samples are empty, so it is left to
     * the store.
     */
    public void testCallOfHitsOverTwiceTheLargestAimIsLeftToTheStore() {
        List<SearchHit> hits = hits(4, 600 * 1024, true);
        long limit = 1024 * 1024;
        assertThat((double) storedBytes(hits), greaterThan(limit * StoredResponseSizeCheck.MARGIN));
        assertThat(heapBytes(hits), greaterThan(limit));
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    /**
     * A call of ten 1 MB hits of zeros and 200 incompressible hits of 8 KB stores in less than the limit. The samples skip the large
     * hits, which count as storing nothing, so the call isn't projected as if they were as incompressible as the small ones.
     */
    public void testSkippedHitsThatCompressBetterAreLeftToTheStore() {
        List<SearchHit> hits = new ArrayList<>(hits(10, 1024 * 1024, false));
        hits.addAll(hits(200, 8 * 1024, true));
        long limit = 4 * 1024 * 1024;
        assertThat(storedBytes(hits), lessThan(limit));
        assertThat(heapBytes(hits), greaterThan((long) (limit * StoredResponseSizeCheck.MARGIN)));
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    /**
     * A call of three incompressible hits of 100 KB and one of 600 KB. The spread sample takes only the last hit, which is over twice
     * the largest aim, so it is empty, and the runs project the call over the limit.
     */
    public void testCallWhoseSpreadSampleIsEmptyIsProjectedWithTheRuns() {
        List<SearchHit> hits = new ArrayList<>(hits(3, 100 * 1024, true));
        hits.addAll(hits(1, 600 * 1024, true));
        long limit = 200 * 1024;
        StoredResponseSizeCheck.Sample spread = StoredResponseSizeCheck.Sample.spread(hits, heapBytes(hits));
        assertNotNull(spread);
        assertThat(spread.hitsBytes(), equalTo(0L));
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(hits, heapBytes(hits)));
    }

    /**
     * A call of three incompressible hits of 100 KB and one of 600 KB of zeros stores in less than the limit. The spread sample skips
     * only the 600 KB hit, so the runs project the rest of the call, rather than all of it as if the zeros were incompressible too.
     */
    public void testHitsThatAnEmptySampleSkippedAreLeftToTheStore() {
        List<SearchHit> hits = new ArrayList<>(hits(3, 100 * 1024, true));
        hits.addAll(hits(1, 600 * 1024, false));
        long limit = 450 * 1024;
        assertThat(storedBytes(hits), lessThan(limit));
        assertThat(heapBytes(hits), greaterThan(limit));
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    /**
     * A call whose fifth hit is 600 KB of random bytes, among 1 MB of zeros, stores in less than the limit. That hit is the first of the
     * spread sample and inside the first run. If the samples took it, spread would be only that hit and the runs mostly that hit, and
     * both would project the call at about twice the limit.
     */
    public void testCallWithALargeIncompressibleHitInBothSamplesPasses() {
        List<SearchHit> hits = new ArrayList<>(hits(4, 8 * 1024, false));
        hits.addAll(hits(1, 600 * 1024, true));
        hits.addAll(hits(124, 8 * 1024, false));
        long limit = 1024 * 1024;
        assertThat(storedBytes(hits), lessThan(limit));
        assertThat(heapBytes(hits), greaterThan(limit));
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    /**
     * A call whose first hit is 384 KB of random bytes, one and a half times what its samples aim for, followed by 8 MB of zeros, stores
     * in less than the limit. The hit fits in the runs, which project the call at several times the limit, but not in the spread sample,
     * which starts half a step in, so it passes.
     */
    public void testCallWithAnIncompressibleFirstHitUnderTwiceTheAimPasses() {
        List<SearchHit> hits = new ArrayList<>(hits(1, 384 * 1024, true));
        hits.addAll(hits(1000, 8 * 1024, false));
        long limit = 1024 * 1024;
        assertThat(storedBytes(hits), lessThan(limit));
        assertThat(heapBytes(hits), greaterThan(limit));
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    /**
     * Small calls whose first hit is 60 KB of random bytes, followed by 120 calls of ten 2 KB hits of zeros, store in less than the limit.
     * The hit is in the shared runs, which project the calls at more than the margin over the limit, but not in the shared spread sample,
     * which starts half a step in, so they pass.
     */
    public void testSmallCallsWithAnIncompressibleFirstHitPass() {
        List<List<SearchHit>> calls = new ArrayList<>();
        calls.add(hits(1, 60 * 1024, true));
        for (int i = 0; i < 120; i++) {
            calls.add(hits(10, 2 * 1024, false));
        }
        long limit = 200 * 1024;
        assertThat(storedBytes(calls.stream().flatMap(List::stream).toList()), lessThan(limit));
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        for (List<SearchHit> call : calls) {
            check.onHits(call, heapBytes(call));
        }
    }

    /**
     * The first quarter of a call's hits are random and the rest zeros. The runs, at the start of each half of the hits, project the
     * call at about twice what it needs, but the single hits at even steps don't, so it passes.
     */
    public void testCallWhoseFirstHitsCompressWorsePasses() {
        List<SearchHit> hits = new ArrayList<>(hits(110, 2048, true));
        hits.addAll(hits(330, 2048, false));
        long limit = storedBytes(hits);
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    /**
     * The first half of a call's hits are zeros and the rest random, so a sample of just the first hits would miss what the call needs.
     */
    public void testCallWhoseLaterHitsCompressWorseFails() {
        List<SearchHit> hits = new ArrayList<>(hits(220, 2048, false));
        hits.addAll(hits(220, 2048, true));
        long limit = storedBytes(hits) / 2;
        StoredResponseSizeCheck check = new StoredResponseSizeCheck(() -> limit, () -> true);
        expectThrows(IllegalArgumentException.class, () -> check.onHits(hits, heapBytes(hits)));
    }

    /**
     * Each document is next to two to four copies of itself, so the store writes little more than one of them. The single hits at even
     * steps see each copy alone and project the call at two to four times what it needs, but the runs don't, so it passes.
     */
    public void testCallOfNeighboringCopiesPasses() {
        List<SearchHit> hits = new ArrayList<>();
        while (hits.size() < 1000) {
            byte[] source = randomByteArrayOfLength(2048);
            for (int copies = between(2, 4); copies > 0; copies--) {
                hits.add(SearchHit.unpooled(hits.size(), "id-" + hits.size()).sourceRef(new BytesArray(source)));
            }
        }
        long limit = storedBytes(hits);
        new StoredResponseSizeCheck(() -> limit, () -> true).onHits(hits, heapBytes(hits));
    }

    public void testEvenlyMixedHitsFailOnlyOnceOverTheLimit() {
        assertMixedHitsFailOnlyOnceOverTheLimit(0.5);
    }

    /**
     * The hits the samples take are more often random than the rest, so the projection is high, by less than the margin.
     */
    public void testHitsWhoseSamplesCompressWorseFailOnlyOnceOverTheLimit() {
        List<SearchHit> chunk = mixedChunk(0, 0.8);
        double skew = (double) projectedStoredBytes(chunk) / storedBytes(chunk);
        // Above 1.3, so a margin of 1.25 would fail this search before the hits reach the limit.
        assertThat(skew, both(greaterThan(1.3)).and(lessThan(StoredResponseSizeCheck.MARGIN)));
        assertMixedHitsFailOnlyOnceOverTheLimit(0.8);
    }

    /**
     * Feeds ten 1 MB chunks that are each about half random letters, with {@code randomInSample} of the sample random. The limit is
     * the stored size of the first six, so those pass and the search fails within the last four.
     */
    private static void assertMixedHitsFailOnlyOnceOverTheLimit(double randomInSample) {
        List<List<SearchHit>> chunks = new ArrayList<>();
        long limitBytes = 0L;
        for (int c = 0; c < 10; c++) {
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
            for (List<SearchHit> chunk : chunks.subList(6, 10)) {
                check.onHits(chunk, heapBytes(chunk));
            }
        });
    }

    /**
     * A 1 MB chunk of hits that are each random letters or repeated text. Of the hits the samples take, {@code randomInSample} are
     * random, and of the rest, half are.
     */
    private static List<SearchHit> mixedChunk(int firstId, double randomInSample) {
        final int sourceSize = 4800;
        // A hit's heap size depends on the size of its source only, so these hits are as large as the random and repeated ones.
        int count = 0;
        for (long chunkBytes = 0L; chunkBytes < 1024 * 1024; count++) {
            chunkBytes += SearchHit.unpooled(0, "id-0").sourceRef(new BytesArray(new byte[sourceSize])).ramBytesUsed();
        }
        Set<Integer> sampled = new HashSet<>(sampledPositions(count, i -> sourceSize, StoredResponseSizeCheck.Sample::spread));
        sampled.addAll(sampledPositions(count, i -> sourceSize, StoredResponseSizeCheck.Sample::runs));
        List<SearchHit> chunk = new ArrayList<>();
        int inSample = 0;
        int randomInSampleCount = 0;
        int rest = 0;
        int randomInRestCount = 0;
        for (int i = 0; i < count; i++) {
            final boolean isRandom;
            if (sampled.contains(i)) {
                isRandom = randomInSampleCount < randomInSample * ++inSample;
                randomInSampleCount += isRandom ? 1 : 0;
            } else {
                isRandom = randomInRestCount < 0.5 * ++rest;
                randomInRestCount += isRandom ? 1 : 0;
            }
            String source = isRandom ? randomAlphaOfLength(sourceSize) : "lorem ipsum ".repeat(sourceSize / 12);
            int id = firstId + i;
            chunk.add(SearchHit.unpooled(id, "id-" + id).sourceRef(new BytesArray(source)));
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
        StoredResponseSizeCheck.Sample.spread(hits, heapBytes(hits));
        StoredResponseSizeCheck.Sample.runs(hits, heapBytes(hits));
        assertThat(compressedBytes.get(), both(greaterThan(0L)).and(lessThanOrEqualTo(2L * HITS_PER_BATCH * SOURCE_SIZE)));
        return compressedBytes.get();
    }

    /**
     * The positions of the hits that {@code sample} takes from a call of {@code count} hits of zeros, whose sources are as large as
     * {@code sourceSize} gives for each position.
     */
    private static List<Integer> sampledPositions(
        int count,
        IntUnaryOperator sourceSize,
        BiFunction<List<SearchHit>, Long, StoredResponseSizeCheck.Sample> sample
    ) {
        List<Integer> written = new ArrayList<>();
        List<SearchHit> hits = recordingHits(count, sourceSize, written);
        sample.apply(hits, heapBytes(hits));
        return written;
    }

    /**
     * Hits of zeros that each add their position to {@code written} when they are compressed.
     */
    private static List<SearchHit> recordingHits(int count, IntUnaryOperator sourceSize, List<Integer> written) {
        List<SearchHit> hits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final int position = i;
            BytesReference source = new ZeroBytesReference(sourceSize.applyAsInt(i)) {
                @Override
                public void writeTo(OutputStream os) throws IOException {
                    written.add(position);
                    super.writeTo(os);
                }
            };
            hits.add(SearchHit.unpooled(i, "id-" + i).sourceRef(source));
        }
        return hits;
    }

    private static List<Integer> range(int from, int to, int step) {
        return IntStream.iterate(from, i -> i < to, i -> i + step).boxed().toList();
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
        double storedPerHeapByte = Math.min(
            StoredResponseSizeCheck.Sample.spread(hits, heapBytes).storedPerHeapByte(),
            StoredResponseSizeCheck.Sample.runs(hits, heapBytes).storedPerHeapByte()
        );
        assertThat(storedPerHeapByte, lessThan(1.0));
        return (long) (heapBytes * storedPerHeapByte);
    }

    /**
     * The stored size of all of {@code hits}.
     */
    private static long storedBytes(List<SearchHit> hits) {
        return StoredResponseSizeCheck.Sample.of(hits).storedBytes();
    }

    /**
     * Whether the shared runs of small calls take a hit after {@code seenBytes} of hits.
     */
    private static boolean isInRun(long seenBytes) {
        return seenBytes % (512 * 1024) < 64 * 1024;
    }

    private static long heapBytes(List<SearchHit> hits) {
        long bytes = 0L;
        for (SearchHit hit : hits) {
            bytes += hit.ramBytesUsed();
        }
        return bytes;
    }
}
