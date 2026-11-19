/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.FetchedHitsCheck;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.CountingStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;

/**
 * Fails an async search as soon as its hits are projected to be too large to store, rather than after the response is built and the
 * store rejects it.
 * <p>
 * The store compresses the response, so the heap size of the hits doesn't give the stored size. Each call's hits are projected from
 * two samples of their own, compressed the way the store does it, so hits of another shard don't skew them. One takes single hits at
 * even steps across the call, so one part of the call can't skew it. The other takes a few runs of neighboring hits, so hits that are
 * alike compress together, as they do in the store. Each can project too high where the other doesn't, so the projection is the
 * lower of the two. Calls with less than {@link #MIN_SAMPLE_BYTES} of hits are too small to sample alone, so they share two samples
 * that take hits from many of them. The search fails once the projections add up to more than {@link #MARGIN} times the limit.
 * <p>
 * Most searches never reach the limit on the heap, so nothing is compressed until they do. Until then, calls are only remembered. From
 * then on, each call samples the remembered ones, up to {@link #MAX_SAMPLED_BYTES_PER_CALL}. Samples with hits released by then,
 * because their shard failed or the search ended, are skipped.
 * <p>
 * Some hits aren't counted, which leaves them to the store: those that arrived before the result was marked as stored, those not
 * sampled yet when the last call returns, small calls whose shared samples aren't full yet, and the rest of the response, such as
 * aggregations. Hits of a chunked shard that fails later can be counted, so a search that goes on with partial results can be
 * projected a little high. Hits that repeat across indices, such as the same documents in two, can be projected high too, because the
 * store sees the copies together and a sample sees each once.
 */
final class StoredResponseSizeCheck implements FetchedHitsCheck {

    /**
     * The smallest sample compressed on its own, by heap size, and the size of each run of neighboring hits. The store compresses all
     * the hits together, so a smaller one misses what repeats across hits, such as field names, and projects too high. Calls with less
     * than this share their samples.
     */
    static final long MIN_SAMPLE_BYTES = ByteSizeValue.ofKb(64).getBytes();

    /**
     * The largest heap size each of a call's samples aims for. Compressing runs on a transport thread, where this takes a few
     * milliseconds. A sample stops at twice what it aims for, plus one hit, so a few hits much larger than the others can't make it
     * large.
     */
    static final long MAX_SAMPLE_BYTES = ByteSizeValue.ofKb(256).getBytes();

    /**
     * Each of a call's samples aims for about one in this many of its hits by heap size, within {@link #MIN_SAMPLE_BYTES} and
     * {@link #MAX_SAMPLE_BYTES}. The samples that calls with less than {@link #MIN_SAMPLE_BYTES} of hits share take about one in this
     * many of their hits too.
     */
    static final int SAMPLE_SHARE = 8;

    /**
     * Stops a call from sampling remembered calls once its samples reach this heap size, so the call that takes the hits to the limit
     * doesn't hold its transport thread until all of them are sampled. The calls after it sample the rest. A call can go over by one
     * call's samples.
     */
    // Twice one call's samples, so each call samples about two waiting calls and the queue empties.
    static final long MAX_SAMPLED_BYTES_PER_CALL = 4 * MAX_SAMPLE_BYTES;

    /**
     * How many times the limit the projection must exceed to fail the search. A sample can compress differently from the rest of the
     * hits, so this leaves room for a projection a little higher than the stored size.
     */
    static final double MARGIN = 1.5;

    private final LongSupplier maxStoredSize;
    private final BooleanSupplier isStored;
    private final AtomicLong hitsBytes = new AtomicLong();
    private final AtomicLong projectedBytes = new AtomicLong();
    // Calls to sample once the hits reach the limit.
    private final Queue<Call> unsampled = new ConcurrentLinkedQueue<>();
    private final SmallCalls smallCalls = new SmallCalls();
    private volatile boolean reachedLimit;

    /**
     * @param maxStoredSize the size limit of stored responses, read on each call since the setting is dynamic
     * @param isStored      whether the response will be stored, which can turn true while the search runs
     */
    StoredResponseSizeCheck(LongSupplier maxStoredSize, BooleanSupplier isStored) {
        this.maxStoredSize = maxStoredSize;
        this.isStored = isStored;
    }

    @Override
    public void onHits(List<SearchHit> hits, long bytes) {
        final long total = hitsBytes.addAndGet(bytes);
        final long limit = maxStoredSize.getAsLong();
        // A limit of zero or less rejects every response, so the store decides.
        if (limit <= 0L || isStored.getAsBoolean() == false) {
            return;
        }
        unsampled.add(new Call(hits, bytes));
        // Read after adding, so a call added while another one samples the remembered calls is sampled by one of the two.
        if (reachedLimit == false) {
            if (total < limit) {
                return;
            }
            reachedLimit = true;
        }
        final long projected = sampleCalls();
        if (projected > limit * MARGIN) {
            throw new IllegalArgumentException(
                "Can't store an async search response larger than ["
                    + limit
                    + "] bytes, and the hits of this search are estimated to need ["
                    + projected
                    + "] bytes. This limit can be set by changing the ["
                    + MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()
                    + "] setting."
            );
        }
    }

    /**
     * Samples calls that aren't sampled yet, up to {@link #MAX_SAMPLED_BYTES_PER_CALL}.
     *
     * @return the stored size projected for all the calls sampled so far
     */
    private long sampleCalls() {
        long sampledBytes = 0L;
        Call call;
        while (sampledBytes < MAX_SAMPLED_BYTES_PER_CALL && (call = unsampled.poll()) != null) {
            if (call.bytes() >= MIN_SAMPLE_BYTES) {
                sampledBytes += project(Sample.spread(call.hits(), call.bytes()), Sample.runs(call.hits(), call.bytes()), call.bytes());
            } else {
                final SharedSample shared = smallCalls.add(call);
                if (shared != null) {
                    sampledBytes += project(Sample.of(shared.spread()), Sample.of(shared.runs()), shared.callsBytes());
                }
            }
        }
        return projectedBytes.get();
    }

    /**
     * Adds to the projection the lower of what {@code spread} and {@code runs} project for {@code bytes} of hits on the heap. The lower
     * one can be too low, which is safe, since the store still checks the real size. Adds nothing if either sample is empty.
     *
     * @return the heap size of the two samples
     */
    private long project(Sample spread, Sample runs, long bytes) {
        if (spread.hitsBytes() > 0L && runs.hitsBytes() > 0L) {
            projectedBytes.addAndGet((long) (bytes * Math.min(spread.storedPerHeapByte(), runs.storedPerHeapByte())));
        }
        return spread.hitsBytes() + runs.hitsBytes();
    }

    /**
     * The heap size each sample of a call with {@code callBytes} of hits aims for.
     */
    static long sampleBytes(long callBytes) {
        return Math.clamp(callBytes / SAMPLE_SHARE, MIN_SAMPLE_BYTES, MAX_SAMPLE_BYTES);
    }

    /**
     * The hits of a call and their heap size.
     */
    record Call(List<SearchHit> hits, long bytes) {}

    /**
     * The hits of the two samples that calls share, and the heap size of all the hits of those calls.
     */
    record SharedSample(List<SearchHit> spread, List<SearchHit> runs, long callsBytes) {}

    /**
     * Builds the samples that calls with less than {@link #MIN_SAMPLE_BYTES} of hits share. Across the calls, one takes every
     * {@link #SAMPLE_SHARE}-th hit. The other takes runs of {@link #MIN_SAMPLE_BYTES} of hits, one in every {@link #SAMPLE_SHARE} times
     * that. Taking the first hits of each call instead would give a call with few hits a larger part of the samples than it has of the
     * hits. Each fills to {@link #MAX_SAMPLE_BYTES}, the most a call's sample aims for, so it draws on about {@link #SAMPLE_SHARE}
     * times that in calls.
     */
    static final class SmallCalls {
        private List<SearchHit> spread = new ArrayList<>();
        private List<SearchHit> runs = new ArrayList<>();
        private long spreadBytes;
        private long runsBytes;
        private long callsBytes;
        private long hitCount;
        private long seenBytes;

        /**
         * @return the samples once both reach {@link #MAX_SAMPLE_BYTES}, and {@code null} until then
         */
        @Nullable
        synchronized SharedSample add(Call call) {
            for (SearchHit hit : call.hits()) {
                // Close enough to place the hit and to tell when a sample is large enough, without reading a hit that may be released.
                final long hitBytes = call.bytes() / call.hits().size();
                if (hitCount++ % SAMPLE_SHARE == 0) {
                    spread.add(hit);
                    spreadBytes += hitBytes;
                }
                if (seenBytes % (SAMPLE_SHARE * MIN_SAMPLE_BYTES) < MIN_SAMPLE_BYTES) {
                    runs.add(hit);
                    runsBytes += hitBytes;
                }
                seenBytes += hitBytes;
            }
            callsBytes += call.bytes();
            if (spreadBytes < MAX_SAMPLE_BYTES || runsBytes < MAX_SAMPLE_BYTES) {
                return null;
            }
            final SharedSample shared = new SharedSample(spread, runs, callsBytes);
            spread = new ArrayList<>();
            runs = new ArrayList<>();
            spreadBytes = 0L;
            runsBytes = 0L;
            callsBytes = 0L;
            return shared;
        }
    }

    /**
     * The heap size of the hits in a sample, and their stored size once compressed and base64-encoded as the store does. A sample is
     * empty if any of its hits was released, including hits of other calls in a shared sample.
     */
    record Sample(long hitsBytes, long storedBytes) {

        /**
         * Compresses all of {@code hits}.
         */
        static Sample of(List<SearchHit> hits) {
            return compress(hits, 1, Long.MAX_VALUE, 1, Long.MAX_VALUE);
        }

        /**
         * Compresses every n-th hit of a call's {@code hits}, starting with the first, to add up to about {@link #sampleBytes} of
         * {@code callBytes} when its hits are about the same size. Stops at twice that.
         */
        static Sample spread(List<SearchHit> hits, long callBytes) {
            final long sampleBytes = sampleBytes(callBytes);
            final int step = Math.toIntExact(Math.max(1L, callBytes / sampleBytes));
            return compress(hits, 1, Long.MAX_VALUE, step, 2 * sampleBytes);
        }

        /**
         * Compresses runs of about {@link #MIN_SAMPLE_BYTES} of neighboring hits, at the start of equal parts of a call's {@code hits},
         * to add up to about {@link #sampleBytes} of {@code callBytes}. Stops at twice that.
         */
        static Sample runs(List<SearchHit> hits, long callBytes) {
            final long sampleBytes = sampleBytes(callBytes);
            final int runs = Math.toIntExact(sampleBytes / MIN_SAMPLE_BYTES);
            return compress(hits, runs, MIN_SAMPLE_BYTES, 1, 2 * sampleBytes);
        }

        /**
         * Splits {@code hits} into {@code runs} equal parts and compresses every {@code step}-th hit of each, from its start, until
         * they reach {@code runBytes}. Stops once the hits it compressed reach {@code maxBytes}, including the hit that crosses it.
         * Returns an empty sample at the first released hit.
         */
        private static Sample compress(List<SearchHit> hits, int runs, long runBytes, int step, long maxBytes) {
            assert runs > 0 && step > 0 : runs + " " + step;
            long heapBytes = 0L;
            final CountingStreamOutput compressed = new CountingStreamOutput();
            try (StreamOutput out = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(compressed)) {
                out.setTransportVersion(TransportVersion.current());
                for (int run = 0; run < runs && heapBytes < maxBytes; run++) {
                    final int end = Math.toIntExact((long) (run + 1) * hits.size() / runs);
                    long bytesInRun = 0L;
                    for (int i = Math.toIntExact((long) run * hits.size() / runs); i < end
                        && bytesInRun < runBytes
                        && heapBytes < maxBytes; i += step) {
                        final SearchHit hit = hits.get(i);
                        if (hit.tryIncRef() == false) {
                            return new Sample(0L, 0L);
                        }
                        try {
                            hit.writeTo(out);
                            final long hitBytes = hit.ramBytesUsed();
                            bytesInRun += hitBytes;
                            heapBytes += hitBytes;
                        } finally {
                            hit.decRef();
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new Sample(heapBytes, (compressed.position() + 2) / 3 * 4);
        }

        /**
         * The stored size per byte of the hits on the heap, which is NaN for an empty sample.
         */
        double storedPerHeapByte() {
            return (double) storedBytes / hitsBytes;
        }
    }
}
