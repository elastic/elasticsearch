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
 * The store compresses the response, so the heap size of the hits doesn't give the stored size. Each call's hits are projected from a
 * sample of their own, compressed the way the store does it, so hits that compress differently in another shard don't skew them. Calls
 * with less than {@link #MIN_SAMPLE_BYTES} of hits are too small to sample alone, so they share a sample that takes hits from each of
 * them. The search fails once the projections add up to more than {@link #MARGIN} times the limit.
 * <p>
 * Most searches never reach the limit on the heap, so nothing is compressed until they do. Until then, calls are only remembered. From
 * then on, each call samples the remembered ones, up to {@link #MAX_SAMPLED_BYTES_PER_CALL}. A sample with hits released by then,
 * because their shard failed or the search ended, is skipped.
 * <p>
 * Some hits aren't counted, which leaves them to the store: those that arrived before the result was marked as stored, those not
 * sampled yet when the last call returns, and the rest of the response, such as aggregations. Hits of a chunked shard that fails later
 * can be counted, so a search that goes on with partial results can be projected a little high. Hits that repeat across indices, such
 * as the same documents in two, can be projected high too, because the store sees the copies together and a sample sees each once.
 */
final class StoredResponseSizeCheck implements FetchedHitsCheck {

    /**
     * The smallest sample compressed on its own, by heap size. The store compresses all the hits together, so a smaller one misses what
     * repeats across hits, such as field names, and projects too high. Calls with less than this share a sample.
     */
    static final long MIN_SAMPLE_BYTES = ByteSizeValue.ofKb(64).getBytes();

    /**
     * Caps a sample at this heap size. Compressing runs on a transport thread, where this takes a few milliseconds.
     */
    static final long MAX_SAMPLE_BYTES = ByteSizeValue.ofKb(256).getBytes();

    /**
     * A call samples one in this many of its hits by heap size, within {@link #MIN_SAMPLE_BYTES} and {@link #MAX_SAMPLE_BYTES}. Calls
     * with less than {@link #MIN_SAMPLE_BYTES} of hits share a sample, which takes one hit in this many.
     */
    static final int SAMPLE_SHARE = 8;

    /**
     * Stops a call from sampling remembered calls once its samples reach this heap size, so the call that takes the hits to the limit
     * doesn't hold its transport thread until all of them are sampled. The calls after it sample the rest. A call can go over by one
     * sample, so it compresses at most about three of the largest samples. The cap is twice the largest sample a call adds, so the
     * remembered calls run out even when every call is that large.
     */
    static final long MAX_SAMPLED_BYTES_PER_CALL = 2 * MAX_SAMPLE_BYTES;

    /**
     * How many times the limit the projection must exceed to fail the search. A sample is only some of the hits, so it can compress
     * differently from the rest. In a simulation of logs, metrics, random and repeated text, arriving in whole shards, in 1 MB chunks or
     * in small calls, the projection was up to 13% high.
     */
    static final double MARGIN = 1.25;

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
                sampledBytes += project(call.hits(), sampleBytes(call.bytes()), call.bytes());
            } else {
                final SharedSample shared = smallCalls.add(call);
                if (shared != null) {
                    sampledBytes += project(shared.hits(), Long.MAX_VALUE, shared.callsBytes());
                }
            }
        }
        return projectedBytes.get();
    }

    /**
     * Adds to the projection what a sample of {@code hits} projects for {@code bytes} of hits on the heap.
     *
     * @return the heap size of the sample
     */
    private long project(List<SearchHit> hits, long sampleBytes, long bytes) {
        final Sample sample = Sample.of(hits, sampleBytes);
        if (sample.hitsBytes() > 0L) {
            projectedBytes.addAndGet((long) ((double) bytes * sample.storedBytes() / sample.hitsBytes()));
        }
        return sample.hitsBytes();
    }

    /**
     * The sample size for a call with {@code callBytes} of hits.
     */
    static long sampleBytes(long callBytes) {
        return Math.clamp(callBytes / SAMPLE_SHARE, MIN_SAMPLE_BYTES, MAX_SAMPLE_BYTES);
    }

    /**
     * The hits of a call and their heap size.
     */
    record Call(List<SearchHit> hits, long bytes) {}

    /**
     * The hits of a sample that calls share, and the heap size of all the hits of those calls.
     */
    record SharedSample(List<SearchHit> hits, long callsBytes) {}

    /**
     * Builds the sample that calls with less than {@link #MIN_SAMPLE_BYTES} of hits share. It takes one hit in {@link #SAMPLE_SHARE}
     * across the calls. Taking the first hits of each call instead would give a call with few hits a larger part of the sample than
     * it has of the hits.
     */
    static final class SmallCalls {
        private List<SearchHit> sample = new ArrayList<>();
        private long sampleBytes;
        private long callsBytes;
        private long hitCount;

        /**
         * @return the sample once it reaches {@link #MIN_SAMPLE_BYTES}, and {@code null} until then
         */
        @Nullable
        synchronized SharedSample add(Call call) {
            for (SearchHit hit : call.hits()) {
                if (hitCount++ % SAMPLE_SHARE == 0) {
                    sample.add(hit);
                    // Close enough to tell when the sample is large enough, without reading a hit that may be released.
                    sampleBytes += call.bytes() / call.hits().size();
                }
            }
            callsBytes += call.bytes();
            if (sampleBytes < MIN_SAMPLE_BYTES) {
                return null;
            }
            final SharedSample shared = new SharedSample(sample, callsBytes);
            sample = new ArrayList<>();
            sampleBytes = 0L;
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
         * Compresses the first of {@code hits} until their heap size reaches {@code sampleBytes}, including the hit that crosses it.
         * Returns an empty sample at the first released hit.
         */
        static Sample of(List<SearchHit> hits, long sampleBytes) {
            long heapBytes = 0L;
            final CountingStreamOutput compressed = new CountingStreamOutput();
            try (StreamOutput out = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(compressed)) {
                out.setTransportVersion(TransportVersion.current());
                for (SearchHit hit : hits) {
                    if (heapBytes >= sampleBytes) {
                        break;
                    }
                    if (hit.tryIncRef() == false) {
                        return new Sample(0L, 0L);
                    }
                    try {
                        hit.writeTo(out);
                        heapBytes += hit.ramBytesUsed();
                    } finally {
                        hit.decRef();
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new Sample(heapBytes, (compressed.position() + 2) / 3 * 4);
        }
    }
}
