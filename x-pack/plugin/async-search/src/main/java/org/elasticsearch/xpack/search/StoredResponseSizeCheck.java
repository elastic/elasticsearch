/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.FetchedHitsCheck;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.CountingStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;

/**
 * Fails an async search as soon as its hits are projected to be too large to store, rather than after the response is built and the
 * store rejects it.
 * <p>
 * The store compresses the response, so the heap size of the hits doesn't give the stored size. Each call's hits are projected from
 * two samples of their own, compressed the way the store does, so hits of another shard don't skew them. One takes single hits at even
 * steps, the other a few runs of neighboring hits, so hits that are alike compress together, as they do in the store. Each can project
 * too high where the other doesn't, so the projection is the lower of the two. Calls with less than {@link #MIN_SAMPLE_BYTES} of hits
 * share two samples. The search fails once the projections add up to more than {@link #MARGIN} times the limit.
 * <p>
 * Nothing is compressed until the hits on the heap reach the limit. Until then calls are only remembered. From then on they are sampled
 * on an executor, since compressing takes too long for the transport thread that delivers the hits. A call fails if the samples so far
 * project too much, and {@link #onFetchDone} samples the calls that are left before the hits are merged. Samples with released hits
 * are skipped.
 * <p>
 * These are left to the store: hits that arrived before the result was marked as stored, small calls that don't fill their shared
 * samples, calls whose hits are each over twice {@link #MAX_SAMPLE_BYTES}, the unsampled hits of a call where one sample finds only
 * such hits, and the rest of the response, such as aggregations. So are the hits still unsampled if the executor rejects the sampling
 * when the fetch phase is done.
 * <p>
 * The projection can be high for hits that repeat across indices, and for hits of a chunked shard that fails later. It can also be high
 * when a hit that compresses much worse than the rest is in both samples. That happens at about one position in 64, in calls of up to
 * a few MB.
 * <p>
 * Hits over twice what a sample aims for aren't sampled. A call that mixes them with smaller hits can be projected low. It can be
 * projected high if they compress much better than the smaller hits and the samples don't land on them.
 */
final class StoredResponseSizeCheck implements FetchedHitsCheck {

    /**
     * The smallest sample compressed on its own, by heap size, and the size of each run of neighboring hits. The store compresses all
     * the hits together, so a smaller one misses what repeats across hits, such as field names, and projects too high. Calls with less
     * than this share their samples.
     */
    static final long MIN_SAMPLE_BYTES = ByteSizeValue.ofKb(64).getBytes();

    /**
     * The largest heap size each of a call's samples aims for. Compressing this much takes a few milliseconds. No sample is larger than
     * twice this.
     */
    static final long MAX_SAMPLE_BYTES = ByteSizeValue.ofKb(256).getBytes();

    /**
     * Each of a call's samples aims for about one in this many of its hits by heap size, within {@link #MIN_SAMPLE_BYTES} and
     * {@link #MAX_SAMPLE_BYTES}. The samples that calls with less than {@link #MIN_SAMPLE_BYTES} of hits share take about one in this
     * many of their hits too.
     */
    static final int SAMPLE_SHARE = 8;

    /**
     * How many times the limit the projection must exceed to fail the search. A sample can compress differently from the rest of the
     * hits, so this leaves room for a projection a little higher than the stored size.
     */
    static final double MARGIN = 1.5;

    private static final Logger logger = LogManager.getLogger(StoredResponseSizeCheck.class);

    private final LongSupplier maxStoredSize;
    private final BooleanSupplier isStored;
    private final Executor executor;
    private final AtomicLong hitsBytes = new AtomicLong();
    private final AtomicLong projectedBytes = new AtomicLong();
    // Calls to sample once the hits reach the limit.
    private final Queue<Call> unsampled = new ConcurrentLinkedQueue<>();
    private final SmallCalls smallCalls = new SmallCalls();
    private volatile boolean reachedLimit;
    // How many times sampling was asked for since it last ran out of calls. Only the first of them starts a task.
    private final AtomicInteger samplingRequests = new AtomicInteger();
    // Completed once the calls left when the fetch phase was done are sampled.
    private final AtomicReference<ActionListener<Void>> fetchDone = new AtomicReference<>();

    /**
     * @param maxStoredSize the size limit of stored responses, read on each call since the setting is dynamic
     * @param isStored      whether the response will be stored, which can turn true while the search runs
     * @param executor      where the hits are compressed
     */
    StoredResponseSizeCheck(LongSupplier maxStoredSize, BooleanSupplier isStored, Executor executor) {
        this.maxStoredSize = maxStoredSize;
        this.isStored = isStored;
        this.executor = executor;
    }

    @Override
    public void onHits(List<SearchHit> hits, long bytes) {
        final long total = hitsBytes.addAndGet(bytes);
        final long limit = maxStoredSize.getAsLong();
        // A limit of zero or less rejects every response, so the store decides.
        if (limit <= 0L || isStored.getAsBoolean() == false) {
            return;
        }
        // Once the projection fails the search, the shards and chunks still arriving fail too, without sampling.
        ensureProjectionFits(limit);
        unsampled.add(new Call(hits, bytes));
        // Read after adding, so a call added while the remembered calls are sampled is sampled too.
        if (reachedLimit == false) {
            if (total < limit) {
                return;
            }
            reachedLimit = true;
        }
        startSampling();
        // The sampling may be done already.
        ensureProjectionFits(limit);
    }

    /**
     * Samples the calls that are left, and fails the listener if the hits are projected to be too large to store. Completes at once if
     * the hits never reached the limit, since nothing was sampled.
     */
    @Override
    public void onFetchDone(ActionListener<Void> listener) {
        if (reachedLimit == false) {
            listener.onResponse(null);
            return;
        }
        final ActionListener<Void> previous = fetchDone.getAndSet(listener);
        assert previous == null : "the fetch phase is done only once";
        startSampling();
    }

    private void ensureProjectionFits(long limit) {
        final long projected = projectedBytes.get();
        if (isOverMargin(limit, projected)) {
            throw tooLarge(limit, projected);
        }
    }

    private boolean projectionFits() {
        final long limit = maxStoredSize.getAsLong();
        return limit <= 0L || isOverMargin(limit, projectedBytes.get()) == false;
    }

    private static boolean isOverMargin(long limit, long projected) {
        return projected > limit * MARGIN;
    }

    private static IllegalArgumentException tooLarge(long limit, long projected) {
        return new IllegalArgumentException(
            "Can't store an async search response larger than ["
                + limit
                + "] bytes, and the hits of this search are estimated to need ["
                + projected
                + "] bytes. This limit can be set by changing the ["
                + MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()
                + "] setting."
        );
    }

    /**
     * Samples the remembered calls on the executor. At most one task samples at a time, and it runs until no request for sampling is
     * left, so every call remembered before a request is sampled.
     */
    private void startSampling() {
        if (samplingRequests.getAndIncrement() != 0) {
            return;
        }
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                int requests = samplingRequests.get();
                do {
                    // Taken before sampling, so every call of the fetch phase is sampled before the listener is completed.
                    final ActionListener<Void> listener = fetchDone.getAndSet(null);
                    try {
                        sampleCalls();
                    } catch (Exception e) {
                        // Must not stop later sampling or leave the fetch phase waiting.
                        assert false : e;
                        logger.warn("failed to sample the hits of an async search", e);
                    }
                    if (listener != null) {
                        completeFetchDone(listener);
                    }
                } while ((requests = samplingRequests.addAndGet(-requests)) > 0);
            }

            @Override
            public void onRejection(Exception e) {
                // The calls stay remembered for a later request to sample, and a fetch phase that is done leaves them to the store.
                samplingRequests.set(0);
                final ActionListener<Void> listener = fetchDone.getAndSet(null);
                if (listener != null) {
                    listener.onResponse(null);
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert false : e;
                logger.warn("failed to sample the hits of an async search", e);
            }
        });
    }

    private void completeFetchDone(ActionListener<Void> listener) {
        final long limit = maxStoredSize.getAsLong();
        final long projected = projectedBytes.get();
        if (limit > 0L && isOverMargin(limit, projected)) {
            listener.onFailure(tooLarge(limit, projected));
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Samples the calls that aren't sampled yet, unless the projection already fails the search.
     */
    private void sampleCalls() {
        Call call;
        while (projectionFits() && (call = unsampled.poll()) != null) {
            if (call.bytes() >= MIN_SAMPLE_BYTES) {
                project(Sample.spread(call.hits(), call.bytes()), Sample.runs(call.hits(), call.bytes()), call.bytes());
            } else {
                final SharedSample shared = smallCalls.add(call);
                if (shared != null) {
                    project(Sample.of(shared.spread()), Sample.of(shared.runs()), shared.callsBytes());
                }
            }
        }
    }

    /**
     * Adds to the projection the lower of what {@code spread} and {@code runs} project for {@code bytes} of hits on the heap. The lower
     * one can be too low, which is safe, since the store still checks the real size. A sample that took no hits found only hits too
     * large for it, and how many of the call's hits are that large isn't known. So only the stored size of the hits the other sample
     * took is added, and the rest of the call is left to the store. Adds nothing if neither took hits, or if either is {@code null}
     * because a hit was released.
     */
    private void project(@Nullable Sample spread, @Nullable Sample runs, long bytes) {
        if (spread == null || runs == null) {
            return;
        }
        if (spread.hitsBytes() > 0L && runs.hitsBytes() > 0L) {
            projectedBytes.addAndGet((long) (bytes * Math.min(spread.storedPerHeapByte(), runs.storedPerHeapByte())));
        } else if (spread.hitsBytes() > 0L || runs.hitsBytes() > 0L) {
            projectedBytes.addAndGet(spread.hitsBytes() > 0L ? spread.storedBytes() : runs.storedBytes());
        }
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
     * Builds the samples that calls under {@link #MIN_SAMPLE_BYTES} of hits share. Across the calls, one takes every
     * {@link #SAMPLE_SHARE}-th hit, starting half a step in so the first hit is only in the runs, and the other runs of
     * {@link #MIN_SAMPLE_BYTES}, one in every {@link #SAMPLE_SHARE} times that. Taking the first hits of each call would give a call with
     * few hits more of the samples than it has of the hits. Each fills to {@link #MAX_SAMPLE_BYTES}, so it draws on about
     * {@link #SAMPLE_SHARE} times that in calls.
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
                if ((hitCount++ + SAMPLE_SHARE / 2) % SAMPLE_SHARE == 0) {
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
     * The heap size of the hits in a sample, the heap size of the hits it skipped, and the stored size of the hits in it once compressed
     * and base64-encoded as the store does. A sample is empty if it took no hits.
     */
    record Sample(long hitsBytes, long skippedBytes, long storedBytes) {

        /**
         * Compresses all of {@code hits}.
         *
         * @return the sample, or {@code null} if a hit was released, including hits of other calls in a shared sample
         */
        @Nullable
        static Sample of(List<SearchHit> hits) {
            return compress(hits, 1, Long.MAX_VALUE, 0, 1, Long.MAX_VALUE);
        }

        /**
         * Compresses every n-th hit of a call's {@code hits}, starting half a step in, to add up to about {@link #sampleBytes} of
         * {@code callBytes} when its hits are about the same size. Stops before it would pass twice that, and skips hits larger than
         * that. The runs start at the first hit, so a first hit that compresses much worse than the rest is in only one of the two
         * samples. A call with fewer hits than half a step starts at its last hit, so it is still sampled.
         *
         * @return the sample, or {@code null} if a hit was released
         */
        @Nullable
        static Sample spread(List<SearchHit> hits, long callBytes) {
            final long sampleBytes = sampleBytes(callBytes);
            final int step = Math.toIntExact(Math.max(1L, callBytes / sampleBytes));
            final int offset = Math.clamp(step / 2, 0, hits.size() - 1);
            return compress(hits, 1, Long.MAX_VALUE, offset, step, 2 * sampleBytes);
        }

        /**
         * Compresses runs of about {@link #MIN_SAMPLE_BYTES} of neighboring hits, at the start of equal parts of a call's {@code hits},
         * to add up to about {@link #sampleBytes} of {@code callBytes}. A hit that would take it past twice that ends its run, and hits
         * larger than that are skipped.
         *
         * @return the sample, or {@code null} if a hit was released
         */
        @Nullable
        static Sample runs(List<SearchHit> hits, long callBytes) {
            final long sampleBytes = sampleBytes(callBytes);
            final int runs = Math.toIntExact(sampleBytes / MIN_SAMPLE_BYTES);
            return compress(hits, runs, MIN_SAMPLE_BYTES, 0, 1, 2 * sampleBytes);
        }

        /**
         * Splits {@code hits} into {@code runs} equal parts and compresses every {@code step}-th hit of each, from {@code offset} hits
         * after its start, until they reach {@code runBytes}. A hit that would take the sample past {@code maxBytes} ends its part, and
         * hits larger than {@code maxBytes} are skipped, which leaves them to the store. If it skips every hit, takes the first of them
         * alone if it is at most twice {@link #MAX_SAMPLE_BYTES}, as if the others were like it, and returns an empty sample otherwise.
         *
         * @return the sample, or {@code null} at the first released hit
         */
        @Nullable
        private static Sample compress(List<SearchHit> hits, int runs, long runBytes, int offset, int step, long maxBytes) {
            assert runs > 0 && step > 0 && offset >= 0 : runs + " " + offset + " " + step;
            long heapBytes = 0L;
            long skippedBytes = 0L;
            SearchHit firstSkipped = null;
            long firstSkippedBytes = 0L;
            final CountingStreamOutput compressed = new CountingStreamOutput();
            try (StreamOutput out = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(compressed)) {
                out.setTransportVersion(TransportVersion.current());
                for (int run = 0; run < runs && heapBytes < maxBytes; run++) {
                    final int end = Math.toIntExact((long) (run + 1) * hits.size() / runs);
                    long bytesInRun = 0L;
                    for (int i = Math.toIntExact((long) run * hits.size() / runs) + offset; i < end && bytesInRun < runBytes; i += step) {
                        final SearchHit hit = hits.get(i);
                        if (hit.tryIncRef() == false) {
                            return null;
                        }
                        try {
                            final long hitBytes = hit.ramBytesUsed();
                            if (hitBytes > maxBytes) {
                                // Not counted in the run, but each is larger than the sample can be, so few sizes are read.
                                skippedBytes += hitBytes;
                                if (firstSkipped == null) {
                                    firstSkipped = hit;
                                    firstSkippedBytes = hitBytes;
                                }
                            } else if (hitBytes > maxBytes - heapBytes) {
                                // Ends the part, so the sizes of its other hits aren't read. The next part may have hits that fit.
                                break;
                            } else {
                                hit.writeTo(out);
                                bytesInRun += hitBytes;
                                heapBytes += hitBytes;
                            }
                        } finally {
                            hit.decRef();
                        }
                    }
                }
                if (heapBytes == 0L && firstSkipped != null) {
                    if (firstSkippedBytes > 2 * MAX_SAMPLE_BYTES) {
                        return new Sample(0L, skippedBytes, 0L);
                    }
                    if (firstSkipped.tryIncRef() == false) {
                        return null;
                    }
                    try {
                        firstSkipped.writeTo(out);
                        heapBytes = firstSkippedBytes;
                        skippedBytes = 0L;
                    } finally {
                        firstSkipped.decRef();
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new Sample(heapBytes, skippedBytes, (compressed.position() + 2) / 3 * 4);
        }

        /**
         * The stored size per byte on the heap of the hits the sample took and skipped. The skipped ones count as storing nothing, so
         * they are left to the store rather than projected like the others. Only meaningful if the sample took hits.
         */
        double storedPerHeapByte() {
            return (double) storedBytes / (hitsBytes + skippedBytes);
        }
    }
}
