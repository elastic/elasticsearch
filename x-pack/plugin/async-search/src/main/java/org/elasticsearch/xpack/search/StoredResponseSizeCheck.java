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
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;

/**
 * Fails an async search as soon as its hits are projected to be too large to store, rather than after the response is built and the
 * store rejects it.
 * <p>
 * The store compresses the response, so the heap size of the hits doesn't give the stored size. Each time the hits pass another
 * multiple of the limit, a sample of the hits that just arrived is compressed the way the store does it. The ratio over all samples
 * then projects the stored size of all the hits so far. The search fails once the projection is over the limit by more than
 * {@link #MARGIN}, which allows for hits that compress differently from the samples. The rest of the response, such as aggregations,
 * isn't counted, so a response just over the limit is still left for the store to reject.
 * <p>
 * Only one caller samples at a time. Callers that arrive meanwhile project with the ratio so far, which is zero before the first
 * sample.
 * <p>
 * The heap size counts every hit charged so far, including those of a chunked shard that fails later, so a search that goes on
 * with partial results can be projected a little high.
 */
final class StoredResponseSizeCheck implements FetchedHitsCheck {

    /**
     * Stops a sample once the hits compressed reach this heap size. A sample covers the hits of the call that passes a multiple of the
     * limit, and the hit that crosses this is compressed whole. Compressing runs on a transport thread, where this takes a few
     * milliseconds.
     */
    static final long SAMPLE_BYTES = ByteSizeValue.ofKb(256).getBytes();

    /**
     * How far over the limit the projection must be to fail the search. A sample is a run of consecutive hits, so it can compress
     * differently from the rest. In a simulation of 100 searches whose 4 KB hits were each either repeated text or random letters,
     * arriving in 1 MB chunks against a 10 MB limit with 256 KB samples, the projection was up to 20.5% high when the hits reached
     * the limit.
     */
    static final double MARGIN = 1.25;

    private final LongSupplier maxStoredSize;
    private final BooleanSupplier isStored;
    private final AtomicLong hitsBytes = new AtomicLong();
    private final AtomicBoolean sampling = new AtomicBoolean();

    // Only read and written by the caller that holds sampling.
    private long sampledHitsBytes;
    private long sampledStoredBytes;

    // The heap size at which the next sample is taken.
    private volatile long nextSampleAt;
    // Stored bytes per heap byte over all samples, 0 until the first sample.
    private volatile double ratio;

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
        // Below the limit on the heap, or with a limit of zero or less that the division below can't take, the store decides.
        if (limit <= 0L || total < limit || isStored.getAsBoolean() == false) {
            return;
        }
        if (total >= nextSampleAt && sampling.compareAndSet(false, true)) {
            try {
                if (total >= nextSampleAt) {
                    Sample sample = Sample.of(hits);
                    // An empty call can reach here when a multiple went unsampled. Its sample has no hits, so it must not use up that
                    // multiple or make the ratio infinite.
                    if (sample.hitsBytes() > 0L) {
                        sampledHitsBytes += sample.hitsBytes();
                        sampledStoredBytes += sample.storedBytes();
                        ratio = (double) sampledStoredBytes / sampledHitsBytes;
                        nextSampleAt = (total / limit + 1) * limit;
                    }
                }
            } finally {
                sampling.set(false);
            }
        }
        final long projected = (long) (total * ratio);
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
     * The heap size of the hits in a sample, and their stored size once compressed and base64-encoded as the store does.
     */
    record Sample(long hitsBytes, long storedBytes) {

        static Sample of(List<SearchHit> hits) {
            long heapBytes = 0L;
            final CountingStreamOutput compressed = new CountingStreamOutput();
            try (StreamOutput out = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(compressed)) {
                out.setTransportVersion(TransportVersion.current());
                for (SearchHit hit : hits) {
                    if (heapBytes >= SAMPLE_BYTES) {
                        break;
                    }
                    hit.writeTo(out);
                    heapBytes += hit.ramBytesUsed();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new Sample(heapBytes, (compressed.position() + 2) / 3 * 4);
        }
    }
}
