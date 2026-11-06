/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.codec.profiler.Counter;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.Timer;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Decorator that counts and times the calls to a {@link ChunkDecompressor}. A reader builds it only when it has a
 * recorder.
 *
 * <p>One decompressor type inflates the chunks of a byte stream, inflates compressed blocks, and reads an
 * uncompressed stream. Each use reports to its own metrics, so the caller passes a {@link Metrics}.
 *
 * <p>The counters change only after the call returns, so a call that throws is not counted. The timer also measures a
 * call that throws.
 */
final class ProfiledChunkDecompressor implements ChunkDecompressor {

    /**
     * The metric definitions for one use of the decorator.
     *
     * @param count           counts the calls
     * @param compressedBytes adds the stored size, or null when another metric reports it
     * @param inflatedBytes   adds the size after the call
     * @param time            times the calls
     */
    record Metrics(CounterMetric count, @Nullable CounterMetric compressedBytes, CounterMetric inflatedBytes, TimerMetric time) {}

    static final CounterMetric CHUNKS_DECOMPRESSED = CounterMetric.of(
        "ColumNAR",
        "chunks",
        "decompressed",
        Metric.Unit.COUNT,
        "Compressed chunks of a column's values that were inflated. Reading the chunk a reader already holds is not counted."
    );

    static final CounterMetric CHUNK_BYTES_COMPRESSED = CounterMetric.of(
        "ColumNAR",
        "chunks",
        "bytes_compressed",
        Metric.Unit.BYTES,
        "Stored size of the chunks that were inflated. This is the decompressor's input, not file or network reads."
    );

    static final CounterMetric CHUNK_BYTES_INFLATED = CounterMetric.of(
        "ColumNAR",
        "chunks",
        "bytes_inflated",
        Metric.Unit.BYTES,
        "Size of the chunks that were inflated, including the bytes of a chunk that the read did not use. This is the compression "
            + "of a byte stream's chunks: values, escaped values, the summary. Integer blocks are counted by blocks.decompressed, and no "
            + "byte is in both. Read-time activity, not the size of the column."
    );

    static final TimerMetric CHUNK_DECOMPRESS_TIME = TimerMetric.of(
        "ColumNAR",
        "chunks",
        "decompress_time_nanos",
        TimingPolicy.EXACT,
        "Elapsed wall-clock time of the calls that inflated a chunk, measured exactly. It covers the decompressor call only, "
            + "and a call that threw is included. It is not CPU time and it can include waits for data. A chunk inflated for a page "
            + "is part of pages.call_time_nanos or pages.byte_lengths_call_time_nanos, so do not add it to those."
    );

    static final CounterMetric CHUNK_CACHE_HITS = CounterMetric.of(
        "ColumNAR",
        "chunks",
        "cache_hits",
        Metric.Unit.COUNT,
        "Times a read needed a chunk and the reader already held it, so nothing was inflated. Together with chunks.decompressed this "
            + "is every chunk the reads needed. A read that spans several chunks needs each of them once."
    );

    /** The metrics of chunk inflation. */
    static final Metrics CHUNKS = new Metrics(CHUNKS_DECOMPRESSED, CHUNK_BYTES_COMPRESSED, CHUNK_BYTES_INFLATED, CHUNK_DECOMPRESS_TIME);

    private final ChunkDecompressor delegate;
    private final Counter inflations;
    @Nullable
    private final Counter compressedBytes;
    private final Counter inflatedBytes;
    private final Timer time;

    ProfiledChunkDecompressor(ChunkDecompressor delegate, MetricRecorder recorder, Metrics metrics) {
        this.delegate = delegate;
        this.inflations = recorder.counter(metrics.count());
        this.compressedBytes = metrics.compressedBytes() == null ? null : recorder.counter(metrics.compressedBytes());
        this.inflatedBytes = recorder.counter(metrics.inflatedBytes());
        this.time = recorder.timer(metrics.time());
    }

    @Override
    public void read(IndexInput in, int storedLength, byte[] dst, int uncompressedLength) throws IOException {
        time.start();
        try {
            delegate.read(in, storedLength, dst, uncompressedLength);
        } finally {
            time.stop();
        }
        inflations.add(1);
        if (compressedBytes != null) {
            compressedBytes.add(storedLength);
        }
        inflatedBytes.add(uncompressedLength);
    }
}
