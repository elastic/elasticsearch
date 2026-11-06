/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.codec.profiler.Counter;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.Timer;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;

import java.io.IOException;

/**
 * Decorator that counts and times the block fetches of a {@link BlockBytesCodec}. A reader builds it only when it has
 * a recorder. The blocks of every integer stream of a column are counted together.
 *
 * <p>A reader fetches a block only when it does not hold it, so a block read again from the reader's buffer is not
 * counted. The counters change only after the fetch returns. The inflation of a compressed block is measured by the
 * {@link ProfiledChunkDecompressor} inside the codec, with {@link #INFLATION}.
 */
final class ProfiledBlockBytesCodec implements BlockBytesCodec {

    static final CounterMetric BLOCKS_FETCHED = CounterMetric.of(
        "ColumNAR",
        "blocks",
        "fetched",
        Metric.Unit.COUNT,
        "Blocks of an integer stream that were fetched because the reader did not hold them: ordinals, length codes and slot "
            + "counts of a string column, or values of a numeric column, all counted together. A reader holds one block of each "
            + "stream, so a block it needs again after moving on is fetched and counted again. A block the reader already holds "
            + "is not counted."
    );

    static final CounterMetric BLOCK_BYTES_FETCHED = CounterMetric.of(
        "ColumNAR",
        "blocks",
        "bytes_fetched",
        Metric.Unit.BYTES,
        "Stored size of the blocks that were fetched, compressed or not, counted again for a block fetched again. It is read-time "
            + "activity, not the size of the column on disk, and it is the codec's input, not file or network reads."
    );

    static final CounterMetric BLOCKS_DECOMPRESSED = CounterMetric.of(
        "ColumNAR",
        "blocks",
        "decompressed",
        Metric.Unit.COUNT,
        "Blocks, among those fetched, that were stored compressed and were inflated. A block stored plain is not counted. This is "
            + "the compression of integer blocks; the chunks of a byte stream are counted by chunks.decompressed, and no byte is in "
            + "both."
    );

    static final CounterMetric BLOCK_BYTES_INFLATED = CounterMetric.of(
        "ColumNAR",
        "blocks",
        "bytes_inflated",
        Metric.Unit.BYTES,
        "Size of the blocks that were inflated, once inflated. Their stored size is in blocks.bytes_fetched."
    );

    static final TimerMetric BLOCK_FETCH_TIME = TimerMetric.of(
        "ColumNAR",
        "blocks",
        "fetch_time_nanos",
        TimingPolicy.ADAPTIVE,
        "Elapsed wall-clock time of the calls that fetched a block, an estimate from a sample of the calls once there are many. "
            + "It runs from the call until the block is available, so it includes inflating a compressed block and any wait for the "
            + "data, and it leaves out decoding the block afterwards. A call that threw is included. It is not CPU time. "
            + "It contains blocks.decompress_time_nanos, so do not add the two or subtract one from the other: an estimate can be "
            + "lower than an exact time it contains."
    );

    static final TimerMetric BLOCK_DECOMPRESS_TIME = TimerMetric.of(
        "ColumNAR",
        "blocks",
        "decompress_time_nanos",
        TimingPolicy.EXACT,
        "Elapsed wall-clock time of the calls that inflated a block, measured exactly. A block stored plain is not timed. "
            + "It is part of the time of fetching the block, so do not add it to blocks.fetch_time_nanos. It is not CPU time."
    );

    /** The metrics of block inflation. The stored size of every block is in {@link #BLOCK_BYTES_FETCHED}. */
    static final ProfiledChunkDecompressor.Metrics INFLATION = new ProfiledChunkDecompressor.Metrics(
        BLOCKS_DECOMPRESSED,
        null,
        BLOCK_BYTES_INFLATED,
        BLOCK_DECOMPRESS_TIME
    );

    private final BlockBytesCodec delegate;
    private final Counter blocks;
    private final Counter storedBytes;
    private final Timer time;

    ProfiledBlockBytesCodec(BlockBytesCodec delegate, MetricRecorder metrics) {
        this.delegate = delegate;
        this.blocks = metrics.counter(BLOCKS_FETCHED);
        this.storedBytes = metrics.counter(BLOCK_BYTES_FETCHED);
        this.time = metrics.timer(BLOCK_FETCH_TIME);
    }

    @Override
    public byte id() {
        return delegate.id();
    }

    @Override
    public void write(BlockEncoder encoder, DataOutput out) throws IOException {
        delegate.write(encoder, out);
    }

    @Override
    public DataInput read(IndexInput in, int length) throws IOException {
        final DataInput block;
        time.start();
        try {
            block = delegate.read(in, length);
        } finally {
            time.stop();
        }
        blocks.add(1);
        storedBytes.add(length);
        return block;
    }
}
