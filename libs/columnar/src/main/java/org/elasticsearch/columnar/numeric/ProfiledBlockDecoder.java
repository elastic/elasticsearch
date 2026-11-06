/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.elasticsearch.codec.profiler.Counter;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.Timer;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;

import java.io.IOException;

/**
 * Decorator that counts and times the block decodes of a {@link NumericBlockDecoder}. A reader builds it only when it
 * has a recorder.
 *
 * <p>The counter changes only after the decode returns, so a decode that throws is not counted. The timer also
 * measures a decode that throws.
 */
final class ProfiledBlockDecoder implements NumericBlockDecoder {

    static final TimerMetric BLOCK_DECODE_TIME = TimerMetric.of(
        "ColumNAR",
        "blocks",
        "decode_time_nanos",
        TimingPolicy.ADAPTIVE,
        "Elapsed wall-clock time of the calls that decoded a fetched block into values, an estimate from a sample of the calls once "
            + "there are many. A block stored plain is read off the file while it is decoded, so the time can include waits for the "
            + "data; a block that was inflated first is decoded from memory. It follows the fetch that blocks.fetch_time_nanos covers "
            + "and does not overlap it. It is not CPU time."
    )
        .withCpuTime(
            "decode_cpu_time_nanos",
            "CPU time the executing thread spent in the calls that blocks.decode_time_nanos covers, an estimate from the same sample "
                + "of the calls. The elapsed time minus this is an approximate off-CPU component: waits such as page faults, "
                + "scheduling or blocking, without saying which. CPU time includes instructions executed while the processor stalls "
                + "on memory, so it does not expose cache or memory stalls, and it can include kernel execution. Present only when "
                + "the collector measures thread CPU time and the platform's clock stayed valid. The search and ES|QL profiles do not "
                + "ask for thread CPU time, so it is not reported there."
        );

    static final CounterMetric VALUES_DECODED = CounterMetric.of(
        "ColumNAR",
        "blocks",
        "values_decoded",
        Metric.Unit.COUNT,
        "Integer values decoded from the blocks that were fetched: a whole block's worth for each fetch, however few of them the "
            + "read needed, and again for a block fetched again. They are the values of every integer stream of the column together: "
            + "ordinals, length codes and slot counts of a string column, never its value bytes. Only for a column of one such "
            + "stream, a numeric column or a dictionary column of single values, does comparing it with the values a read returned "
            + "approximate decode amplification, and the ratio is not an exact factor."
    );

    private final NumericBlockDecoder delegate;
    private final Timer time;
    private final Counter values;

    ProfiledBlockDecoder(NumericBlockDecoder delegate, MetricRecorder metrics) {
        this.delegate = delegate;
        this.time = metrics.timer(BLOCK_DECODE_TIME);
        this.values = metrics.counter(VALUES_DECODED);
    }

    @Override
    public void decode(DataInput in, int valueCount, long[] out) throws IOException {
        time.start();
        try {
            delegate.decode(in, valueCount, out);
        } finally {
            time.stop();
        }
        values.add(valueCount);
    }
}
