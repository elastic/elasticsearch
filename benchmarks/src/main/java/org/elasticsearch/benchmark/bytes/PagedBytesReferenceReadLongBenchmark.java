/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.bytes;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.PagedBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class PagedBytesReferenceReadLongBenchmark {

    @Param(value = { "1" })
    private int dataMb;

    private BytesReference pagedBytes;

    private StreamInput streamInput;

    @Setup
    public void initResults() throws IOException {
        final BytesStreamOutput tmp = new BytesStreamOutput();
        final long bytes = new ByteSizeValue(dataMb, ByteSizeUnit.MB).getBytes();
        for (int i = 0; i < bytes / 8; i++) {
            tmp.writeLong(i);
        }
        pagedBytes = tmp.bytes();
        if (pagedBytes instanceof PagedBytesReference == false) {
            throw new AssertionError("expected PagedBytesReference but saw [" + pagedBytes.getClass() + "]");
        }
        this.streamInput = pagedBytes.streamInput();
    }

    @Benchmark
    public long readLong() throws IOException {
        long res = 0L;
        streamInput.reset();
        final int reads = pagedBytes.length() / 8;
        for (int i = 0; i < reads; i++) {
            res = res ^ streamInput.readLong();
        }
        return res;
    }
}
