/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.bytes;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
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
public class PagedBytesReferenceReadVLongBenchmark {

    private BytesReference pagedBytes;

    @Param(value = { "10000000" })
    int entries;

    @Setup
    public void initResults() throws IOException {
        final BytesStreamOutput tmp = new BytesStreamOutput();
        for (int i = 0; i < entries / 2; i++) {
            tmp.writeVLong(i);
        }
        for (int i = 0; i < entries / 2; i++) {
            tmp.writeVLong(Long.MAX_VALUE - i);
        }
        pagedBytes = tmp.bytes();
        if (pagedBytes instanceof PagedBytesReference == false) {
            throw new AssertionError("expected paged PagedBytesReference but saw [" + pagedBytes.getClass() + "]");
        }
    }

    @Benchmark
    public long readVLong() throws IOException {
        long res = 0;
        try (StreamInput streamInput = pagedBytes.streamInput()) {
            for (int i = 0; i < entries; i++) {
                res = res ^ streamInput.readVLong();
            }
        }
        return res;
    }
}
