/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.common.util;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.PagedBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
@OperationsPerInvocation(2621440)
public class IntArrayBenchmark {
    static final int SIZE = IntArrayBenchmark.class.getAnnotation(OperationsPerInvocation.class).value();

    @Param({ "array", "paged_bytes_array", "composite_256kb", "composite_262344b", "composite_1mb" })
    private String type;

    private IntArray read;

    @Setup
    public void init() throws IOException {
        IntArray ints = BigArrays.NON_RECYCLING_INSTANCE.newIntArray(SIZE);
        for (int i = 0; i < SIZE; i++) {
            ints.set(i, i);
        }
        BytesStreamOutput out = new BytesStreamOutput();
        ints.writeTo(out);
        read = IntArray.readFrom(new ReleasableBytesReference(bytesImpl(out.bytes()), () -> {}).streamInput());
    }

    private BytesReference bytesImpl(BytesReference bytes) {
        if (type.equals("array")) {
            return new BytesArray(bytes.toBytesRef());
        }
        if (type.equals("paged_bytes_array")) {
            if (bytes instanceof PagedBytesReference == false) {
                throw new AssertionError("expected PagedBytesReference but saw [" + bytes.getClass() + "]");
            }
            return bytes;
        }
        if (type.startsWith("composite_")) {
            int size = Math.toIntExact(ByteSizeValue.parseBytesSizeValue(type.substring("composite_".length()), "type").getBytes());
            List<BytesReference> references = new ArrayList<>();
            for (int from = 0; from < bytes.length(); from += size) {
                int sliceSize = Math.min(size, bytes.length() - from);
                references.add(new BytesArray(bytes.slice(from, Math.min(from + size, sliceSize)).toBytesRef()));
            }
            BytesReference ref = CompositeBytesReference.of(references.toArray(BytesReference[]::new));
            if (ref instanceof CompositeBytesReference == false) {
                throw new AssertionError("expected CompositeBytesReference but saw [" + bytes.getClass() + "]");
            }
            return ref;
        }
        throw new IllegalArgumentException("unsupported [type] " + type);
    }

    @Benchmark
    public long read() {
        int res = 0;
        for (int i = 0; i < SIZE; i++) {
            res = res ^ read.get(i);
        }
        return res;
    }
}
