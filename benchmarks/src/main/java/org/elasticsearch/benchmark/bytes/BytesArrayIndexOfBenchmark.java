/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.bytes;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.LogConfigurator;
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

import java.util.concurrent.TimeUnit;

@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class BytesArrayIndexOfBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static final byte MARKER = (byte) '\n';

    @Param(value = { "64", "127", "128", "4096", "16384", "65536", "1048576" })
    public int size;

    BytesArray bytesArray;

    @Setup
    public void setup() {
        byte[] bytes = new byte[size];
        bytes[bytes.length - 1] = MARKER;
        bytesArray = new BytesArray(bytes, 0, bytes.length);
    }

    @Benchmark
    public int indexOf() {
        return bytesArray.indexOf(MARKER, 0);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int indexOfPanama() {
        return bytesArray.indexOf(MARKER, 0);
    }

    @Benchmark
    public int withOffsetIndexOf() {
        return bytesArray.indexOf(MARKER, 1);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int withOffsetIndexPanama() {
        return bytesArray.indexOf(MARKER, 1);
    }
}
