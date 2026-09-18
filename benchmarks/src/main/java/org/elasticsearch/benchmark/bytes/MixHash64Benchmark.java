/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.bytes;

import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.bytes.MixHash64;
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3)
@State(Scope.Thread)
public class MixHash64Benchmark {

    @Param({ "4", "5", "6", "7", "8", "9", "12", "13", "16", "17", "31", "32", "33", "64", "128" })
    int length;

    private byte[] bytes;

    @Setup
    public void setup() {
        bytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(bytes);
    }

    @Benchmark
    public long mixHash64() {
        return MixHash64.hash64(bytes, 0, length);
    }

    @Benchmark
    public int murmur32() {
        return StringHelper.murmurhash3_x86_32(bytes, 0, length, StringHelper.GOOD_FAST_HASH_SEED);
    }
}
