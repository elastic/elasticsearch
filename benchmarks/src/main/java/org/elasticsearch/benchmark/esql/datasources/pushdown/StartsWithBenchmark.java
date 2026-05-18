/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.xpack.esql.datasources.pushdown.ByteMatchers;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the pre-existing scalar {@code startsWith} byte loop in {@code ParquetPushedExpressions}
 * against {@link ByteMatchers#startsWith}, which routes through the JDK-intrinsified
 * {@link java.util.Arrays#equals(byte[], int, int, byte[], int, int)}.
 *
 * <p>The sweep covers prefix lengths typical of URL/path columns (4-32 bytes) where the JDK
 * partial-inlining path applies. The intrinsic should win or tie at every length; if it loses
 * for very short prefixes (the open-coded loop has zero JNI/stub overhead) this benchmark
 * documents the threshold so a future hybrid dispatcher could route accordingly.
 *
 * <p>A self-test runs at setup time to confirm the two implementations agree on every value.
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class StartsWithBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Prefix length in bytes. Sweep brackets the JDK partial-inline window (~32 bytes). */
    @Param({ "4", "8", "16", "32" })
    public int prefixLength;

    /** Average value length in bytes. Picked to be larger than the prefix in every case. */
    @Param({ "32", "80" })
    public int valueLength;

    private static final int NUM_VALUES = 1024;

    private BytesRef[] values;
    private BytesRef prefix;
    private int index;

    @Setup
    public void setup() {
        Random random = new Random(1L);
        // The prefix is the same across all values so the predicate has a 50% hit rate (we flip
        // a coin per value for whether to start with the prefix).
        byte[] prefixBytes = randomAsciiBytes(random, prefixLength);
        prefix = new BytesRef(prefixBytes);
        values = new BytesRef[NUM_VALUES];
        for (int i = 0; i < NUM_VALUES; i++) {
            int targetLen = Math.max(prefixLength, valueLength + random.nextInt(8) - 4);
            byte[] bytes = new byte[targetLen];
            if (random.nextBoolean()) {
                System.arraycopy(prefixBytes, 0, bytes, 0, prefixLength);
                fillRandomAsciiBytes(random, bytes, prefixLength);
            } else {
                fillRandomAsciiBytes(random, bytes, 0);
                // Ensure the first byte differs so we don't accidentally start with the prefix.
                bytes[0] = (byte) (((bytes[0] - 'a' + 1) % 26) + 'a');
            }
            values[i] = new BytesRef(bytes);
        }
        // Self-test: scalar and intrinsic must agree on every value.
        for (int i = 0; i < NUM_VALUES; i++) {
            BytesRef v = values[i];
            boolean scalarAns = scalarStartsWith(v, prefix);
            boolean intrinsicAns = ByteMatchers.startsWith(v, prefix);
            if (scalarAns != intrinsicAns) {
                throw new AssertionError("Disagreement at value " + i);
            }
        }
    }

    @Benchmark
    public boolean scalar() {
        int idx = index++ % NUM_VALUES;
        return scalarStartsWith(values[idx], prefix);
    }

    @Benchmark
    public boolean intrinsic() {
        int idx = index++ % NUM_VALUES;
        return ByteMatchers.startsWith(values[idx], prefix);
    }

    /** Verbatim copy of the pre-existing open-coded scalar loop in ParquetPushedExpressions. */
    private static boolean scalarStartsWith(BytesRef value, BytesRef prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int j = 0; j < prefix.length; j++) {
            if (value.bytes[value.offset + j] != prefix.bytes[prefix.offset + j]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] randomAsciiBytes(Random random, int length) {
        byte[] out = new byte[length];
        fillRandomAsciiBytes(random, out, 0);
        return out;
    }

    private static void fillRandomAsciiBytes(Random random, byte[] target, int from) {
        for (int i = from; i < target.length; i++) {
            target[i] = (byte) ('a' + random.nextInt(26));
        }
    }
}
