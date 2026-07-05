/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.util;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.simdvec.ESVectorUtil;
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

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class ContainsBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "20", "35", "50", "100", "500", "1000" })
    public int avgValueLength;

    @Param({ "5", "10", "20" })
    public int avgTermLength;

    private byte[][] values;
    private byte[][] terms;
    private int index = 0;
    private static final int NUM_VALUES = 1024;

    @Setup
    public void setup() {
        values = new byte[NUM_VALUES][];
        terms = new byte[NUM_VALUES][];
        Random random = new Random(1);

        for (int i = 0; i < NUM_VALUES; i++) {
            int valueLen = Math.max(1, random.nextInt(avgValueLength * 2));
            String value = UTF8StringBytesBenchmark.generateAsciiString(valueLen);
            values[i] = value.getBytes(StandardCharsets.UTF_8);

            int termLen = Math.max(1, Math.min(random.nextInt(avgTermLength * 2), valueLen));
            if (random.nextBoolean()) {
                int start = random.nextInt(valueLen - termLen + 1);
                terms[i] = new byte[termLen];
                System.arraycopy(values[i], start, terms[i], 0, termLen);
            } else {
                String term = UTF8StringBytesBenchmark.generateAsciiString(termLen);
                terms[i] = term.getBytes(StandardCharsets.UTF_8);
            }
        }
    }

    @Benchmark
    public boolean scalar() {
        int idx = index++ & (NUM_VALUES - 1);
        byte[] value = values[idx];
        byte[] term = terms[idx];
        return scalarContains(value, 0, value.length, term, 0, term.length);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public boolean panamaSimd() {
        int idx = index++ & (NUM_VALUES - 1);
        byte[] value = values[idx];
        byte[] term = terms[idx];
        return ESVectorUtil.contains(value, 0, value.length, term, 0, term.length);
    }

    static boolean scalarContains(byte[] value, int valueOffset, int valueLength, byte[] term, int termOffset, int termLength) {
        if (termLength == 0) {
            return true;
        }
        if (termLength > valueLength) {
            return false;
        }
        byte first = term[termOffset];
        int max = valueOffset + valueLength - termLength;
        for (int i = valueOffset; i <= max; i++) {
            if (value[i] != first) {
                while (++i <= max && value[i] != first)
                    ;
            }
            if (i <= max) {
                int j = i + 1;
                int end = j + termLength - 1;
                for (int k = termOffset + 1; j < end && value[j] == term[k]; j++, k++)
                    ;
                if (j == end) {
                    return true;
                }
            }
        }
        return false;
    }
}
