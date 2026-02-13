/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.logging.LogConfigurator;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class CodePointCountBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Param({ "1", "5", "10", "20", "50", "100", "1000" })
    public int avgNumCodePoints;

    @Param({ "ascii", "unicode" })
    public String type;

    private BytesRef[] data;
    private int index = 0;
    private static final int NUM_VALUES = 1024;

    @Setup
    public void setup() {
        data = new BytesRef[NUM_VALUES];
        Random random = new Random(1);

        for (int i = 0; i < NUM_VALUES; i++) {
            int numCodePoints = random.nextInt(avgNumCodePoints * 2);
            String s = type.equals("ascii")
                ? UTF8StringBytesBenchmark.generateAsciiString(numCodePoints)
                : UTF8StringBytesBenchmark.generateUTF8String(numCodePoints);
            data[i] = new BytesRef(s);
        }
    }

    @Benchmark
    public int luceneUnicodeUtil() {
        BytesRef bytes = data[index++ & (NUM_VALUES - 1)];
        return UnicodeUtil.codePointCount(bytes);
    }

    @Benchmark
    public int elasticsearchSwar() {
        BytesRef bytes = data[index++ & (NUM_VALUES - 1)];
        return ESVectorUtil.codePointCount(bytes);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int elasticsearchPanamaSimd() {
        BytesRef bytes = data[index++ & (NUM_VALUES - 1)];
        return ESVectorUtil.codePointCount(bytes);
    }
}
