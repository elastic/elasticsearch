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

    @Param({ "1", "5", "10", "50", "100", "500", "1000" })
    public int numCodePoints;

    @Param({ "ascii", "unicode" })
    public String type;

    private BytesRef bytesRef;

    @Setup
    public void setup() {
        String s = switch (type) {
            case "ascii" -> UTF8StringBytesBenchmark.generateAsciiString(numCodePoints);
            case "unicode" -> UTF8StringBytesBenchmark.generateUTF8String(numCodePoints);
            default -> throw new IllegalArgumentException("Unknown string type: " + type);
        };
        bytesRef = new BytesRef(s);
    }

    @Benchmark
    public int luceneUnicodeUtil() {
        return UnicodeUtil.codePointCount(bytesRef);
    }

    @Benchmark
    public int elasticsearchSwar() {
        return ESVectorUtil.codePointCount(bytesRef);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int elasticsearchPanamaSimd() {
        return ESVectorUtil.codePointCount(bytesRef);
    }
}
