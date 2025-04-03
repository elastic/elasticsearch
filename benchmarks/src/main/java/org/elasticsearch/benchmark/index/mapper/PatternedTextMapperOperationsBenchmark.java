/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.elasticsearch.xpack.patternedtext.PatternedTextValueProcessor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
public class PatternedTextMapperOperationsBenchmark {
    @Param(
        {
            "550e8400-e29b-41d4-a716-446655440000", // valid UUID
            "not-a-uuid", // early identifiable invalid UUID
            "123e4567-e89b-12d3-a456-4266141740000" // late identifiable invalid UUID
        }
    )
    public String uuid;

    @Benchmark
    public void testUuidMatchManual(Blackhole blackhole) {
        blackhole.consume(PatternedTextValueProcessor.isUUID_manual(uuid));
    }

    @Benchmark
    public void testUuidMatchManualWithValidation(Blackhole blackhole) {
        blackhole.consume(PatternedTextValueProcessor.isUUID_manual_withValidation(uuid));
    }

    @Benchmark
    public void testUuidMatchRegex(Blackhole blackhole) {
        blackhole.consume(PatternedTextValueProcessor.isUUID_regex(uuid));
    }

    @Param({ "172.16.0", "255.255.255.255" })
    public String ip;

    @Benchmark
    public void testIpv4MatchManual(Blackhole blackhole) {
        blackhole.consume(PatternedTextValueProcessor.isIpv4_manual(ip));
    }

    @Benchmark
    public void testIpv4MatchManual_Iterative(Blackhole blackhole) {
        blackhole.consume(PatternedTextValueProcessor.isIpv4_manual_iterative(ip));
    }

    @Benchmark
    public void testIpv4MatchRegex(Blackhole blackhole) {
        blackhole.consume(PatternedTextValueProcessor.isIpv4_regex(ip));
    }
}
