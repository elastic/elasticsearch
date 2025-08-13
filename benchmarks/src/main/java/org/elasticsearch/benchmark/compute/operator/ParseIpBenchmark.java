/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ParseIp;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class ParseIpBenchmark {
    private final BytesRef ip = new BytesRef("192.168.0.1");
    private final BreakingBytesRefBuilder scratch = ParseIp.buildScratch(new NoopCircuitBreaker("request"));

    @Benchmark
    public BytesRef leadingZerosRejected() {
        return ParseIp.leadingZerosRejected(ip, scratch);
    }

    @Benchmark
    public BytesRef leadingZerosAreDecimal() {
        return ParseIp.leadingZerosAreDecimal(ip, scratch);
    }

    @Benchmark
    public BytesRef leadingZerosAreOctal() {
        return ParseIp.leadingZerosAreOctal(ip, scratch);
    }

    @Benchmark
    public BytesRef original() {
        InetAddress inetAddress = InetAddresses.forString(ip.utf8ToString());
        return new BytesRef(InetAddressPoint.encode(inetAddress));
    }
}
