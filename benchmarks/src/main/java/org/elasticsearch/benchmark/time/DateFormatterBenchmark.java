/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.time;

import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.time.DateFormatter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class DateFormatterBenchmark {

    private final DateFormatter javaFormatter = DateFormatter.forPattern("8year_month_day||ordinal_date||epoch_millis");
    private final DateFormatter jodaFormatter = Joda.forPattern("year_month_day||ordinal_date||epoch_millis");

    @Benchmark
    public TemporalAccessor parseJavaDate() {
        return javaFormatter.parse("1234567890");
    }

    @Benchmark
    public TemporalAccessor parseJodaDate() {
        return jodaFormatter.parse("1234567890");
    }
}
