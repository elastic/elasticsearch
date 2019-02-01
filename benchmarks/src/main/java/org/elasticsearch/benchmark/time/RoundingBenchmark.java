/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.benchmark.time;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTimeZone;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.Rounding.DateTimeUnit.DAY_OF_MONTH;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") //invoked by benchmarking framework
public class RoundingBenchmark {

    private final ZoneId zoneId = ZoneId.of("Europe/Amsterdam");
    private final DateTimeZone timeZone = DateUtils.zoneIdToDateTimeZone(zoneId);

    private final long timestamp = 1548879021354L;

    private final org.elasticsearch.common.rounding.Rounding jodaRounding =
        org.elasticsearch.common.rounding.Rounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(timeZone).build();
    private final Rounding javaRounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY)
        .timeZone(zoneId).build();

    @Benchmark
    public long timeRoundingDateTimeUnitJoda() {
        return jodaRounding.round(timestamp);
    }

    @Benchmark
    public long timeRoundingDateTimeUnitJava() {
        return javaRounding.round(timestamp);
    }

    private final org.elasticsearch.common.rounding.Rounding jodaDayOfMonthRounding =
        org.elasticsearch.common.rounding.Rounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(timeZone).build();
    private final Rounding javaDayOfMonthRounding = Rounding.builder(DAY_OF_MONTH)
        .timeZone(zoneId).build();

    @Benchmark
    public long timeRoundingDateTimeUnitDayOfMonthJoda() {
        return jodaDayOfMonthRounding.round(timestamp);
    }

    @Benchmark
    public long timeRoundingDateTimeUnitDayOfMonthJava() {
        return javaDayOfMonthRounding.round(timestamp);
    }

    private final org.elasticsearch.common.rounding.Rounding timeIntervalRoundingJoda =
        org.elasticsearch.common.rounding.Rounding.builder(TimeValue.timeValueMinutes(60)).timeZone(timeZone).build();
    private final Rounding timeIntervalRoundingJava = Rounding.builder(TimeValue.timeValueMinutes(60))
        .timeZone(zoneId).build();

    @Benchmark
    public long timeIntervalRoundingJava() {
        return timeIntervalRoundingJava.round(timestamp);
    }

    @Benchmark
    public long timeIntervalRoundingJoda() {
        return timeIntervalRoundingJoda.round(timestamp);
    }

    private final org.elasticsearch.common.rounding.Rounding timeUnitRoundingUtcMonthOfYearJoda =
        org.elasticsearch.common.rounding.Rounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(DateTimeZone.UTC).build();
    private final Rounding timeUnitRoundingUtcMonthOfYearJava = Rounding.builder(DAY_OF_MONTH)
        .timeZone(ZoneOffset.UTC).build();

    @Benchmark
    public long timeUnitRoundingUtcMonthOfYearJava() {
        return timeUnitRoundingUtcMonthOfYearJava.round(timestamp);
    }

    @Benchmark
    public long timeUnitRoundingUtcMonthOfYearJoda() {
        return timeUnitRoundingUtcMonthOfYearJoda.round(timestamp);
    }
}
