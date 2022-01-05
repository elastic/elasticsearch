/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.indices.common;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
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
import org.openjdk.jmh.infra.Blackhole;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class RoundingBenchmark {
    private static final DateFormatter FORMATTER = DateFormatter.forPattern("date_optional_time");

    @Param(
        {
            "2000-01-01 to 2020-01-01", // A super long range
            "2000-10-01 to 2000-11-01", // A whole month which is pretty believable
            "2000-10-29 to 2000-10-30", // A date right around daylight savings time.
            "2000-06-01 to 2000-06-02"  // A date fully in one time zone. Should be much faster than above.
        }
    )
    public String range;

    @Param({ "java time", "es" })
    public String rounder;

    @Param({ "UTC", "America/New_York" })
    public String zone;

    @Param({ "calendar year", "calendar hour", "10d", "5d", "1h" })
    public String interval;

    @Param({ "1", "10000", "1000000", "100000000" })
    public int count;

    private long min;
    private long max;
    private long[] dates;
    private Supplier<Rounding.Prepared> rounderBuilder;

    @Setup
    public void buildDates() {
        String[] r = range.split(" to ");
        min = FORMATTER.parseMillis(r[0]);
        max = FORMATTER.parseMillis(r[1]);
        dates = new long[count];
        long date = min;
        long diff = (max - min) / dates.length;
        for (int i = 0; i < dates.length; i++) {
            if (date >= max) {
                throw new IllegalStateException("made a bad date [" + date + "]");
            }
            dates[i] = date;
            date += diff;
        }
        Rounding.Builder roundingBuilder;
        if (interval.startsWith("calendar ")) {
            roundingBuilder = Rounding.builder(
                DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.substring("calendar ".length()))
            );
        } else {
            roundingBuilder = Rounding.builder(TimeValue.parseTimeValue(interval, "interval"));
        }
        Rounding rounding = roundingBuilder.timeZone(ZoneId.of(zone)).build();
        rounderBuilder = switch (rounder) {
            case "java time" -> rounding::prepareJavaTime;
            case "es" -> () -> rounding.prepare(min, max);
            default -> throw new IllegalArgumentException("Expected rounder to be [java time] or [es]");
        };
    }

    @Benchmark
    public void round(Blackhole bh) {
        Rounding.Prepared rounder = rounderBuilder.get();
        for (int i = 0; i < dates.length; i++) {
            bh.consume(rounder.round(dates[i]));
        }
    }

    @Benchmark
    public void nextRoundingValue(Blackhole bh) {
        Rounding.Prepared rounder = rounderBuilder.get();
        for (int i = 0; i < dates.length; i++) {
            bh.consume(rounder.nextRoundingValue(dates[i]));
        }
    }
}
