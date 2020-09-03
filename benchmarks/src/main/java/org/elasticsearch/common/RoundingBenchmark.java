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

package org.elasticsearch.common;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.TimeValue;
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

    @Param({
        "2000-01-01 to 2020-01-01", // A super long range
        "2000-10-01 to 2000-11-01", // A whole month which is pretty believable
        "2000-10-29 to 2000-10-30", // A date right around daylight savings time.
        "2000-06-01 to 2000-06-02"  // A date fully in one time zone. Should be much faster than above.
    })
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
        switch (rounder) {
            case "java time":
                rounderBuilder = rounding::prepareJavaTime;
                break;
            case "es":
                rounderBuilder = () -> rounding.prepare(min, max);
                break;
            default:
                throw new IllegalArgumentException("Expectd rounder to be [java time] or [es]");
        }
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
