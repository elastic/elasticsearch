package org.elasticsearch.common;

import org.elasticsearch.common.Rounding.DateTimeUnit;
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
import org.openjdk.jmh.infra.Blackhole;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

@Fork(3)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class RoundingBenchmark {
    private final int count = 1000;
    private final DateFormatter formatter = DateFormatter.forPattern("date_optional_time");
    private final long min = formatter.parseMillis("2000-01-01");
    private final long max = formatter.parseMillis("2021-01-01");
    private final Rounding rounding = Rounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(ZoneId.of("America/New_York")).build();
    private final LongUnaryOperator javaRounding = rounding.javaTimeRounder();
    private final LongUnaryOperator esRounding = rounding.rounder(min, max);

    // NOCOMMIT include building rounding in benchmark
    // NOCOMMIT a "short range" test and a "broad range" test
    // NOCOMMIT a "single round" test
    private long[] dates;

    public RoundingBenchmark() {
        dates = new long[1000];
        long date = min;
        long diff = (max - min) / (count + 1);
        for (int i = 0; i < dates.length; i++) {
            dates[i] = date;
            date += diff;
            assert date < max;
        }
    }

    @Benchmark
    public void roundWithJavaUtilTime(Blackhole bh) {
        for (int i = 0; i < dates.length; i++) {
            bh.consume(javaRounding.applyAsLong(dates[i]));
        }
    }

    @Benchmark
    public void roundWithES(Blackhole bh) {
        for (int i = 0; i < dates.length; i++) {
            bh.consume(esRounding.applyAsLong(dates[i]));
        }
    }
}
