package org.elasticsearch.common;

import org.elasticsearch.common.time.DateFormatter;
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
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class RoundingBenchmark {
    private static final DateFormatter FORMATTER = DateFormatter.forPattern("date_optional_time");

    @Param({
        "2000-01-01 to 2020-01-01",
        "2000-01-01 to 2001-01-01",
        "2000-01-01 to 2000-01-02",
        "2000-10-29 to 2000-10-30"
    })
    public String range;

    @Param({"java time", "es"})
    public String rounder;

    @Param({"America/New_York"})
    public String zone;

    @Param({"MONTH_OF_YEAR", "HOUR_OF_DAY"})
    public String timeUnit;

    @Param({"1", "1000000"})
    public int count;

    private long min;
    private long max; 
    private long[] dates;
    private Supplier<PreparedRounding> rounderBuilder;

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
        Rounding rounding = Rounding.builder(Rounding.DateTimeUnit.valueOf(timeUnit)).timeZone(ZoneId.of(zone)).build();
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
        PreparedRounding rounder = rounderBuilder.get();
        for (int i = 0; i < dates.length; i++) {
            bh.consume(rounder.round(dates[i]));
        }
    }

    @Benchmark
    public void nextRoundingValue(Blackhole bh) {
        PreparedRounding rounder = rounderBuilder.get();
        for (int i = 0; i < dates.length; i++) {
            bh.consume(rounder.nextRoundingValue(dates[i]));
        }
    }
}
