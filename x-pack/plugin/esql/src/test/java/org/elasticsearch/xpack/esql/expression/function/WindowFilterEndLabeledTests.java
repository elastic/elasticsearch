/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link WindowFilter} over <em>end-labeled</em> (right-closed) buckets, i.e. buckets created with
 * {@link org.elasticsearch.common.Rounding.RoundingConvention#UP}. This is the bucketing used by PromQL range
 * queries (and the {@code TSTEP} grouping), where a bucket is identified by its right edge {@code B} and covers
 * {@code (B - bucket, B]}.
 * <p>
 * The sibling {@link WindowFilterTests} only covers start-labeled ({@code DOWN}) buckets.
 * For an end-labeled bucket, a sample at timestamp {@code ts} must be kept exactly when it falls in the trailing,
 * left-open {@code window} of its bucket, i.e. {@code ts > rightEdge(ts) - window}, where {@code rightEdge(ts)} is
 * the right boundary of the bucket containing {@code ts}.
 */
public class WindowFilterEndLabeledTests extends AbstractConfigurationFunctionTestCase {

    public WindowFilterEndLabeledTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // 5m bucket, 1m window: for the bucket (0m, 5m] only (4m, 5m] passes; for (5m, 10m] only (9m, 10m] passes.
        // The window is left-open, so its lower edge (4m, 9m) is EXCLUDED while the bucket's right edge (5m, 10m) is
        // INCLUDED. Note how this differs from the start-labeled convention.
        utcCase(suppliers, "5m/1m: 30s excluded", 5, 1, seconds(30), false);
        utcCase(suppliers, "5m/1m: 3m59s excluded", 5, 1, minutes(3) + seconds(59), false);
        utcCase(suppliers, "5m/1m: 4m excluded (window lower edge)", 5, 1, minutes(4), false);
        utcCase(suppliers, "5m/1m: 4m30s included", 5, 1, minutes(4) + seconds(30), true);
        utcCase(suppliers, "5m/1m: 4m59s included", 5, 1, minutes(4) + seconds(59), true);
        utcCase(suppliers, "5m/1m: 5m included (right edge)", 5, 1, minutes(5), true);
        utcCase(suppliers, "5m/1m: 5m30s excluded", 5, 1, minutes(5) + seconds(30), false);
        utcCase(suppliers, "5m/1m: 6m excluded", 5, 1, minutes(6), false);
        utcCase(suppliers, "5m/1m: 9m excluded (window lower edge)", 5, 1, minutes(9), false);
        utcCase(suppliers, "5m/1m: 9m59s included", 5, 1, minutes(9) + seconds(59), true);
        utcCase(suppliers, "5m/1m: 10m included (right edge)", 5, 1, minutes(10), true);

        // 5m bucket, 2m window: for the bucket (0m, 5m] only (3m, 5m] passes.
        utcCase(suppliers, "5m/2m: 2m59s excluded", 5, 2, minutes(2) + seconds(59), false);
        utcCase(suppliers, "5m/2m: 3m excluded (window lower edge)", 5, 2, minutes(3), false);
        utcCase(suppliers, "5m/2m: 4m included", 5, 2, minutes(4), true);
        utcCase(suppliers, "5m/2m: 5m included (right edge)", 5, 2, minutes(5), true);
        utcCase(suppliers, "5m/2m: 5m30s excluded", 5, 2, minutes(5) + seconds(30), false);

        // window == bucket: everything in the bucket passes (this case never reaches WindowFilter in production, but
        // the evaluator must still behave correctly when exercised directly).
        utcCase(suppliers, "5m/5m: 30s included", 5, 5, seconds(30), true);
        utcCase(suppliers, "5m/5m: 5m included", 5, 5, minutes(5), true);

        // Random timezone cases with an independently computed oracle for the end-labeled right edge.
        randomZoneCase(suppliers, "5m/1m random tz: 30s", 5, 1, seconds(30));
        randomZoneCase(suppliers, "5m/1m random tz: 4m30s", 5, 1, minutes(4) + seconds(30));
        randomZoneCase(suppliers, "5m/1m random tz: 5m", 5, 1, minutes(5));
        randomZoneCase(suppliers, "5m/2m random tz: 3m", 5, 2, minutes(3));

        // date_nanos timestamps: the left-open lower edge must be nudged by one nanosecond, not one millisecond,
        // so a sample one nanosecond above the window's lower edge is already inside the window.
        utcNanosCase(suppliers, "nanos 5m/1m: 4m excluded (window lower edge)", 5, 1, minuteNanos(4), false);
        utcNanosCase(suppliers, "nanos 5m/1m: 4m plus 1ns included", 5, 1, minuteNanos(4) + 1, true);
        utcNanosCase(suppliers, "nanos 5m/1m: 5m included (right edge)", 5, 1, minuteNanos(5), true);
        // Bucket membership is decided on the truncated millisecond value, matching how TSTEP groups date_nanos rows
        // (see DateTrunc#processDateNanos): a sample less than one millisecond above the right edge still belongs to
        // this bucket and its trailing window, while a sample one millisecond above falls into the next bucket.
        utcNanosCase(suppliers, "nanos 5m/1m: 5m plus 1ns included (truncated to right edge)", 5, 1, minuteNanos(5) + 1, true);
        utcNanosCase(suppliers, "nanos 5m/1m: 5m plus 1ms excluded", 5, 1, minuteNanos(5) + TimeUnit.MILLISECONDS.toNanos(1), false);
        utcNanosCase(suppliers, "nanos 5m/2m: 3m excluded (window lower edge)", 5, 2, minuteNanos(3), false);
        utcNanosCase(suppliers, "nanos 5m/2m: 3m plus 1ns included", 5, 2, minuteNanos(3) + 1, true);
        // The trailing minute of the final five-minute bucket starts after the date_nanos range.
        utcNanosCase(suppliers, "nanos 5m/1m: maximum timestamp excluded", 5, 1, DateUtils.MAX_NANOSECOND, false);

        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void utcCase(
        List<TestCaseSupplier> suppliers,
        String name,
        int bucketMinutes,
        int windowMinutes,
        long timestampMillis,
        boolean expected
    ) {
        Duration window = Duration.ofMinutes(windowMinutes);
        Duration bucket = Duration.ofMinutes(bucketMinutes);

        suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, DataType.TIME_DURATION, DataType.DATETIME), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(window, DataType.TIME_DURATION, "window").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(bucket, DataType.TIME_DURATION, "bucket").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(timestampMillis, DataType.DATETIME, "@timestamp"));
            return new TestCaseSupplier.TestCase(
                args,
                Matchers.startsWith("WindowFilterEvaluator[window=" + window.toMillis()),
                DataType.BOOLEAN,
                equalTo(expected)
            ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC));
        }));
    }

    private static void utcNanosCase(
        List<TestCaseSupplier> suppliers,
        String name,
        int bucketMinutes,
        int windowMinutes,
        long timestampNanos,
        boolean expected
    ) {
        Duration window = Duration.ofMinutes(windowMinutes);
        Duration bucket = Duration.ofMinutes(bucketMinutes);

        suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, DataType.TIME_DURATION, DataType.DATE_NANOS), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(window, DataType.TIME_DURATION, "window").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(bucket, DataType.TIME_DURATION, "bucket").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(timestampNanos, DataType.DATE_NANOS, "@timestamp"));
            return new TestCaseSupplier.TestCase(
                args,
                Matchers.startsWith("WindowFilterDateNanosEvaluator[window=" + window.toMillis()),
                DataType.BOOLEAN,
                equalTo(expected)
            ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC));
        }));
    }

    private static void randomZoneCase(
        List<TestCaseSupplier> suppliers,
        String name,
        int bucketMinutes,
        int windowMinutes,
        long timestampMillis
    ) {
        Duration window = Duration.ofMinutes(windowMinutes);
        Duration bucket = Duration.ofMinutes(bucketMinutes);

        suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, DataType.TIME_DURATION, DataType.DATETIME), () -> {
            ZoneId zone = randomZone();
            // Left-open trailing window (rightEdge - window, rightEdge]: the lower edge is excluded, so use strict >.
            boolean expected = timestampMillis > endLabeledRightEdge(timestampMillis, bucket, zone) - window.toMillis();

            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(window, DataType.TIME_DURATION, "window").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(bucket, DataType.TIME_DURATION, "bucket").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(timestampMillis, DataType.DATETIME, "@timestamp"));
            return new TestCaseSupplier.TestCase(
                args,
                Matchers.startsWith("WindowFilterEvaluator[window=" + window.toMillis()),
                DataType.BOOLEAN,
                equalTo(expected)
            ).withConfiguration(TEST_SOURCE, configurationForTimezone(zone));
        }));
    }

    /**
     * Independent oracle (does not use the UP rounding under test) for the right boundary of the right-closed bucket
     * containing {@code timestampMillis}: the floor grid point if {@code timestamp} sits exactly on the grid, otherwise
     * the next grid point above the floor.
     */
    private static long endLabeledRightEdge(long timestampMillis, Duration bucket, ZoneId zone) {
        Rounding.Prepared floorRounding = Rounding.builder(TimeValue.timeValueMillis(bucket.toMillis()))
            .timeZone(zone)
            .build()
            .prepareForUnknown();
        long floor = floorRounding.round(timestampMillis);
        return floor == timestampMillis ? timestampMillis : floorRounding.nextRoundingValue(floor);
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        Expression window = args.get(0);
        Expression bucketSize = args.get(1);
        Expression timestamp = args.get(2);
        Bucket bucket = new Bucket(source, timestamp, bucketSize, null, null, configuration, 0L, Rounding.RoundingConvention.UP);
        return new WindowFilter(source, window, bucket, timestamp);
    }

    @Override
    public void testFold() {
        assumeTrue("WindowFilter is not foldable", false);
    }

    private static long minutes(long m) {
        return TimeUnit.MINUTES.toMillis(m);
    }

    private static long seconds(long s) {
        return TimeUnit.SECONDS.toMillis(s);
    }

    private static long minuteNanos(long m) {
        return TimeUnit.MINUTES.toNanos(m);
    }
}
