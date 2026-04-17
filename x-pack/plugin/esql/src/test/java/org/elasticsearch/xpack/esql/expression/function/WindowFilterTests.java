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

public class WindowFilterTests extends AbstractConfigurationFunctionTestCase {

    public WindowFilterTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // 5m bucket, 1m window, UTC: only [4m, 5m) +5m pass
        utcCase(suppliers, "5m/1m: 0s excluded", 5, 1, minutes(0), false);
        utcCase(suppliers, "5m/1m: 30s excluded", 5, 1, seconds(30), false);
        utcCase(suppliers, "5m/1m: 1m excluded", 5, 1, minutes(1), false);
        utcCase(suppliers, "5m/1m: 3m59s excluded", 5, 1, minutes(3) + seconds(59), false);
        utcCase(suppliers, "5m/1m: 4m included", 5, 1, minutes(4), true);
        utcCase(suppliers, "5m/1m: 4m30s included", 5, 1, minutes(4) + seconds(30), true);
        utcCase(suppliers, "5m/1m: 4m59s included", 5, 1, minutes(4) + seconds(59), true);
        utcCase(suppliers, "5m/1m: 5m excluded", 5, 1, minutes(5), false);
        utcCase(suppliers, "5m/1m: 6m excluded", 5, 1, minutes(6), false);
        utcCase(suppliers, "5m/1m: 9m included", 5, 1, minutes(9), true);
        utcCase(suppliers, "5m/1m: 9m59s included", 5, 1, minutes(9) + seconds(59), true);

        // 5m bucket, 2m window, UTC: only [3m, 5m) passes
        utcCase(suppliers, "5m/2m: 2m59s excluded", 5, 2, minutes(2) + seconds(59), false);
        utcCase(suppliers, "5m/2m: 3m included", 5, 2, minutes(3), true);
        utcCase(suppliers, "5m/2m: 4m included", 5, 2, minutes(4), true);
        utcCase(suppliers, "5m/2m: 5m excluded", 5, 2, minutes(5), false);

        // 5m bucket, 5m window, UTC: everything passes
        utcCase(suppliers, "5m/5m: 0s included", 5, 5, 0L, true);
        utcCase(suppliers, "5m/5m: 2m30s included", 5, 5, minutes(2) + seconds(30), true);
        utcCase(suppliers, "5m/5m: 4m59s included", 5, 5, minutes(4) + seconds(59), true);
        utcCase(suppliers, "5m/5m: 5m included", 5, 5, minutes(5), true);

        // Window larger than bucket, UTC: everything passes
        utcCase(suppliers, "5m/10m: 0s included", 5, 10, minutes(0), true);
        utcCase(suppliers, "5m/10m: 2m included", 5, 10, minutes(2), true);
        utcCase(suppliers, "5m/10m: 4m59s included", 5, 10, minutes(4) + seconds(59), true);
        utcCase(suppliers, "5m/10m: 5m included", 5, 10, minutes(5), true);
        utcCase(suppliers, "5m/10m: 6m included", 5, 10, minutes(6), true);
        utcCase(suppliers, "5m/10m: 10m included", 5, 10, minutes(10), true);
        utcCase(suppliers, "5m/10m: 11m included", 5, 10, minutes(11), true);
        utcCase(suppliers, "5m/10m: 15m", 5, 10, minutes(15), true);
        utcCase(suppliers, "5m/10m: 16m", 5, 10, minutes(16), true);
        utcCase(suppliers, "1m/5m: 0s included", 1, 5, seconds(0), true);
        utcCase(suppliers, "1m/5m: 30s included", 1, 5, seconds(30), true);
        utcCase(suppliers, "1m/5m: 59s included", 1, 5, seconds(59), true);
        utcCase(suppliers, "1m/5m: 1m included", 1, 5, minutes(1), true);
        utcCase(suppliers, "1m/5m: 5m included", 1, 5, minutes(5), true);
        utcCase(suppliers, "1m/5m: 5m30s included", 1, 5, minutes(6) + seconds(30), true);
        utcCase(suppliers, "1m/5m: 6m included", 1, 5, minutes(6), true);
        utcCase(suppliers, "1m/5m: 7m included", 1, 5, minutes(7), true);

        // Random timezone cases with dynamically computed expected values
        randomZoneCase(suppliers, "5m/1m random tz: 0s", 5, 1, minutes(0));
        randomZoneCase(suppliers, "5m/1m random tz: 30s", 5, 1, seconds(30));
        randomZoneCase(suppliers, "5m/1m random tz: 3m59s", 5, 1, minutes(3) + seconds(59));
        randomZoneCase(suppliers, "5m/1m random tz: 4m", 5, 1, minutes(4));
        randomZoneCase(suppliers, "5m/1m random tz: 4m30s", 5, 1, minutes(4) + seconds(30));
        randomZoneCase(suppliers, "5m/1m random tz: 5m", 5, 1, minutes(5));
        randomZoneCase(suppliers, "5m/1m random tz: 6m", 5, 1, minutes(6));
        randomZoneCase(suppliers, "1m/5m random tz: 0s", 1, 5, seconds(0));
        randomZoneCase(suppliers, "1m/5m random tz: 59s", 1, 5, seconds(59));
        randomZoneCase(suppliers, "1m/5m random tz: 1m", 1, 5, minutes(1));
        randomZoneCase(suppliers, "1m/5m random tz: 2m", 1, 5, minutes(2));
        randomZoneCase(suppliers, "1m/5m random tz: 5m", 1, 5, minutes(5));
        randomZoneCase(suppliers, "1m/5m random tz: 5m30s", 1, 5, minutes(5) + seconds(30));
        randomZoneCase(suppliers, "1m/5m random tz: 6m", 1, 5, minutes(6));
        randomZoneCase(suppliers, "1m/5m random tz: 7m", 1, 5, minutes(7));

        for (int i = 0; i < 10; i++) {
            fullyRandomCase(suppliers, "random case " + i);
        }

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
                Matchers.startsWith(
                    "WindowFilterEvaluator[window=" + window.toMillis() + ", bucket=Rounding[" + bucket.toMillis() + " in Z][fixed]"
                ),
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
            Rounding.Prepared prepared = Rounding.builder(TimeValue.timeValueMillis(bucket.toMillis()))
                .timeZone(zone)
                .build()
                .prepareForUnknown();
            long bucketStart = prepared.round(timestampMillis);
            long bucketEnd = prepared.nextRoundingValue(bucketStart);
            boolean expected = timestampMillis >= bucketEnd - window.toMillis();

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

    private static void fullyRandomCase(List<TestCaseSupplier> suppliers, String name) {
        suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, DataType.TIME_DURATION, DataType.DATETIME), () -> {
            int bucketMinutes = randomIntBetween(1, 60);
            int windowMinutes = randomIntBetween(1, 120);
            Duration window = Duration.ofMinutes(windowMinutes);
            Duration bucket = Duration.ofMinutes(bucketMinutes);
            long timestampMillis = randomLongBetween(0, minutes(bucketMinutes * 100));
            ZoneId zone = randomZone();

            Rounding.Prepared prepared = Rounding.builder(TimeValue.timeValueMillis(bucket.toMillis()))
                .timeZone(zone)
                .build()
                .prepareForUnknown();
            long bucketStart = prepared.round(timestampMillis);
            long bucketEnd = prepared.nextRoundingValue(bucketStart);
            boolean expected = timestampMillis >= bucketEnd - window.toMillis();

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

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        Expression window = args.get(0);
        Expression bucketSize = args.get(1);
        Expression timestamp = args.get(2);
        Bucket bucket = new Bucket(source, timestamp, bucketSize, null, null, configuration);
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
}
