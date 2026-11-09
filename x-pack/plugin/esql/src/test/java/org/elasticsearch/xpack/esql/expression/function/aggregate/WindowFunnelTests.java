/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class WindowFunnelTests extends AbstractAggregationTestCase {
    public WindowFunnelTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType timestampType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            for (int steps : List.of(1, 2, 3, WindowFunnel.MAX_CONDITIONS)) {
                suppliers.add(makeSupplier(timestampType, steps));
            }
        }
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier makeSupplier(DataType timestampType, int steps) {
        List<DataType> types = new ArrayList<>(List.of(DataType.TIME_DURATION, timestampType));
        types.addAll(IntStream.range(0, steps).mapToObj(i -> DataType.BOOLEAN).toList());
        return new TestCaseSupplier("window_funnel with " + timestampType + " and " + steps + " conditions", types, () -> {
            int rows = between(0, 100);
            boolean nanos = timestampType == DataType.DATE_NANOS;
            long unitsPerSecond = nanos ? 1_000_000_000L : 1_000L;
            boolean coarse = randomBoolean();
            Duration maxspan = randomFrom(
                Duration.ofSeconds(between(0, coarse ? 10 : 300)),
                Duration.ofMillis(between(0, 300_000)),
                Duration.ofDays(400 * 365)
            );
            long span;
            try {
                span = nanos ? maxspan.toNanos() : maxspan.toMillis();
            } catch (ArithmeticException e) {
                span = Long.MAX_VALUE;
            }
            List<Long> timestamps = IntStream.range(0, rows)
                .mapToObj(i -> coarse ? between(-10, 10) * unitsPerSecond : randomLongBetween(-300 * unitsPerSecond, 300 * unitsPerSecond))
                .toList();
            List<List<Boolean>> conditions = IntStream.range(0, steps)
                .mapToObj(s -> IntStream.range(0, rows).mapToObj(i -> randomBoolean()).toList())
                .toList();

            List<TestCaseSupplier.TypedData> data = new ArrayList<>();
            data.add(new TestCaseSupplier.TypedData(maxspan, DataType.TIME_DURATION, "maxspan").forceLiteral());
            data.add(TestCaseSupplier.TypedData.multiRow(timestamps, timestampType, "timestamp"));
            for (int s = 0; s < steps; s++) {
                data.add(TestCaseSupplier.TypedData.multiRow(conditions.get(s), DataType.BOOLEAN, "condition" + s));
            }
            return new TestCaseSupplier.TestCase(
                data,
                "WindowFunnel",
                DataType.INTEGER,
                equalTo(expectedLevel(timestamps, conditions, span))
            );
        });
    }

    private static int expectedLevel(List<Long> timestamps, List<List<Boolean>> conditions, long span) {
        int best = 0;
        for (int start = 0; start < timestamps.size(); start++) {
            if (conditions.getFirst().get(start) == false) {
                continue;
            }
            long previous = timestamps.get(start);
            long deadline = previous > Long.MAX_VALUE - span ? Long.MAX_VALUE : previous + span;
            int level = 1;
            while (level < conditions.size()) {
                Long next = null;
                for (int row = 0; row < timestamps.size(); row++) {
                    long timestamp = timestamps.get(row);
                    if (conditions.get(level).get(row)
                        && timestamp >= previous
                        && timestamp <= deadline
                        && (next == null || timestamp < next)) {
                        next = timestamp;
                    }
                }
                if (next == null) {
                    break;
                }
                previous = next;
                level++;
            }
            best = Math.max(best, level);
        }
        return best;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new WindowFunnel(source, args.get(0), args.get(1), args.subList(2, args.size()));
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }
}
