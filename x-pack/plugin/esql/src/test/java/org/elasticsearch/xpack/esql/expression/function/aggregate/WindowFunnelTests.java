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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

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
        return new WindowFunnel(source, args.get(0), args.get(1), args.subList(2, args.size()), testCase.getConfiguration());
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    public void testSurrogateShape() {
        WindowFunnel funnel = (WindowFunnel) buildFieldExpression(testCase);
        int steps = funnel.conditions().size();
        Coalesce coalesce = as(funnel.surrogate(), Coalesce.class);
        assertThat(coalesce.children(), hasSize(2));
        assertThat(as(coalesce.children().get(1), Literal.class).value(), equalTo(0));
        WindowFunnelLevel level = as(coalesce.children().getFirst(), WindowFunnelLevel.class);
        assertThat(as(level.steps(), Literal.class).value(), equalTo(steps));
        assertThat(level.timestamp(), sameInstance(funnel.timestamp()));
        assertThat(level.maxspan(), sameInstance(funnel.maxspan()));
        List<Case> stepMasks = level.mask().collect(Case.class);
        assertThat(stepMasks, hasSize(steps));
        for (int step = 0; step < steps; step++) {
            Expression condition = funnel.conditions().get(step);
            Case stepMask = stepMasks.stream().filter(c -> c.children().getFirst().equals(condition)).findFirst().orElseThrow();
            assertThat(as(stepMask.children().get(1), Literal.class).value(), equalTo(1L << step));
            assertThat(as(stepMask.children().get(2), Literal.class).value(), equalTo(0L));
        }
    }

    public void testSurrogateMaskUsesFunnelConfiguration() {
        WindowFunnel funnel = (WindowFunnel) buildFieldExpression(testCase);
        List<Add> adds = funnel.surrogate().collect(Add.class);
        assertThat(adds, hasSize(funnel.conditions().size() - 1));
        for (Add add : adds) {
            assertThat(add.configuration(), sameInstance(funnel.configuration()));
        }
    }
}
