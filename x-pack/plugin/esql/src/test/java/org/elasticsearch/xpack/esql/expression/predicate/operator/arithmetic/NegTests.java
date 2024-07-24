/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class NegTests extends AbstractScalarFunctionTestCase {

    public NegTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "NegIntsEvaluator[v=Attribute[channel=0]]",
            DataType.INTEGER,
            Math::negateExact,
            Integer.MIN_VALUE + 1,
            Integer.MAX_VALUE,
            List.of()
        );
        // out of bounds integer
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "NegIntsEvaluator[v=Attribute[channel=0]]",
            DataType.INTEGER,
            z -> null,
            Integer.MIN_VALUE,
            Integer.MIN_VALUE,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: integer overflow"
            )
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "NegLongsEvaluator[v=Attribute[channel=0]]",
            DataType.LONG,
            Math::negateExact,
            Long.MIN_VALUE + 1,
            Long.MAX_VALUE,
            List.of()
        );
        // out of bounds long
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "NegLongsEvaluator[v=Attribute[channel=0]]",
            DataType.LONG,
            z -> null,
            Long.MIN_VALUE,
            Long.MIN_VALUE,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: long overflow"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "NegDoublesEvaluator[v=Attribute[channel=0]]",
            DataType.DOUBLE,
            // TODO: Probably we don't want to allow negative zeros
            d -> -d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        // TODO: Wire up edge case generation functions for these
        suppliers.addAll(List.of(new TestCaseSupplier("Duration", List.of(DataType.TIME_DURATION), () -> {
            Duration arg = (Duration) randomLiteral(DataType.TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.TIME_DURATION, "arg")),
                "No evaluator since this expression is only folded",
                DataType.TIME_DURATION,
                equalTo(arg.negated())
            );
        }), new TestCaseSupplier("Period", List.of(DataType.DATE_PERIOD), () -> {
            Period arg = (Period) randomLiteral(DataType.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.DATE_PERIOD, "arg")),
                "No evaluator since this expression is only folded",
                DataType.DATE_PERIOD,
                equalTo(arg.negated())
            );
        })));
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers, (v, p) -> "numeric, date_period or time_duration");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Neg(source, args.get(0));
    }

    public void testEdgeCases() {
        // Run the assertions for the current test cases type only to avoid running the same assertions multiple times.
        // TODO: These remaining cases should get rolled into generation functions for periods and durations
        DataType testCaseType = testCase.getData().get(0).type();
        if (testCaseType == DataType.DATE_PERIOD) {
            Period maxPeriod = Period.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
            Period negatedMaxPeriod = Period.of(-Integer.MAX_VALUE, -Integer.MAX_VALUE, -Integer.MAX_VALUE);
            assertEquals(negatedMaxPeriod, process(maxPeriod));

            Period minPeriod = Period.of(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
            VerificationException e = expectThrows(
                VerificationException.class,
                "Expected exception when negating minimal date period.",
                () -> process(minPeriod)
            );
            assertEquals(e.getMessage(), "arithmetic exception in expression []: [integer overflow]");
        } else if (testCaseType == DataType.TIME_DURATION) {
            Duration maxDuration = Duration.ofSeconds(Long.MAX_VALUE, 0);
            Duration negatedMaxDuration = Duration.ofSeconds(-Long.MAX_VALUE, 0);
            assertEquals(negatedMaxDuration, process(maxDuration));

            Duration minDuration = Duration.ofSeconds(Long.MIN_VALUE, 0);
            VerificationException e = expectThrows(
                VerificationException.class,
                "Expected exception when negating minimal time duration.",
                () -> process(minDuration)
            );
            assertEquals(
                e.getMessage(),
                "arithmetic exception in expression []: [Exceeds capacity of Duration: 9223372036854775808000000000]"
            );
        }
    }

    private Object process(Object val) {
        if (testCase.canBuildEvaluator()) {
            Neg neg = new Neg(Source.EMPTY, field("val", typeOf(val)));
            try (Block block = evaluator(neg).get(driverContext()).eval(row(List.of(val)))) {
                return toJavaObject(block, 0);
            }
        } else { // just fold if type is not representable
            Neg neg = new Neg(Source.EMPTY, new Literal(Source.EMPTY, val, typeOf(val)));
            return neg.fold();
        }
    }

    private static DataType typeOf(Object val) {
        if (val instanceof Integer) {
            return DataType.INTEGER;
        }
        if (val instanceof Long) {
            return DataType.LONG;
        }
        if (val instanceof Double) {
            return DataType.DOUBLE;
        }
        if (val instanceof Duration) {
            return DataType.TIME_DURATION;
        }
        if (val instanceof Period) {
            return DataType.DATE_PERIOD;
        }
        throw new UnsupportedOperationException("unsupported type [" + val.getClass() + "]");
    }
}
