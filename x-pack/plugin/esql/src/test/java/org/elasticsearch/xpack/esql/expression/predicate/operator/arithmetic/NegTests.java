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
import org.elasticsearch.xpack.esql.analysis.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
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
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Integer", () -> {
            // Ensure we don't have an overflow
            int arg = randomIntBetween((Integer.MIN_VALUE + 1), Integer.MAX_VALUE);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataTypes.INTEGER, "arg")),
                "NegIntsEvaluator[v=Attribute[channel=0]]",
                DataTypes.INTEGER,
                equalTo(Math.negateExact(arg))
            );
        }), new TestCaseSupplier("Long", () -> {
            // Ensure we don't have an overflow
            long arg = randomLongBetween((Long.MIN_VALUE + 1), Long.MAX_VALUE);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataTypes.LONG, "arg")),
                "NegLongsEvaluator[v=Attribute[channel=0]]",
                DataTypes.LONG,
                equalTo(Math.negateExact(arg))
            );
        }), new TestCaseSupplier("Double", () -> {
            double arg = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataTypes.DOUBLE, "arg")),
                "NegDoublesEvaluator[v=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(-arg)
            );
        }), new TestCaseSupplier("Duration", () -> {
            Duration arg = (Duration) randomLiteral(EsqlDataTypes.TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, EsqlDataTypes.TIME_DURATION, "arg")),
                "No evaluator since this expression is only folded",
                EsqlDataTypes.TIME_DURATION,
                equalTo(arg.negated())
            );
        }), new TestCaseSupplier("Period", () -> {
            Period arg = (Period) randomLiteral(EsqlDataTypes.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, EsqlDataTypes.DATE_PERIOD, "arg")),
                "No evaluator since this expression is only folded",
                EsqlDataTypes.DATE_PERIOD,
                equalTo(arg.negated())
            );
        })));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Neg(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        // More precisely: numerics without unsigned longs; however, `Neg::resolveType` uses `numeric`.
        List<DataType> types = new ArrayList<>(Arrays.asList(numerics()));
        types.add(EsqlDataTypes.DATE_PERIOD);
        types.add(EsqlDataTypes.TIME_DURATION);
        return List.of(required(types.toArray(DataType[]::new)));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    public void testEdgeCases() {
        // Run the assertions for the current test cases type only to avoid running the same assertions multiple times.
        DataType testCaseType = testCase.getData().get(0).type();
        if (testCaseType.equals(DataTypes.INTEGER)) {
            assertEquals(null, process(Integer.MIN_VALUE));
            assertCriticalWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: integer overflow"
            );

            return;
        }
        if (testCaseType.equals(DataTypes.LONG)) {
            assertEquals(null, process(Long.MIN_VALUE));
            assertCriticalWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: long overflow"
            );

            return;
        }
        if (testCaseType.equals(DataTypes.DOUBLE)) {
            var negMaxValue = -Double.MAX_VALUE;
            assertEquals(negMaxValue, process(Double.MAX_VALUE));
            assertEquals(Double.MAX_VALUE, process(negMaxValue));

            var negMinValue = -Double.MIN_VALUE;
            assertEquals(negMinValue, process(Double.MIN_VALUE));
            assertEquals(Double.MIN_VALUE, process(negMinValue));

            assertEquals(Double.NEGATIVE_INFINITY, process(Double.POSITIVE_INFINITY));
            assertEquals(Double.POSITIVE_INFINITY, process(Double.NEGATIVE_INFINITY));

            assertEquals(Double.NaN, process(Double.NaN));

            return;
        }
        if (testCaseType == EsqlDataTypes.DATE_PERIOD) {
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

            return;
        }
        if (testCaseType == EsqlDataTypes.TIME_DURATION) {
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

            return;
        }

        throw new AssertionError("Edge cases not tested for negation with type [" + testCaseType.typeName() + "]");
    }

    private Object process(Object val) {
        if (testCase.allTypesAreRepresentable()) {
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
            return DataTypes.INTEGER;
        }
        if (val instanceof Long) {
            return DataTypes.LONG;
        }
        if (val instanceof Double) {
            return DataTypes.DOUBLE;
        }
        if (val instanceof Duration) {
            return EsqlDataTypes.TIME_DURATION;
        }
        if (val instanceof Period) {
            return EsqlDataTypes.DATE_PERIOD;
        }
        throw new UnsupportedOperationException("unsupported type [" + val.getClass() + "]");
    }
}
