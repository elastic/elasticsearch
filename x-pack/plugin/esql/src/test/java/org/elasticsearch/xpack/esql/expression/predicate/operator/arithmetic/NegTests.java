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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class NegTests extends AbstractFunctionTestCase {

    public NegTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "NegIntsEvaluator[v=Attribute[channel=0]]",
            DataTypes.INTEGER,
            Math::negateExact,
            Integer.MIN_VALUE + 1,
            Integer.MAX_VALUE,
            List.of()
        );
        // out of bounds integer
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "NegIntsEvaluator[v=Attribute[channel=0]]",
            DataTypes.INTEGER,
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
            DataTypes.LONG,
            Math::negateExact,
            Long.MIN_VALUE + 1,
            Long.MAX_VALUE,
            List.of()
        );
        // out of bounds long
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "NegLongsEvaluator[v=Attribute[channel=0]]",
            DataTypes.LONG,
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
            DataTypes.DOUBLE,
            // TODO: Probably we don't want to allow negative zeros
            d -> -d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        // TODO: Wire up edge case generation functions for these
        suppliers.addAll(List.of(new TestCaseSupplier("Duration", List.of(EsqlDataTypes.TIME_DURATION), () -> {
            Duration arg = (Duration) randomLiteral(EsqlDataTypes.TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, EsqlDataTypes.TIME_DURATION, "arg")),
                "No evaluator since this expression is only folded",
                EsqlDataTypes.TIME_DURATION,
                equalTo(arg.negated())
            );
        }), new TestCaseSupplier("Period", List.of(EsqlDataTypes.DATE_PERIOD), () -> {
            Period arg = (Period) randomLiteral(EsqlDataTypes.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, EsqlDataTypes.DATE_PERIOD, "arg")),
                "No evaluator since this expression is only folded",
                EsqlDataTypes.DATE_PERIOD,
                equalTo(arg.negated())
            );
        })));
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(false, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Neg(source, args.get(0));
    }

    public void testEdgeCases() {
        // Run the assertions for the current test cases type only to avoid running the same assertions multiple times.
        // TODO: These remaining cases should get rolled into generation functions for periods and durations
        DataType testCaseType = testCase.getData().get(0).type();
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
        } else if (testCaseType == EsqlDataTypes.TIME_DURATION) {
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
