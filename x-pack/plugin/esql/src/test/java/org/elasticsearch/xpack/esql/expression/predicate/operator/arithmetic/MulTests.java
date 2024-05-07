/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.AbstractArithmeticTestCase.arithmeticExceptionOverflowCase;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.hamcrest.Matchers.equalTo;

public class MulTests extends AbstractFunctionTestCase {
    public MulTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<Number>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(-255, 255, (l, r) -> l.intValue() * r.intValue(), "MulIntsEvaluator"),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        -1024L,
                        1024L,
                        (l, r) -> l.longValue() * r.longValue(),
                        "MulLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        -1024D,
                        1024D,
                        (l, r) -> l.doubleValue() * r.doubleValue(),
                        "MulDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                (lhs, rhs) -> List.of(),
                true
            )
        );

        suppliers.add(new TestCaseSupplier("Double * Double", List.of(DataTypes.DOUBLE, DataTypes.DOUBLE), () -> {
            double rhs = randomDouble();
            double lhs = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataTypes.DOUBLE, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataTypes.DOUBLE, "rhs")
                ),
                "MulDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataTypes.DOUBLE,
                equalTo(lhs * rhs)
            );
        }));
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataTypes.INTEGER,
                () -> randomBoolean() ? Integer.MIN_VALUE : Integer.MAX_VALUE,
                () -> randomIntBetween(2, Integer.MAX_VALUE),
                "MulIntsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataTypes.LONG,
                () -> randomBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE,
                () -> randomLongBetween(2L, Long.MAX_VALUE),
                "MulLongsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataTypes.UNSIGNED_LONG,
                () -> asLongUnsigned(UNSIGNED_LONG_MAX),
                () -> asLongUnsigned(randomLongBetween(-Long.MAX_VALUE, Long.MAX_VALUE)),
                "MulUnsignedLongsEvaluator"
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Mul(source, args.get(0), args.get(1));
    }
}
