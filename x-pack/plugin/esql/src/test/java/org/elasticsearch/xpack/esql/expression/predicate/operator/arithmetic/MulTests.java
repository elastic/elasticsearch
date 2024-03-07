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
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.AbstractArithmeticTestCase.arithmeticExceptionOverflowCase;
import static org.elasticsearch.xpack.qlcore.util.NumericUtils.asLongUnsigned;
import static org.hamcrest.Matchers.equalTo;

public class MulTests extends AbstractFunctionTestCase {
    public MulTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Int * Int", () -> {
            // Ensure we don't have an overflow
            int rhs = randomIntBetween(-255, 255);
            int lhs = randomIntBetween(-255, 255);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataTypes.INTEGER, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataTypes.INTEGER, "rhs")
                ),
                "MulIntsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataTypes.INTEGER,
                equalTo(lhs * rhs)
            );
        }), new TestCaseSupplier("Long * Long", () -> {
            // Ensure we don't have an overflow
            long rhs = randomLongBetween(-1024, 1024);
            long lhs = randomLongBetween(-1024, 1024);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataTypes.LONG, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataTypes.LONG, "rhs")
                ),
                "MulLongsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataTypes.LONG,
                equalTo(lhs * rhs)
            );
        }), new TestCaseSupplier("Double * Double", () -> {
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
        }), /* new TestCaseSupplier("ULong * ULong", () -> {
             // Ensure we don't have an overflow
             long rhs = randomLongBetween(0, 1024);
             long lhs = randomLongBetween(0, 1024);
             BigInteger lhsBI = unsignedLongAsBigInteger(lhs);
             BigInteger rhsBI = unsignedLongAsBigInteger(rhs);
             return new TestCase(
                 Source.EMPTY,
                 List.of(new TypedData(lhs, DataTypes.UNSIGNED_LONG, "lhs"), new TypedData(rhs, DataTypes.UNSIGNED_LONG, "rhs")),
                 "MulUnsignedLongsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                 equalTo(asLongUnsigned(lhsBI.multiply(rhsBI).longValue()))
             );
            })
            */
            arithmeticExceptionOverflowCase(
                DataTypes.INTEGER,
                () -> randomBoolean() ? Integer.MIN_VALUE : Integer.MAX_VALUE,
                () -> randomIntBetween(2, Integer.MAX_VALUE),
                "MulIntsEvaluator"
            ),
            arithmeticExceptionOverflowCase(
                DataTypes.LONG,
                () -> randomBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE,
                () -> randomLongBetween(2L, Long.MAX_VALUE),
                "MulLongsEvaluator"
            ),
            arithmeticExceptionOverflowCase(
                DataTypes.UNSIGNED_LONG,
                () -> asLongUnsigned(UNSIGNED_LONG_MAX),
                () -> asLongUnsigned(randomLongBetween(-Long.MAX_VALUE, Long.MAX_VALUE)),
                "MulUnsignedLongsEvaluator"
            )
        ));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Mul(source, args.get(0), args.get(1));
    }
}
