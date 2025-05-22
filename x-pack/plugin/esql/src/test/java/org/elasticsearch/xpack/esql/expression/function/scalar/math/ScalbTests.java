/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ScalbTests extends AbstractScalarFunctionTestCase {
    public ScalbTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(
            true,
            List.of(
                // Int scale factor
                test(DataType.DOUBLE, DataType.INTEGER, 3.14d, 6, 200.96d),
                test(DataType.INTEGER, DataType.INTEGER, 666, 6, 42624.0),
                test(DataType.LONG, DataType.INTEGER, 23L, 5, 736.d),
                test(DataType.UNSIGNED_LONG, DataType.INTEGER, 24325342L, 34, 1.584563250289466E29),

                // Long scale factor
                test(DataType.DOUBLE, DataType.LONG, 3.14d, 6L, 200.96d),
                test(DataType.INTEGER, DataType.LONG, 666, 6L, 42624.0),
                test(DataType.LONG, DataType.LONG, 23L, 5L, 736.d),
                test(DataType.UNSIGNED_LONG, DataType.LONG, 24325342L, 34L, 1.584563250289466E29),

                // null values
                test(DataType.LONG, DataType.LONG, null, 34L, null),
                test(DataType.LONG, DataType.LONG, 23L, null, null),

                // Overflow - Result
                test(
                    DataType.LONG,
                    DataType.LONG,
                    24325342L,
                    344444L,
                    null,
                    List.of(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                        "Line 1:1: java.lang.ArithmeticException: not a finite double number: Infinity"
                    )
                ),

                // Overflow in a constant scale factor

                new TestCaseSupplier(
                    "<long>, <long>",
                    List.of(DataType.LONG, DataType.LONG),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(18446744L, DataType.LONG, "number"),
                            new TestCaseSupplier.TypedData(2147483648L, DataType.LONG, "decimals")
                        ),
                        "ScalbLongEvaluator[d=CastLongToDoubleEvaluator[v=Attribute[channel=0]], scaleFactor=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(null)
                    ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning("Line 1:1: java.lang.ArithmeticException: integer overflow")

                )
            )
        );
    }

    private static TestCaseSupplier test(DataType dType, DataType scaleType, Number d, Number scaleFactor, Number res) {
        return test(dType, scaleType, d, scaleFactor, res, List.of());
    }

    private static TestCaseSupplier test(
        DataType dType,
        DataType scaleType,
        Number d,
        Number scaleFactor,
        Number res,
        Iterable<String> warns
    ) {
        var supplier = new TestCaseSupplier.TestCase(
            List.of(new TestCaseSupplier.TypedData(d, dType, "d"), new TestCaseSupplier.TypedData(scaleFactor, scaleType, "scaleFactor")),
            Strings.format(
                "Scalb%sEvaluator[d=%s, scaleFactor=Attribute[channel=1]]",
                pascalize(scaleType),
                cast(dType)

            ),
            DataType.DOUBLE,
            equalTo(res)
        );
        for (var warn : warns) {
            supplier = supplier.withWarning(warn);
        }
        var finalSupplier = supplier;
        return new TestCaseSupplier(
            Strings.format("<%s>, <%s>", dType.typeName(), scaleType.typeName()),
            List.of(dType, scaleType),
            () -> finalSupplier
        );
    }

    private static String cast(DataType from) {
        return from == DataType.DOUBLE
            ? "Attribute[channel=0]"
            : Strings.format("Cast%sToDoubleEvaluator[v=Attribute[channel=0]]", pascalize(from));
    }

    private static String pascalize(DataType type) {
        return switch (type) {
            case UNSIGNED_LONG -> "UnsignedLong";
            case INTEGER -> "Int";
            default -> Strings.capitalize(type.typeName());
        };
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Scalb(source, args.get(0), args.get(1));
    }
}
