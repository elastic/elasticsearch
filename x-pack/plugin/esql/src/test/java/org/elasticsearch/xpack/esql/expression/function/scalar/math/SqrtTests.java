/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;

public class SqrtTests extends AbstractScalarFunctionTestCase {
    public SqrtTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // Valid values
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "SqrtIntEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::sqrt,
            0,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "SqrtLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::sqrt,
            0,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "SqrtUnsignedLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            ul -> Math.sqrt(ul == null ? null : unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "SqrtDoubleEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::sqrt,
            -0d,
            Double.MAX_VALUE,
            List.of()
        );
        suppliers = anyNullIsNull(true, suppliers);

        // Out of range values (there are no out of range unsigned longs)
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "SqrtIntEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            k -> null,
            Integer.MIN_VALUE,
            -1,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: Square root of negative"
            )
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "SqrtLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            k -> null,
            Long.MIN_VALUE,
            -1,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: Square root of negative"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "SqrtDoubleEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            k -> null,
            Double.NEGATIVE_INFINITY,
            -Double.MIN_VALUE,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: Square root of negative"
            )
        );
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(suppliers, (v, p) -> "numeric"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sqrt(source, args.get(0));
    }
}
