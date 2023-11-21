/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.NumericUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class Log10Tests extends AbstractFunctionTestCase {
    public Log10Tests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // Cases in valid range
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "Log10IntEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            Math::log10,
            1,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "Log10LongEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            Math::log10,
            1L,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "Log10UnsignedLongEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            ul -> Math.log10(ul == null ? null : NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))),
            BigInteger.ONE,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "Log10DoubleEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            Math::log10,
            Double.MIN_VALUE,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        // Add in null cases here; the out of range cases won't set the right warnings on a null input.
        suppliers = anyNullIsNull(true, suppliers);

        // Cases with invalid inputs
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "Log10IntEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            k -> null,
            Integer.MIN_VALUE,
            0,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: Log of non-positive number"
            )
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "Log10LongEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            k -> null,
            Long.MIN_VALUE,
            0L,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: Log of non-positive number"
            )
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "Log10UnsignedLongEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            k -> null,
            BigInteger.ZERO,
            BigInteger.ZERO,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: Log of non-positive number"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "Log10DoubleEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            k -> null,
            Double.NEGATIVE_INFINITY,
            0d,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.ArithmeticException: Log of non-positive number"
            )
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Log10(source, args.get(0));
    }
}
