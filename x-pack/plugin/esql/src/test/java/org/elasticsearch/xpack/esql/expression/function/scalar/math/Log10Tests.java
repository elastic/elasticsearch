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
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.bigIntegerToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;

public class Log10Tests extends AbstractScalarFunctionTestCase {
    public Log10Tests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        UnaryTestCaseHelper valid = unary().expectedOutputType(DataType.DOUBLE);

        // Cases in valid range
        valid.ints(1, Integer.MAX_VALUE).expectedFromInt(Math::log10).evaluatorToString("Log10IntEvaluator[val=%0]").build(suppliers);
        valid.longs(1, Long.MAX_VALUE).expectedFromLong(Math::log10).evaluatorToString("Log10LongEvaluator[val=%0]").build(suppliers);
        valid.unsignedLongs(BigInteger.ONE, UNSIGNED_LONG_MAX)
            .expectedFromBigInteger(ul -> Math.log10(unsignedLongToDouble(bigIntegerToUnsignedLong(ul))))
            .evaluatorToString("Log10UnsignedLongEvaluator[val=%0]")
            .build(suppliers);
        valid.doubles(Double.MIN_VALUE /* near 0 */, Double.POSITIVE_INFINITY)
            .expectedFromDouble(Math::log10)
            .evaluatorToString("Log10DoubleEvaluator[val=%0]")
            .build(suppliers);

        // Add in null cases here; the out of range cases won't set the right warnings on a null input.
        suppliers = anyNullIsNull(true, suppliers);

        // Cases with invalid inputs
        UnaryTestCaseHelper invalid = valid.expectNullAndWarnings(
            o -> List.of("Line 1:1: java.lang.ArithmeticException: Log of non-positive number")
        );
        invalid.ints(Integer.MIN_VALUE, 0).evaluatorToString("Log10IntEvaluator[val=%0]").build(suppliers);
        invalid.longs(Long.MIN_VALUE, 0L).evaluatorToString("Log10LongEvaluator[val=%0]").build(suppliers);
        invalid.unsignedLongs(BigInteger.ZERO, BigInteger.ZERO).evaluatorToString("Log10UnsignedLongEvaluator[val=%0]").build(suppliers);
        invalid.doubles(Double.NEGATIVE_INFINITY, 0d).evaluatorToString("Log10DoubleEvaluator[val=%0]").build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Log10(source, args.get(0));
    }
}
