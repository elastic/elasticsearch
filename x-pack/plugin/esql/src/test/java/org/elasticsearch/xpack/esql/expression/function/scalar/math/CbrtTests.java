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

public class CbrtTests extends AbstractScalarFunctionTestCase {
    public CbrtTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // Valid values
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "CbrtIntEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::cbrt,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "CbrtLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::cbrt,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "CbrtUnsignedLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            ul -> Math.cbrt(unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "CbrtDoubleEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::cbrt,
            Double.MIN_VALUE,
            Double.MAX_VALUE,
            List.of()
        );
        suppliers = anyNullIsNull(true, suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Cbrt(source, args.get(0));
    }
}
