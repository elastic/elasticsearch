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

public class ExpTests extends AbstractScalarFunctionTestCase {
    public ExpTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    // e^710 is Double.POSITIVE_INFINITY
    private static final int MAX_EXP_VALUE = 709;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "ExpIntEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            i -> Math.exp(i),
            Integer.MIN_VALUE,
            MAX_EXP_VALUE,
            List.of()
        );

        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ExpLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            l -> Math.exp(l),
            Long.MIN_VALUE,
            MAX_EXP_VALUE,
            List.of()
        );

        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ExpUnsignedLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            ul -> Math.exp(NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))),
            BigInteger.ZERO,
            BigInteger.valueOf(MAX_EXP_VALUE),
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ExpDoubleEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::exp,
            -Double.MAX_VALUE,
            MAX_EXP_VALUE,
            List.of()
        );

        suppliers = anyNullIsNull(true, suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Exp(source, args.get(0));
    }
}
