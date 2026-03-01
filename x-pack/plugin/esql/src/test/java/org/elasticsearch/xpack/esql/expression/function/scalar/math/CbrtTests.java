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

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;

public class CbrtTests extends AbstractScalarFunctionTestCase {
    public CbrtTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        var helper = unary().expectedOutputType(DataType.DOUBLE);
        helper.ints().expectedFromInt(Math::cbrt).evaluatorToString("CbrtIntEvaluator[val=%0]").build(suppliers);
        helper.longs().expectedFromLong(Math::cbrt).evaluatorToString("CbrtLongEvaluator[val=%0]").build(suppliers);
        helper.unsignedLongs(BigInteger.ZERO, UNSIGNED_LONG_MAX)
            .expectedFromBigInteger(ul -> Math.cbrt(unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))))
            .evaluatorToString("CbrtUnsignedLongEvaluator[val=%0]")
            .build(suppliers);
        helper.doubles(Double.MIN_VALUE, Double.MAX_VALUE)
            .expectedFromDouble(Math::cbrt)
            .evaluatorToString("CbrtDoubleEvaluator[val=%0]")
            .build(suppliers);
        suppliers = anyNullIsNull(true, suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Cbrt(source, args.get(0));
    }
}
