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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class FloorTests extends AbstractScalarFunctionTestCase {
    public FloorTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        unary().expectedOutputType(DataType.INTEGER).ints().expectedFromInt(i -> i).evaluatorToString("%0").build(suppliers);
        unary().expectedOutputType(DataType.LONG).longs().expectedFromLong(l -> l).evaluatorToString("%0").build(suppliers);
        unary().expectedOutputType(DataType.UNSIGNED_LONG)
            .unsignedLongs(BigInteger.ZERO, UNSIGNED_LONG_MAX)
            .expectedFromBigInteger(ul -> ul)
            .evaluatorToString("%0")
            .build(suppliers);
        unary().expectedOutputType(DataType.DOUBLE)
            .doubles()
            .expectedFromDouble(Math::floor)
            .evaluatorToString("FloorDoubleEvaluator[val=%0]")
            .build(suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Floor(source, args.get(0));
    }
}
