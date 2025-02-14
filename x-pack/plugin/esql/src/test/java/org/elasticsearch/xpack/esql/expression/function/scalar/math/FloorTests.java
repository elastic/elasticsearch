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

public class FloorTests extends AbstractScalarFunctionTestCase {
    public FloorTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(suppliers, read, DataType.INTEGER, i -> i, Integer.MIN_VALUE, Integer.MAX_VALUE, List.of());
        TestCaseSupplier.forUnaryLong(suppliers, read, DataType.LONG, l -> l, Long.MIN_VALUE, Long.MAX_VALUE, List.of());
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            read,
            DataType.UNSIGNED_LONG,
            ul -> ul,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "FloorDoubleEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::floor,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Floor(source, args.get(0));
    }
}
