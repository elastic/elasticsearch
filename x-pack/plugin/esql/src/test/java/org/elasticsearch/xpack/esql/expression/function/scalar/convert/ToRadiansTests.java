/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

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
import java.util.function.Function;
import java.util.function.Supplier;

public class ToRadiansTests extends AbstractScalarFunctionTestCase {
    public ToRadiansTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        Function<String, String> evaluatorName = eval -> "ToRadiansEvaluator[field=" + eval + "[field=Attribute[channel=0]]]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorName.apply("ToDoubleFromIntEvaluator"),
            DataType.DOUBLE,
            Math::toRadians,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName.apply("ToDoubleFromLongEvaluator"),
            DataType.DOUBLE,
            Math::toRadians,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("ToDoubleFromUnsignedLongEvaluator"),
            DataType.DOUBLE,
            ul -> Math.toRadians(ul.doubleValue()),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToRadiansEvaluator[field=Attribute[channel=0]]",
            DataType.DOUBLE,
            Math::toRadians,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers, (v, p) -> "numeric");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToRadians(source, args.get(0));
    }
}
