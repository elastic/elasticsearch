/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class ToRadiansTests extends AbstractFunctionTestCase {
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
            DataTypes.DOUBLE,
            Math::toRadians,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName.apply("ToDoubleFromLongEvaluator"),
            DataTypes.DOUBLE,
            Math::toRadians,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("ToDoubleFromUnsignedLongEvaluator"),
            DataTypes.DOUBLE,
            ul -> Math.toRadians(ul.doubleValue()),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToRadiansEvaluator[field=Attribute[channel=0]]",
            DataTypes.DOUBLE,
            Math::toRadians,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToRadians(source, args.get(0));
    }
}
