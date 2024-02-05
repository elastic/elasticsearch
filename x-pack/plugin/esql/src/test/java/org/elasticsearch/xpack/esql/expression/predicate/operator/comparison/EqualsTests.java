/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class EqualsTests extends AbstractFunctionTestCase {
    public EqualsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryComparisonWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Integer.MIN_VALUE >> 1) - 1,
                        (Integer.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.intValue() == r.intValue(),
                        "EqualsIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() == r.longValue(),
                        "EqualsLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        // NB: this has different behavior than Double::equals
                        (l, r) -> l.doubleValue() == r.doubleValue(),
                        "EqualsDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                List.of()
            )
        );

        // Unsigned Long cases
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsLongsEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataTypes.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE)),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE)),
                List.of()
            )
        );
        // Datetime
        // TODO: I'm surprised this passes.  Shouldn't there be a cast from DateTime to Long? 
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsLongsEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataTypes.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateCases(),
                List.of()
            )
        );
        for (DataType type : AbstractConvertFunction.STRING_TYPES) {
            suppliers.addAll(
                TestCaseSupplier.forBinaryNotCasting(
                    "EqualsKeywordsEvaluator",
                    "lhs",
                    "rhs",
                    Object::equals,
                    DataTypes.BOOLEAN,
                    TestCaseSupplier.stringCases(type),
                    TestCaseSupplier.stringCases(type),
                    List.of()
                )
            );
        }
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataTypes.BOOLEAN,
                TestCaseSupplier.geoPointCases(),
                TestCaseSupplier.geoPointCases(),
                List.of()
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "EqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                Object::equals,
                DataTypes.BOOLEAN,
                TestCaseSupplier.geoShapeCases(),
                TestCaseSupplier.geoShapeCases(),
                List.of()
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Equals(source, args.get(0), args.get(1));
    }
}
