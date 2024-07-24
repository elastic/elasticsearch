/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

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

public class NotEqualsTests extends AbstractScalarFunctionTestCase {
    public NotEqualsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
                        (l, r) -> l.intValue() != r.intValue(),
                        "NotEqualsIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() != r.longValue(),
                        "NotEqualsLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        // NB: this has different behavior than Double::equals
                        (l, r) -> l.doubleValue() != r.doubleValue(),
                        "NotEqualsDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                (lhs, rhs) -> List.of(),
                false
            )
        );
        // Unsigned Long cases
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                List.of(),
                true
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsBoolsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.booleanCases(),
                TestCaseSupplier.booleanCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsKeywordsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.ipCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsKeywordsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.versionCases(""),
                TestCaseSupplier.versionCases(""),
                List.of(),
                false
            )
        );
        // Datetime
        // TODO: I'm surprised this passes. Shouldn't there be a cast from DateTime to Long?
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.stringCases(
                (l, r) -> false == l.equals(r),
                (lhsType, rhsType) -> "NotEqualsKeywordsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                List.of(),
                DataType.BOOLEAN
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.geoPointCases(),
                TestCaseSupplier.geoPointCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.geoShapeCases(),
                TestCaseSupplier.geoShapeCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.cartesianPointCases(),
                TestCaseSupplier.cartesianPointCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "NotEqualsGeometriesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> false == l.equals(r),
                DataType.BOOLEAN,
                TestCaseSupplier.cartesianShapeCases(),
                TestCaseSupplier.cartesianShapeCases(),
                List.of(),
                false
            )
        );
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(
                anyNullIsNull(true, suppliers),
                (o, v, t) -> AbstractScalarFunctionTestCase.errorMessageStringForBinaryOperators(o, v, t, (l, p) -> "")
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new NotEquals(source, args.get(0), args.get(1));
    }
}
