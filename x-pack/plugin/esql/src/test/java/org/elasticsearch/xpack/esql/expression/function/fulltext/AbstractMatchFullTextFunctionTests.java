/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.stringCases;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractMatchFullTextFunctionTests extends AbstractFunctionTestCase {

    protected static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        AbstractMatchFullTextFunctionTests.addUnsignedLongCases(suppliers);
        AbstractMatchFullTextFunctionTests.addNumericCases(suppliers);
        AbstractMatchFullTextFunctionTests.addNonNumericCases(suppliers);
        AbstractMatchFullTextFunctionTests.addQueryAsStringTestCases(suppliers);
        AbstractMatchFullTextFunctionTests.addStringTestCases(suppliers);
        return suppliers;
    }

    private static void addNonNumericCases(List<TestCaseSupplier> suppliers) {
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.booleanCases(),
                TestCaseSupplier.booleanCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.ipCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.versionCases(""),
                TestCaseSupplier.versionCases(""),
                List.of(),
                false
            )
        );
        // Datetime
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.dateNanosCases(),
                List.of(),
                false
            )
        );
    }

    private static void addNumericCases(List<TestCaseSupplier> suppliers) {
        suppliers.addAll(
            TestCaseSupplier.forBinaryComparisonWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Integer.MIN_VALUE >> 1) - 1,
                        (Integer.MAX_VALUE >> 1) - 1,
                        (l, r) -> true,
                        "EqualsIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> true,
                        "EqualsLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        // NB: this has different behavior than Double::equals
                        (l, r) -> true,
                        "EqualsDoublesEvaluator"
                    )
                ),
                "field",
                "query",
                (lhs, rhs) -> List.of(),
                false
            )
        );
    }

    private static void addUnsignedLongCases(List<TestCaseSupplier> suppliers) {
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.longCases(Long.MIN_VALUE, Long.MAX_VALUE, true),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.doubleCases(Double.MIN_VALUE, Double.MAX_VALUE, true),
                List.of(),
                false
            )
        );
    }

    private static void addQueryAsStringTestCases(List<TestCaseSupplier> suppliers) {

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.longCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.doubleCases(Double.MIN_VALUE, Double.MAX_VALUE, true),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );

        // Unsigned Long cases
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.booleanCases(),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.versionCases(""),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );
        // Datetime
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.stringCases(DataType.KEYWORD),
                List.of(),
                false
            )
        );
    }

    private static void addStringTestCases(List<TestCaseSupplier> suppliers) {
        for (DataType fieldType : DataType.stringTypes()) {
            if (DataType.UNDER_CONSTRUCTION.containsKey(fieldType)) {
                continue;
            }
            for (TestCaseSupplier.TypedDataSupplier queryDataSupplier : stringCases(fieldType)) {
                suppliers.add(
                    TestCaseSupplier.testCaseSupplier(
                        queryDataSupplier,
                        new TestCaseSupplier.TypedDataSupplier(fieldType.typeName(), () -> randomAlphaOfLength(10), DataType.KEYWORD),
                        (d1, d2) -> equalTo("string"),
                        DataType.BOOLEAN,
                        (o1, o2) -> true
                    )
                );
            }
        }
    }

    public final void testLiteralExpressions() {
        Expression expression = buildLiteralExpression(testCase);
        assertFalse("expected resolved", expression.typeResolved().unresolved());
    }
}
