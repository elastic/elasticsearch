/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.hamcrest.Matchers.equalTo;

public class CaseTests extends AbstractScalarFunctionTestCase {

    public CaseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Generate the test cases for this test
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType type : List.of(
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.DATE_NANOS,
            DataType.DOUBLE,
            DataType.INTEGER,
            DataType.LONG,
            DataType.UNSIGNED_LONG,
            DataType.IP,
            DataType.VERSION,
            DataType.CARTESIAN_POINT,
            DataType.GEO_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.GEO_SHAPE
        )) {
            suppliers.add(threeArgCase(true, true, type, List.of()));
            suppliers.add(threeArgCase(false, false, type, List.of()));
            suppliers.add(threeArgCase(null, false, type, List.of()));
            suppliers.add(
                threeArgCase(
                    List.of(true, true),
                    false,
                    type,
                    List.of(
                        "Line -1:-1: evaluation of [cond] failed, treating result as false. Only first 20 failures recorded.",
                        "Line -1:-1: java.lang.IllegalArgumentException: CASE expects a single-valued boolean"
                    )
                )
            );

            suppliers.add(twoArgsCase(true, true, type, List.of()));
            suppliers.add(twoArgsCase(false, false, type, List.of()));
            suppliers.add(twoArgsCase(null, false, type, List.of()));
            suppliers.add(
                twoArgsCase(
                    List.of(true, true),
                    false,
                    type,
                    List.of(
                        "Line -1:-1: evaluation of [cond] failed, treating result as false. Only first 20 failures recorded.",
                        "Line -1:-1: java.lang.IllegalArgumentException: CASE expects a single-valued boolean"
                    )
                )
            );

        }
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier threeArgCase(Object cond, boolean lhsOrRhs, DataType type, List<String> warnings) {
        return new TestCaseSupplier(
            TestCaseSupplier.nameFrom(Arrays.asList(cond, type, type)),
            List.of(DataType.BOOLEAN, type, type),
            () -> {
                Object lhs = randomLiteral(type).value();
                Object rhs = randomLiteral(type).value();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(cond, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(lhs, type, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, type, "rhs")
                );
                return testCase(
                    type,
                    typedData,
                    lhsOrRhs ? lhs : rhs,
                    "CaseEvaluator[conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=Attribute[channel=2]]",
                    warnings
                );
            }
        );
    }

    private static TestCaseSupplier twoArgsCase(Object cond, boolean lhsOrNull, DataType type, List<String> warnings) {
        return new TestCaseSupplier(TestCaseSupplier.nameFrom(Arrays.asList(cond, type)), List.of(DataType.BOOLEAN, type), () -> {
            Object lhs = randomLiteral(type).value();
            List<TestCaseSupplier.TypedData> typedData = List.of(
                new TestCaseSupplier.TypedData(cond, DataType.BOOLEAN, "cond"),
                new TestCaseSupplier.TypedData(lhs, type, "lhs")
            );
            return testCase(
                type,
                typedData,
                lhsOrNull ? lhs : null,
                "CaseEvaluator[conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                    + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                warnings
            );
        });
    }

    private static TestCaseSupplier.TestCase testCase(
        DataType type,
        List<TestCaseSupplier.TypedData> typedData,
        Object result,
        String evaluatorToString,
        List<String> warnings
    ) {
        if (type == DataType.UNSIGNED_LONG && result != null) {
            result = NumericUtils.unsignedLongAsBigInteger((Long) result);
        }
        TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(typedData, evaluatorToString, type, equalTo(result));
        for (String warning : warnings) {
            testCase = testCase.withWarning(warning);
        }
        return testCase;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Case(Source.EMPTY, args.get(0), args.subList(1, args.size()));
    }
}
