/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalToIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class CaseTests extends AbstractScalarFunctionTestCase {

    private static final List<DataType> RETURN_TYPES = Arrays.stream(DataType.values())
        .filter(t -> t.supportedVersion().supportedLocally())
        .filter(DataType::isRepresentable)
        .filter(t -> t != DataType.DOC_DATA_TYPE)
        .filter(t -> t != DataType.TSID_DATA_TYPE)
        .filter(t -> t != DataType.DATE_RANGE) // TODO(pr/133309): implement
        .toList();

    /**
     * Generate the test cases for this test
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType returnType : RETURN_TYPES) {
            twoAndThreeArgs(suppliers, true, true, returnType, List.of());
            twoAndThreeArgs(suppliers, false, false, returnType, List.of());
            twoAndThreeArgs(suppliers, null, false, returnType, List.of());
            twoAndThreeArgs(
                suppliers,
                randomMultivaluedCondition(),
                false,
                returnType,
                List.of(
                    "Line -1:-1: evaluation of [cond] failed, treating result as false. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.IllegalArgumentException: CASE expects a single-valued boolean"
                )
            );
        }

        for (DataType returnType : RETURN_TYPES) {
            fourAndFiveArgs(suppliers, true, randomSingleValuedCondition(), 0, returnType, List.of());
            fourAndFiveArgs(suppliers, false, true, 1, returnType, List.of());
            fourAndFiveArgs(suppliers, false, false, 2, returnType, List.of());
            fourAndFiveArgs(suppliers, null, true, 1, returnType, List.of());
            fourAndFiveArgs(suppliers, null, false, 2, returnType, List.of());
            fourAndFiveArgs(
                suppliers,
                randomMultivaluedCondition(),
                true,
                1,
                returnType,
                List.of(
                    "Line -1:-1: evaluation of [cond1] failed, treating result as false. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.IllegalArgumentException: CASE expects a single-valued boolean"
                )
            );
            fourAndFiveArgs(
                suppliers,
                false,
                randomMultivaluedCondition(),
                2,
                returnType,
                List.of(
                    "Line -1:-1: evaluation of [cond2] failed, treating result as false. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.IllegalArgumentException: CASE expects a single-valued boolean"
                )
            );
        }
        FunctionAppliesTo histogramPreviewAppliesTo = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", false);
        FunctionAppliesTo histogramGaAppliesTo = appliesTo(FunctionAppliesToLifecycle.GA, "9.4.0", "", true);
        suppliers = TestCaseSupplier.mapTestCases(suppliers, tc -> tc.withData(tc.getData().stream().map(typedData -> {
            DataType type = typedData.type();
            if (type == DataType.HISTOGRAM || type == DataType.EXPONENTIAL_HISTOGRAM || type == DataType.TDIGEST) {
                return typedData.withAppliesTo(histogramPreviewAppliesTo).withAppliesTo(histogramGaAppliesTo);
            }
            return typedData;
        }).toList()));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void twoAndThreeArgs(
        List<TestCaseSupplier> suppliers,
        Object cond,
        boolean lhsOrRhs,
        DataType returnType,
        List<String> warnings
    ) {
        suppliers.add(
            new TestCaseSupplier(TestCaseSupplier.nameFrom(Arrays.asList(cond, returnType)), List.of(DataType.BOOLEAN, returnType), () -> {
                Object lhs = randomLiteral(returnType).value();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    cond(cond, "cond"),
                    new TestCaseSupplier.TypedData(lhs, returnType, "lhs")
                );
                return testCase(returnType, typedData, lhsOrRhs ? lhs : null, toStringMatcher(1, true), false, null, addWarnings(warnings));
            })
        );
        suppliers.add(
            new TestCaseSupplier(
                TestCaseSupplier.nameFrom(Arrays.asList(cond, returnType, returnType)),
                List.of(DataType.BOOLEAN, returnType, returnType),
                () -> {
                    Object lhs = randomLiteral(returnType).value();
                    Object rhs = randomLiteral(returnType).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond, "cond"),
                        new TestCaseSupplier.TypedData(lhs, returnType, "lhs"),
                        new TestCaseSupplier.TypedData(rhs, returnType, "rhs")
                    );
                    return testCase(
                        returnType,
                        typedData,
                        lhsOrRhs ? lhs : rhs,
                        toStringMatcher(1, false),
                        false,
                        null,
                        addWarnings(warnings)
                    );
                }
            )
        );
        if (returnType.noText() == DataType.KEYWORD) {
            DataType otherType = returnType == DataType.KEYWORD ? DataType.TEXT : DataType.KEYWORD;
            suppliers.add(
                new TestCaseSupplier(
                    TestCaseSupplier.nameFrom(Arrays.asList(cond, returnType, otherType)),
                    List.of(DataType.BOOLEAN, returnType, otherType),
                    () -> {
                        Object lhs = randomLiteral(returnType).value();
                        Object rhs = randomLiteral(otherType).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            cond(cond, "cond"),
                            new TestCaseSupplier.TypedData(lhs, returnType, "lhs"),
                            new TestCaseSupplier.TypedData(rhs, otherType, "rhs")
                        );
                        return testCase(
                            returnType,
                            typedData,
                            lhsOrRhs ? lhs : rhs,
                            toStringMatcher(1, false),
                            false,
                            null,
                            addWarnings(warnings)
                        );
                    }
                )
            );
        }
        if (lhsOrRhs) {
            suppliers.add(
                new TestCaseSupplier(
                    "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond, returnType, returnType)),
                    List.of(DataType.BOOLEAN, returnType, returnType),
                    () -> {
                        Object lhs = randomLiteral(returnType).value();
                        Object rhs = randomLiteral(returnType).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            cond(cond, "cond").forceLiteral(),
                            new TestCaseSupplier.TypedData(lhs, returnType, "lhs").forceLiteral(),
                            new TestCaseSupplier.TypedData(rhs, returnType, "rhs")
                        );
                        return testCase(
                            returnType,
                            typedData,
                            lhs,
                            startsWith("LiteralsEvaluator[lit="),
                            true,
                            null,
                            addBuildEvaluatorWarnings(warnings)
                        );
                    }
                )
            );
            suppliers.add(
                new TestCaseSupplier(
                    "partial foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond, returnType)),
                    List.of(DataType.BOOLEAN, returnType),
                    () -> {
                        Object lhs = randomLiteral(returnType).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            cond(cond, "cond").forceLiteral(),
                            new TestCaseSupplier.TypedData(lhs, returnType, "lhs")
                        );
                        return testCase(
                            returnType,
                            typedData,
                            lhs,
                            startsWith("CaseEagerEvaluator[conditions=[ConditionEvaluator[condition=LiteralsEvaluator"),
                            false,
                            List.of(typedData.get(1)),
                            addBuildEvaluatorWarnings(warnings)
                        );
                    }
                )
            );
        } else {
            suppliers.add(
                new TestCaseSupplier(
                    "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond, returnType)),
                    List.of(DataType.BOOLEAN, returnType),
                    () -> {
                        Object lhs = randomLiteral(returnType).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            cond(cond, "cond").forceLiteral(),
                            new TestCaseSupplier.TypedData(lhs, returnType, "lhs")
                        );
                        return testCase(
                            returnType,
                            typedData,
                            null,
                            startsWith("LiteralsEvaluator[lit="),
                            true,
                            List.of(new TestCaseSupplier.TypedData(null, returnType, "null").forceLiteral()),
                            addBuildEvaluatorWarnings(warnings)
                        );
                    }
                )
            );
        }
        suppliers.add(
            new TestCaseSupplier(
                "partial foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond, returnType, returnType)),
                List.of(DataType.BOOLEAN, returnType, returnType),
                () -> {
                    Object lhs = randomLiteral(returnType).value();
                    Object rhs = randomLiteral(returnType).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond, "cond").forceLiteral(),
                        new TestCaseSupplier.TypedData(lhs, returnType, "lhs"),
                        new TestCaseSupplier.TypedData(rhs, returnType, "rhs")
                    );
                    return testCase(
                        returnType,
                        typedData,
                        lhsOrRhs ? lhs : rhs,
                        startsWith("CaseEagerEvaluator[conditions=[ConditionEvaluator[condition=LiteralsEvaluator"),
                        false,
                        List.of(typedData.get(lhsOrRhs ? 1 : 2)),
                        addWarnings(warnings)
                    );
                }
            )
        );

        // Fill in some cases with null conditions or null values
        if (cond == null) {
            suppliers.add(
                new TestCaseSupplier(
                    TestCaseSupplier.nameFrom(Arrays.asList(DataType.NULL, returnType)),
                    List.of(DataType.NULL, returnType),
                    () -> {
                        Object lhs = randomLiteral(returnType).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            new TestCaseSupplier.TypedData(null, DataType.NULL, "cond"),
                            new TestCaseSupplier.TypedData(lhs, returnType, "lhs")
                        );
                        return testCase(
                            returnType,
                            typedData,
                            lhsOrRhs ? lhs : null,
                            startsWith("CaseEagerEvaluator[conditions=[ConditionEvaluator[condition="),
                            false,
                            null,
                            addWarnings(warnings)
                        );
                    }
                )
            );
            suppliers.add(
                new TestCaseSupplier(
                    TestCaseSupplier.nameFrom(Arrays.asList(DataType.NULL, returnType, returnType)),
                    List.of(DataType.NULL, returnType, returnType),
                    () -> {
                        Object lhs = randomLiteral(returnType).value();
                        Object rhs = randomLiteral(returnType).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            new TestCaseSupplier.TypedData(null, DataType.NULL, "cond"),
                            new TestCaseSupplier.TypedData(lhs, returnType, "lhs"),
                            new TestCaseSupplier.TypedData(rhs, returnType, "rhs")
                        );
                        return testCase(
                            returnType,
                            typedData,
                            lhsOrRhs ? lhs : rhs,
                            startsWith("CaseEagerEvaluator[conditions=[ConditionEvaluator[condition="),
                            false,
                            null,
                            addWarnings(warnings)
                        );
                    }
                )
            );
            if (returnType.noText() == DataType.KEYWORD) {
                DataType otherType = returnType == DataType.KEYWORD ? DataType.TEXT : DataType.KEYWORD;
                suppliers.add(
                    new TestCaseSupplier(
                        TestCaseSupplier.nameFrom(Arrays.asList(DataType.NULL, returnType, otherType)),
                        List.of(DataType.NULL, returnType, otherType),
                        () -> {
                            Object lhs = randomLiteral(returnType).value();
                            Object rhs = randomLiteral(otherType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                new TestCaseSupplier.TypedData(null, DataType.NULL, "cond"),
                                new TestCaseSupplier.TypedData(lhs, returnType, "lhs"),
                                new TestCaseSupplier.TypedData(rhs, otherType, "rhs")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                lhsOrRhs ? lhs : rhs,
                                startsWith("CaseEagerEvaluator[conditions=[ConditionEvaluator[condition="),
                                false,
                                null,
                                addWarnings(warnings)
                            );
                        }
                    )
                );
            }
        }
        suppliers.add(
            new TestCaseSupplier(
                TestCaseSupplier.nameFrom(Arrays.asList(cond, DataType.NULL, returnType)),
                List.of(DataType.BOOLEAN, DataType.NULL, returnType),
                () -> {
                    Object rhs = randomLiteral(returnType).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond, "cond"),
                        new TestCaseSupplier.TypedData(null, DataType.NULL, "lhs"),
                        new TestCaseSupplier.TypedData(rhs, returnType, "rhs")
                    );
                    return testCase(
                        returnType,
                        typedData,
                        lhsOrRhs ? null : rhs,
                        startsWith("CaseEagerEvaluator[conditions=[ConditionEvaluator[condition="),
                        false,
                        null,
                        addWarnings(warnings)
                    );
                }
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                TestCaseSupplier.nameFrom(Arrays.asList(cond, returnType, DataType.NULL)),
                List.of(DataType.BOOLEAN, returnType, DataType.NULL),
                () -> {
                    Object lhs = randomLiteral(returnType).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond, "cond"),
                        new TestCaseSupplier.TypedData(lhs, returnType, "lhs"),
                        new TestCaseSupplier.TypedData(null, DataType.NULL, "rhs")
                    );
                    return testCase(
                        returnType,
                        typedData,
                        lhsOrRhs ? lhs : null,
                        startsWith("CaseEagerEvaluator[conditions=[ConditionEvaluator[condition="),
                        false,
                        null,
                        addWarnings(warnings)
                    );
                }
            )
        );
    }

    private static void fourAndFiveArgs(
        List<TestCaseSupplier> suppliers,
        Object cond1,
        Object cond2,
        int result,
        DataType returnType,
        List<String> warnings
    ) {
        suppliers.add(
            new TestCaseSupplier(
                TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType)),
                List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType),
                () -> {
                    Object r1 = randomLiteral(returnType).value();
                    Object r2 = randomLiteral(returnType).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond1, "cond1"),
                        new TestCaseSupplier.TypedData(r1, returnType, "r1"),
                        cond(cond2, "cond2"),
                        new TestCaseSupplier.TypedData(r2, returnType, "r2")
                    );
                    return testCase(returnType, typedData, switch (result) {
                        case 0 -> r1;
                        case 1 -> r2;
                        case 2 -> null;
                        default -> throw new AssertionError("unsupported result " + result);
                    }, toStringMatcher(2, true), false, null, addWarnings(warnings));
                }
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType, returnType)),
                List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType, returnType),
                () -> {
                    Object r1 = randomLiteral(returnType).value();
                    Object r2 = randomLiteral(returnType).value();
                    Object r3 = randomLiteral(returnType).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond1, "cond1"),
                        new TestCaseSupplier.TypedData(r1, returnType, "r1"),
                        cond(cond2, "cond2"),
                        new TestCaseSupplier.TypedData(r2, returnType, "r2"),
                        new TestCaseSupplier.TypedData(r3, returnType, "r3")
                    );
                    return testCase(returnType, typedData, switch (result) {
                        case 0 -> r1;
                        case 1 -> r2;
                        case 2 -> r3;
                        default -> throw new AssertionError("unsupported result " + result);
                    }, toStringMatcher(2, false), false, null, addWarnings(warnings));
                }
            )
        );
        // Add some foldable and partially foldable cases. This isn't every combination of fold-ability, but it's many.
        switch (result) {
            case 0 -> {
                suppliers.add(
                    new TestCaseSupplier(
                        "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            Object r3 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1").forceLiteral(),
                                cond(cond2, "cond2"),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2"),
                                new TestCaseSupplier.TypedData(r3, returnType, "r3")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r1,
                                startsWith("LiteralsEvaluator[lit="),
                                true,
                                null,
                                addBuildEvaluatorWarnings(warnings)
                            );
                        }
                    )
                );
                suppliers.add(
                    new TestCaseSupplier(
                        "partial foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            Object r3 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1"),
                                cond(cond2, "cond2"),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2"),
                                new TestCaseSupplier.TypedData(r3, returnType, "r3")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r1,
                                startsWith("CaseLazyEvaluator[conditions=[ConditionEvaluator[condition=LiteralsEvaluator[lit="),
                                false,
                                List.of(typedData.get(1)),
                                addBuildEvaluatorWarnings(warnings)
                            );
                        }
                    )
                );
            }
            case 1 -> {
                suppliers.add(
                    new TestCaseSupplier(
                        "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            Object r3 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1").forceLiteral(),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r3, returnType, "r3")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r2,
                                startsWith("LiteralsEvaluator[lit="),
                                true,
                                null,
                                addBuildEvaluatorWarnings(warnings)
                            );
                        }
                    )
                );
                suppliers.add(
                    new TestCaseSupplier(
                        "partial foldable 1 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            Object r3 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1").forceLiteral(),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2"),
                                new TestCaseSupplier.TypedData(r3, returnType, "r3")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r2,
                                startsWith("CaseLazyEvaluator[conditions=[ConditionEvaluator[condition=LiteralsEvaluator[lit="),
                                false,
                                List.of(typedData.get(3)),
                                addWarnings(warnings)
                            );
                        }
                    )
                );
                suppliers.add(
                    new TestCaseSupplier(
                        "partial foldable 2 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1").forceLiteral(),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r2,
                                startsWith("CaseLazyEvaluator[conditions=[ConditionEvaluator[condition=LiteralsEvaluator[lit="),
                                false,
                                List.of(typedData.get(3)),
                                addWarnings(warnings)
                            );
                        }
                    )
                );
                suppliers.add(
                    new TestCaseSupplier(
                        "partial foldable 3 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1").forceLiteral(),
                                cond(cond2, "cond2"),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r2,
                                startsWith("CaseLazyEvaluator[conditions=[ConditionEvaluator[condition=LiteralsEvaluator[lit="),
                                false,
                                typedData.subList(2, 4),
                                addWarnings(warnings)
                            );
                        }
                    )
                );
            }
            case 2 -> {
                suppliers.add(
                    new TestCaseSupplier(
                        "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            Object r3 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1"),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2"),
                                new TestCaseSupplier.TypedData(r3, returnType, "r3").forceLiteral()
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r3,
                                startsWith("LiteralsEvaluator[lit="),
                                true,
                                null,
                                addBuildEvaluatorWarnings(warnings)
                            );
                        }
                    )
                );
                suppliers.add(
                    new TestCaseSupplier(
                        "partial foldable 1 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            Object r3 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1"),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2"),
                                new TestCaseSupplier.TypedData(r3, returnType, "r3")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r3,
                                startsWith("CaseLazyEvaluator[conditions=[ConditionEvaluator[condition=LiteralsEvaluator[lit="),
                                false,
                                List.of(typedData.get(4)),
                                addWarnings(warnings)
                            );
                        }
                    )
                );
                suppliers.add(
                    new TestCaseSupplier(
                        "partial foldable 2 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, returnType, cond2, returnType, returnType)),
                        List.of(DataType.BOOLEAN, returnType, DataType.BOOLEAN, returnType, returnType),
                        () -> {
                            Object r1 = randomLiteral(returnType).value();
                            Object r2 = randomLiteral(returnType).value();
                            Object r3 = randomLiteral(returnType).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, returnType, "r1"),
                                cond(cond2, "cond2"),
                                new TestCaseSupplier.TypedData(r2, returnType, "r2"),
                                new TestCaseSupplier.TypedData(r3, returnType, "r3")
                            );
                            return testCase(
                                returnType,
                                typedData,
                                r3,
                                startsWith("CaseLazyEvaluator[conditions=[ConditionEvaluator[condition=LiteralsEvaluator[lit="),
                                false,
                                typedData.subList(2, 5),
                                addWarnings(warnings)
                            );
                        }
                    )
                );
            }
            default -> throw new IllegalArgumentException("unsupported " + result);
        }
    }

    private static Matcher<String> toStringMatcher(int conditions, boolean trailingNull) {
        StringBuilder result = new StringBuilder();
        result.append("Case");
        result.append(conditions == 1 ? "Eager" : "Lazy");
        result.append("Evaluator[conditions=[");
        int channel = 0;
        for (int i = 0; i < conditions; i++) {
            if (i != 0) {
                result.append(", ");
            }
            result.append("ConditionEvaluator[condition=Attribute[channel=").append(channel++);
            result.append("], value=Attribute[channel=").append(channel++).append("]]");
        }
        if (trailingNull) {
            result.append("], elseVal=LiteralsEvaluator[lit=null]]");
        } else {
            result.append("], elseVal=Attribute[channel=").append(channel).append("]]");
        }
        return equalTo(result.toString());
    }

    private static TestCaseSupplier.TypedData cond(Object cond, String name) {
        return new TestCaseSupplier.TypedData(cond instanceof Supplier<?> s ? s.get() : cond, DataType.BOOLEAN, name);
    }

    private static TestCaseSupplier.TestCase testCase(
        DataType returnType,
        List<TestCaseSupplier.TypedData> typedData,
        Object result,
        Matcher<String> evaluatorToString,
        boolean foldable,
        @Nullable List<TestCaseSupplier.TypedData> partialFold,
        Function<TestCaseSupplier.TestCase, TestCaseSupplier.TestCase> decorate
    ) {
        if (returnType == DataType.UNSIGNED_LONG && result != null) {
            result = NumericUtils.unsignedLongAsBigInteger((Long) result);
        }
        return decorate.apply(
            new TestCaseSupplier.TestCase(typedData, evaluatorToString, returnType, equalTo(result)).withExtra(
                new Extra(foldable, partialFold)
            )
        );
    }

    public CaseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Case build(Source source, List<Expression> args) {
        return new Case(Source.EMPTY, args.get(0), args.subList(1, args.size()));
    }

    private static Supplier<Boolean> randomSingleValuedCondition() {
        return new Supplier<>() {
            @Override
            public Boolean get() {
                return randomBoolean();
            }

            @Override
            public String toString() {
                return "multivalue";
            }
        };
    }

    private static Supplier<List<Boolean>> randomMultivaluedCondition() {
        return new Supplier<>() {
            @Override
            public List<Boolean> get() {
                return randomList(2, 100, ESTestCase::randomBoolean);
            }

            @Override
            public String toString() {
                return "multivalue";
            }
        };
    }

    public void testFancyFolding() {
        Expression e = buildFieldExpression(testCase);
        if (extra().foldable == false) {
            assertThat(e.foldable(), equalTo(false));
            return;
        }
        assertThat(e.foldable(), equalTo(true));
        Object result = e.fold(FoldContext.small());
        if (testCase.getExpectedBuildEvaluatorWarnings() != null) {
            assertWarnings(testCase.getExpectedBuildEvaluatorWarnings());
        }
        if (testCase.expectedType() == DataType.UNSIGNED_LONG && result != null) {
            result = NumericUtils.unsignedLongAsBigInteger((Long) result);
        }
        assertThat(result, testCase.getMatcher());
        if (testCase.getExpectedWarnings() != null) {
            assertWarnings(testCase.getExpectedWarnings());
        }
    }

    public void testPartialFold() {
        if (extra().foldable()) {
            // Nothing to do
            return;
        }
        Case c = (Case) buildFieldExpression(testCase);
        if (extra().expectedPartialFold == null) {
            assertThat(c.partiallyFold(FoldContext.small()), sameInstance(c));
            return;
        }
        if (extra().expectedPartialFold.size() == 1) {
            assertThat(c.partiallyFold(FoldContext.small()), equalToIgnoringIds(extra().expectedPartialFold.get(0).asField()));
            return;
        }
        Case expected = build(
            Source.synthetic("expected"),
            extra().expectedPartialFold.stream().map(TestCaseSupplier.TypedData::asField).toList()
        );
        assertThat(c.partiallyFold(FoldContext.small()), equalToIgnoringIds(expected));
    }

    private static Function<TestCaseSupplier.TestCase, TestCaseSupplier.TestCase> addWarnings(List<String> warnings) {
        return c -> {
            TestCaseSupplier.TestCase r = c;
            for (String warning : warnings) {
                r = r.withWarning(warning);
            }
            return r;
        };
    }

    private static Function<TestCaseSupplier.TestCase, TestCaseSupplier.TestCase> addBuildEvaluatorWarnings(List<String> warnings) {
        return c -> {
            TestCaseSupplier.TestCase r = c;
            for (String warning : warnings) {
                r = r.withBuildEvaluatorWarning(warning);
            }
            return r;
        };
    }

    private record Extra(boolean foldable, List<TestCaseSupplier.TypedData> expectedPartialFold) {}

    private Extra extra() {
        return (Extra) testCase.extra();
    }

    @Override
    protected Matcher<Object> allNullsMatcher() {
        if (extra().foldable) {
            return testCase.getMatcher();
        }
        return super.allNullsMatcher();
    }
}
