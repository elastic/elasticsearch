/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class CaseTests extends AbstractScalarFunctionTestCase {

    private static final List<DataType> TYPES;
    static {
        List<DataType> t = Stream.of(
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.DOUBLE,
            DataType.INTEGER,
            DataType.LONG,
            DataType.UNSIGNED_LONG,
            DataType.IP,
            DataType.VERSION,
            DataType.CARTESIAN_POINT,
            DataType.GEO_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.GEO_SHAPE,
            DataType.NULL
        ).collect(Collectors.toList());
        if (Build.current().isSnapshot()) {
            t.addAll(DataType.UNDER_CONSTRUCTION.keySet());
        }
        TYPES = unmodifiableList(t);
    }

    public CaseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Generate the test cases for this test
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType type : TYPES) {
            twoAndThreeArgs(suppliers, true, true, type, List.of());
            twoAndThreeArgs(suppliers, false, false, type, List.of());
            twoAndThreeArgs(suppliers, null, false, type, List.of());
            twoAndThreeArgs(
                suppliers,
                randomMultivaluedCondition(),
                false,
                type,
                List.of(
                    "Line -1:-1: evaluation of [cond] failed, treating result as false. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.IllegalArgumentException: CASE expects a single-valued boolean"
                )
            );
        }
        suppliers = errorsForCasesWithoutExamples(
            suppliers,
            (includeOrdinal, validPerPosition, types) -> typeErrorMessage(includeOrdinal, types)
        );

        for (DataType type : TYPES) {
            fourAndFiveArgs(suppliers, true, randomSingleValuedCondition(), 0, type, List.of());
            fourAndFiveArgs(suppliers, false, true, 1, type, List.of());
            fourAndFiveArgs(suppliers, false, false, 2, type, List.of());
            fourAndFiveArgs(suppliers, null, true, 1, type, List.of());
            fourAndFiveArgs(suppliers, null, false, 2, type, List.of());
            fourAndFiveArgs(
                suppliers,
                randomMultivaluedCondition(),
                true,
                1,
                type,
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
                type,
                List.of(
                    "Line -1:-1: evaluation of [cond2] failed, treating result as false. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.IllegalArgumentException: CASE expects a single-valued boolean"
                )
            );
        }
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void twoAndThreeArgs(
        List<TestCaseSupplier> suppliers,
        Object cond,
        boolean lhsOrRhs,
        DataType type,
        List<String> warnings
    ) {
        suppliers.add(new TestCaseSupplier(TestCaseSupplier.nameFrom(Arrays.asList(cond, type)), List.of(DataType.BOOLEAN, type), () -> {
            Object lhs = randomLiteral(type).value();
            List<TestCaseSupplier.TypedData> typedData = List.of(cond(cond, "cond"), new TestCaseSupplier.TypedData(lhs, type, "lhs"));
            return testCase(type, typedData, lhsOrRhs ? lhs : null, toStringMatcher(1, true), false, null, addWarnings(warnings));
        }));
        suppliers.add(
            new TestCaseSupplier(TestCaseSupplier.nameFrom(Arrays.asList(cond, type, type)), List.of(DataType.BOOLEAN, type, type), () -> {
                Object lhs = randomLiteral(type).value();
                Object rhs = randomLiteral(type).value();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    cond(cond, "cond"),
                    new TestCaseSupplier.TypedData(lhs, type, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, type, "rhs")
                );
                return testCase(type, typedData, lhsOrRhs ? lhs : rhs, toStringMatcher(1, false), false, null, addWarnings(warnings));
            })
        );
        if (lhsOrRhs) {
            suppliers.add(
                new TestCaseSupplier(
                    "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond, type, type)),
                    List.of(DataType.BOOLEAN, type, type),
                    () -> {
                        Object lhs = randomLiteral(type).value();
                        Object rhs = randomLiteral(type).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            cond(cond, "cond").forceLiteral(),
                            new TestCaseSupplier.TypedData(lhs, type, "lhs").forceLiteral(),
                            new TestCaseSupplier.TypedData(rhs, type, "rhs")
                        );
                        return testCase(
                            type,
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
                    "partial foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond, type)),
                    List.of(DataType.BOOLEAN, type),
                    () -> {
                        Object lhs = randomLiteral(type).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            cond(cond, "cond").forceLiteral(),
                            new TestCaseSupplier.TypedData(lhs, type, "lhs")
                        );
                        return testCase(
                            type,
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
                    "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond, type)),
                    List.of(DataType.BOOLEAN, type),
                    () -> {
                        Object lhs = randomLiteral(type).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            cond(cond, "cond").forceLiteral(),
                            new TestCaseSupplier.TypedData(lhs, type, "lhs")
                        );
                        return testCase(
                            type,
                            typedData,
                            null,
                            startsWith("LiteralsEvaluator[lit="),
                            true,
                            List.of(new TestCaseSupplier.TypedData(null, type, "null").forceLiteral()),
                            addBuildEvaluatorWarnings(warnings)
                        );
                    }
                )
            );
        }

        suppliers.add(
            new TestCaseSupplier(
                "partial foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond, type, type)),
                List.of(DataType.BOOLEAN, type, type),
                () -> {
                    Object lhs = randomLiteral(type).value();
                    Object rhs = randomLiteral(type).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond, "cond").forceLiteral(),
                        new TestCaseSupplier.TypedData(lhs, type, "lhs"),
                        new TestCaseSupplier.TypedData(rhs, type, "rhs")
                    );
                    return testCase(
                        type,
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
                new TestCaseSupplier(TestCaseSupplier.nameFrom(Arrays.asList(DataType.NULL, type)), List.of(DataType.NULL, type), () -> {
                    Object lhs = randomLiteral(type).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        new TestCaseSupplier.TypedData(null, DataType.NULL, "cond"),
                        new TestCaseSupplier.TypedData(lhs, type, "lhs")
                    );
                    return testCase(
                        type,
                        typedData,
                        lhsOrRhs ? lhs : null,
                        startsWith("CaseEagerEvaluator[conditions=[ConditionEvaluator[condition="),
                        false,
                        null,
                        addWarnings(warnings)
                    );
                })
            );
            suppliers.add(
                new TestCaseSupplier(
                    TestCaseSupplier.nameFrom(Arrays.asList(DataType.NULL, type, type)),
                    List.of(DataType.NULL, type, type),
                    () -> {
                        Object lhs = randomLiteral(type).value();
                        Object rhs = randomLiteral(type).value();
                        List<TestCaseSupplier.TypedData> typedData = List.of(
                            new TestCaseSupplier.TypedData(null, DataType.NULL, "cond"),
                            new TestCaseSupplier.TypedData(lhs, type, "lhs"),
                            new TestCaseSupplier.TypedData(rhs, type, "rhs")
                        );
                        return testCase(
                            type,
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
        suppliers.add(
            new TestCaseSupplier(
                TestCaseSupplier.nameFrom(Arrays.asList(cond, DataType.NULL, type)),
                List.of(DataType.BOOLEAN, DataType.NULL, type),
                () -> {
                    Object rhs = randomLiteral(type).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond, "cond"),
                        new TestCaseSupplier.TypedData(null, DataType.NULL, "lhs"),
                        new TestCaseSupplier.TypedData(rhs, type, "rhs")
                    );
                    return testCase(
                        type,
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
                TestCaseSupplier.nameFrom(Arrays.asList(cond, type, DataType.NULL)),
                List.of(DataType.BOOLEAN, type, DataType.NULL),
                () -> {
                    Object lhs = randomLiteral(type).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond, "cond"),
                        new TestCaseSupplier.TypedData(lhs, type, "lhs"),
                        new TestCaseSupplier.TypedData(null, DataType.NULL, "rhs")
                    );
                    return testCase(
                        type,
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
        DataType type,
        List<String> warnings
    ) {
        suppliers.add(
            new TestCaseSupplier(
                TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type)),
                List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type),
                () -> {
                    Object r1 = randomLiteral(type).value();
                    Object r2 = randomLiteral(type).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond1, "cond1"),
                        new TestCaseSupplier.TypedData(r1, type, "r1"),
                        cond(cond2, "cond2"),
                        new TestCaseSupplier.TypedData(r2, type, "r2")
                    );
                    return testCase(type, typedData, switch (result) {
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
                TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type, type)),
                List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type, type),
                () -> {
                    Object r1 = randomLiteral(type).value();
                    Object r2 = randomLiteral(type).value();
                    Object r3 = randomLiteral(type).value();
                    List<TestCaseSupplier.TypedData> typedData = List.of(
                        cond(cond1, "cond1"),
                        new TestCaseSupplier.TypedData(r1, type, "r1"),
                        cond(cond2, "cond2"),
                        new TestCaseSupplier.TypedData(r2, type, "r2"),
                        new TestCaseSupplier.TypedData(r3, type, "r3")
                    );
                    return testCase(type, typedData, switch (result) {
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
                        "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            Object r3 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1").forceLiteral(),
                                cond(cond2, "cond2"),
                                new TestCaseSupplier.TypedData(r2, type, "r2"),
                                new TestCaseSupplier.TypedData(r3, type, "r3")
                            );
                            return testCase(
                                type,
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
                        "partial foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            Object r3 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1"),
                                cond(cond2, "cond2"),
                                new TestCaseSupplier.TypedData(r2, type, "r2"),
                                new TestCaseSupplier.TypedData(r3, type, "r3")
                            );
                            return testCase(
                                type,
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
                        "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            Object r3 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1").forceLiteral(),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, type, "r2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r3, type, "r3")
                            );
                            return testCase(
                                type,
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
                        "partial foldable 1 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            Object r3 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1").forceLiteral(),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, type, "r2"),
                                new TestCaseSupplier.TypedData(r3, type, "r3")
                            );
                            return testCase(
                                type,
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
                        "partial foldable 2 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1").forceLiteral(),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, type, "r2")
                            );
                            return testCase(
                                type,
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
                        "partial foldable 3 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1").forceLiteral(),
                                cond(cond2, "cond2"),
                                new TestCaseSupplier.TypedData(r2, type, "r2")
                            );
                            return testCase(
                                type,
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
                        "foldable " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            Object r3 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1"),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, type, "r2"),
                                new TestCaseSupplier.TypedData(r3, type, "r3").forceLiteral()
                            );
                            return testCase(
                                type,
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
                        "partial foldable 1 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            Object r3 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1"),
                                cond(cond2, "cond2").forceLiteral(),
                                new TestCaseSupplier.TypedData(r2, type, "r2"),
                                new TestCaseSupplier.TypedData(r3, type, "r3")
                            );
                            return testCase(
                                type,
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
                        "partial foldable 2 " + TestCaseSupplier.nameFrom(Arrays.asList(cond1, type, cond2, type, type)),
                        List.of(DataType.BOOLEAN, type, DataType.BOOLEAN, type, type),
                        () -> {
                            Object r1 = randomLiteral(type).value();
                            Object r2 = randomLiteral(type).value();
                            Object r3 = randomLiteral(type).value();
                            List<TestCaseSupplier.TypedData> typedData = List.of(
                                cond(cond1, "cond1").forceLiteral(),
                                new TestCaseSupplier.TypedData(r1, type, "r1"),
                                cond(cond2, "cond2"),
                                new TestCaseSupplier.TypedData(r2, type, "r2"),
                                new TestCaseSupplier.TypedData(r3, type, "r3")
                            );
                            return testCase(
                                type,
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
        DataType type,
        List<TestCaseSupplier.TypedData> typedData,
        Object result,
        Matcher<String> evaluatorToString,
        boolean foldable,
        @Nullable List<TestCaseSupplier.TypedData> partialFold,
        Function<TestCaseSupplier.TestCase, TestCaseSupplier.TestCase> decorate
    ) {
        if (type == DataType.UNSIGNED_LONG && result != null) {
            result = NumericUtils.unsignedLongAsBigInteger((Long) result);
        }
        return decorate.apply(
            new TestCaseSupplier.TestCase(typedData, evaluatorToString, type, equalTo(result)).withExtra(new Extra(foldable, partialFold))
        );
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
        if (testCase.getExpectedTypeError() != null) {
            // Nothing to do
            return;
        }
        Expression e = buildFieldExpression(testCase);
        if (extra().foldable == false) {
            assertThat(e.foldable(), equalTo(false));
            return;
        }
        assertThat(e.foldable(), equalTo(true));
        Object result = e.fold();
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
        if (testCase.getExpectedTypeError() != null || extra().foldable()) {
            // Nothing to do
            return;
        }
        Case c = (Case) buildFieldExpression(testCase);
        if (extra().expectedPartialFold == null) {
            assertThat(c.partiallyFold(), sameInstance(c));
            return;
        }
        if (extra().expectedPartialFold.size() == 1) {
            assertThat(c.partiallyFold(), equalTo(extra().expectedPartialFold.get(0).asField()));
            return;
        }
        Case expected = build(
            Source.synthetic("expected"),
            extra().expectedPartialFold.stream().map(TestCaseSupplier.TypedData::asField).toList()
        );
        assertThat(c.partiallyFold(), equalTo(expected));
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

    private static String typeErrorMessage(boolean includeOrdinal, List<DataType> types) {
        if (types.get(0) != DataType.BOOLEAN && types.get(0) != DataType.NULL) {
            return typeErrorMessage(includeOrdinal, types, 0, "boolean");
        }
        DataType mainType = types.get(1);
        for (int i = 2; i < types.size(); i++) {
            if (i % 2 == 0 && i != types.size() - 1) {
                // condition
                if (types.get(i) != DataType.BOOLEAN && types.get(i) != DataType.NULL) {
                    return typeErrorMessage(includeOrdinal, types, i, "boolean");
                }
            } else {
                // value
                if (types.get(i) != mainType) {
                    return typeErrorMessage(includeOrdinal, types, i, mainType.typeName());
                }
            }
        }
        throw new IllegalStateException("can't find bad arg for " + types);
    }

    private static String typeErrorMessage(boolean includeOrdinal, List<DataType> types, int badArgPosition, String expectedTypeString) {
        String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " " : "";
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [] must be [" + expectedTypeString + "], found value [" + name + "] type [" + name + "]";
    }
}
