/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedData.MULTI_ROW_NULL;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedData.NULL;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MvLessTests extends AbstractScalarFunctionTestCase {
    public MvLessTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static String channels(boolean includeBound) {
        return "[field=Attribute[channel=0], bound=Attribute[channel=1], greater=false, includeBound=" + includeBound + "]";
    }

    private static final String CHANNELS = channels(false);

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        ints(suppliers);
        longType(suppliers, DataType.LONG, ESTestCase::randomLong);
        longType(suppliers, DataType.UNSIGNED_LONG, ESTestCase::randomLong);
        longType(suppliers, DataType.DATETIME, ESTestCase::randomLong);
        longType(suppliers, DataType.DATE_NANOS, ESTestCase::randomNonNegativeLong);
        doubles(suppliers);
        strings(suppliers);
        bytesRefType(suppliers, DataType.IP);
        bytesRefType(suppliers, DataType.VERSION);

        addInt(suppliers, List.of(1, 5, 10), 4, true); // 1 clears 4
        addInt(suppliers, List.of(0, 100), 40, true); // 0 clears 40
        addInt(suppliers, List.of(4), 4, false); // strict: equal does not match
        addInt(suppliers, List.of(5), 4, false); // above the bound

        List<TestCaseSupplier> withNulls = anyNullIsNull(
            suppliers,
            (nullPosition, nullValueDataType, original) -> original.expectedType(),
            (nullPosition, nullData, original) -> original
        );

        addIntOpt(withNulls, List.of(4), 4, true, true);
        addIntOpt(withNulls, List.of(4), 4, false, false);
        addIntOpt(withNulls, List.of(3), 4, false, true);
        addLongOpt(withNulls, DataType.LONG, List.of(5L), 5L, true, true);
        addLongOpt(withNulls, DataType.LONG, List.of(5L), 5L, false, false);
        addLongOpt(withNulls, DataType.DATETIME, List.of(5L), 9L, false, true);
        addDoubleOpt(withNulls, List.of(1.5), 1.5, true, true);
        addDoubleOpt(withNulls, List.of(1.5), 1.5, false, false);
        addBytesRefOpt(withNulls, "b", "b", true, true);
        addBytesRefOpt(withNulls, "b", "b", false, false);
        addBytesRefOpt(withNulls, "a", "b", false, true);

        return parameterSuppliersFromTypedData(withNulls);
    }

    private static TestCaseSupplier.TypedData options(boolean includeBound) {
        return new TestCaseSupplier.TypedData(
            new MapExpression(
                Source.EMPTY,
                List.of(Literal.keyword(Source.EMPTY, "include_bound"), new Literal(Source.EMPTY, includeBound, DataType.BOOLEAN))
            ),
            DataType.UNSUPPORTED,
            "options"
        ).forceLiteral();
    }

    private static void addIntOpt(
        List<TestCaseSupplier> suppliers,
        List<Integer> field,
        int bound,
        boolean includeBound,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                field + (includeBound ? " <= " : " < ") + bound,
                List.of(DataType.INTEGER, DataType.INTEGER, DataType.UNSUPPORTED),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(field, DataType.INTEGER, "field"),
                        new TestCaseSupplier.TypedData(bound, DataType.INTEGER, "bound"),
                        options(includeBound)
                    ),
                    "MvCompareIntEvaluator" + channels(includeBound),
                    DataType.BOOLEAN,
                    equalTo(expected)
                )
            )
        );
    }

    private static void addLongOpt(
        List<TestCaseSupplier> suppliers,
        DataType type,
        List<Long> field,
        long bound,
        boolean includeBound,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                type.typeName() + " " + field + (includeBound ? " <= " : " < ") + bound,
                List.of(type, type, DataType.UNSUPPORTED),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(field, type, "field"),
                        new TestCaseSupplier.TypedData(bound, type, "bound"),
                        options(includeBound)
                    ),
                    "MvCompareLongEvaluator" + channels(includeBound),
                    DataType.BOOLEAN,
                    equalTo(expected)
                )
            )
        );
    }

    private static void addDoubleOpt(
        List<TestCaseSupplier> suppliers,
        List<Double> field,
        double bound,
        boolean includeBound,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                "double " + field + (includeBound ? " <= " : " < ") + bound,
                List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.UNSUPPORTED),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(field, DataType.DOUBLE, "field"),
                        new TestCaseSupplier.TypedData(bound, DataType.DOUBLE, "bound"),
                        options(includeBound)
                    ),
                    "MvCompareDoubleEvaluator" + channels(includeBound),
                    DataType.BOOLEAN,
                    equalTo(expected)
                )
            )
        );
    }

    private static void addBytesRefOpt(
        List<TestCaseSupplier> suppliers,
        String field,
        String bound,
        boolean includeBound,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                "keyword " + field + (includeBound ? " <= " : " < ") + bound,
                List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.UNSUPPORTED),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(List.of(new BytesRef(field)), DataType.KEYWORD, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef(bound), DataType.KEYWORD, "bound"),
                        options(includeBound)
                    ),
                    "MvCompareBytesRefEvaluator" + channels(includeBound),
                    DataType.BOOLEAN,
                    equalTo(expected)
                )
            )
        );
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier("integer", List.of(DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> field = ESTestCase.randomList(1, 6, ESTestCase::randomInt);
            int bound = ESTestCase.randomInt();
            boolean expected = field.stream().anyMatch(v -> v < bound);
            return testCase(field, bound, DataType.INTEGER, "Int", expected);
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier("double", List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            List<Double> field = ESTestCase.randomList(1, 6, ESTestCase::randomDouble);
            double bound = ESTestCase.randomDouble();
            boolean expected = field.stream().anyMatch(v -> v < bound);
            return testCase(field, bound, DataType.DOUBLE, "Double", expected);
        }));
    }

    private static void longType(List<TestCaseSupplier> suppliers, DataType type, Supplier<Long> gen) {
        suppliers.add(new TestCaseSupplier(type.typeName(), List.of(type, type), () -> {
            List<Long> field = ESTestCase.randomList(1, 6, gen);
            long bound = gen.get();
            boolean expected = field.stream().anyMatch(v -> v < bound);
            return testCase(field, bound, type, "Long", expected);
        }));
    }

    private static void strings(List<TestCaseSupplier> suppliers) {
        DataType[] stringTypes = { DataType.KEYWORD, DataType.TEXT };
        for (DataType fieldType : stringTypes) {
            for (DataType boundType : stringTypes) {
                suppliers.add(new TestCaseSupplier(fieldType + " < " + boundType, List.of(fieldType, boundType), () -> {
                    List<BytesRef> field = ESTestCase.randomList(1, 6, () -> (BytesRef) randomLiteral(DataType.KEYWORD).value());
                    BytesRef bound = (BytesRef) randomLiteral(DataType.KEYWORD).value();
                    boolean expected = field.stream().anyMatch(v -> v.compareTo(bound) < 0);
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(field, fieldType, "field"),
                            new TestCaseSupplier.TypedData(bound, boundType, "bound")
                        ),
                        "MvCompareBytesRefEvaluator" + CHANNELS,
                        DataType.BOOLEAN,
                        equalTo(expected)
                    );
                }));
            }
        }
    }

    private static void bytesRefType(List<TestCaseSupplier> suppliers, DataType type) {
        suppliers.add(new TestCaseSupplier(type.typeName(), List.of(type, type), () -> {
            List<BytesRef> field = ESTestCase.randomList(1, 6, () -> (BytesRef) randomLiteral(type).value());
            BytesRef bound = (BytesRef) randomLiteral(type).value();
            boolean expected = field.stream().anyMatch(v -> v.compareTo(bound) < 0);
            return testCase(field, bound, type, "BytesRef", expected);
        }));
    }

    private static void addInt(List<TestCaseSupplier> suppliers, List<Integer> field, int bound, boolean expected) {
        suppliers.add(
            new TestCaseSupplier(
                field + " < " + bound,
                List.of(DataType.INTEGER, DataType.INTEGER),
                () -> testCase(field, bound, DataType.INTEGER, "Int", expected)
            )
        );
    }

    private static TestCaseSupplier.TestCase testCase(List<?> field, Object bound, DataType type, String evaluator, boolean expected) {
        return new TestCaseSupplier.TestCase(
            List.of(new TestCaseSupplier.TypedData(field, type, "field"), new TestCaseSupplier.TypedData(bound, type, "bound")),
            "MvCompare" + evaluator + "Evaluator" + CHANNELS,
            DataType.BOOLEAN,
            equalTo(expected)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvLess(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(false);
    }

    protected static List<TestCaseSupplier> anyNullIsNull(
        List<TestCaseSupplier> testCaseSuppliers,
        ExpectedType expectedType,
        ExpectedEvaluatorToString evaluatorToString
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers);
        Set<List<DataType>> uniqueSignatures = new HashSet<>();
        for (TestCaseSupplier original : testCaseSuppliers) {
            boolean firstTimeSeenSignature = uniqueSignatures.add(original.types());
            for (int typeIndex = 0; typeIndex < original.types().size(); typeIndex++) {
                int nullPosition = typeIndex;

                suppliers.add(new TestCaseSupplier("G1: " + original.name() + " null in " + nullPosition, original.types(), () -> {
                    TestCaseSupplier.TestCase originalTestCase = original.get();
                    List<TestCaseSupplier.TypedData> typeDataWithNull = new ArrayList<>(originalTestCase.getData());
                    var data = typeDataWithNull.get(nullPosition);
                    typeDataWithNull.set(nullPosition, data.withData(data.isMultiRow() ? Collections.singletonList(null) : null));
                    TestCaseSupplier.TypedData nulledData = originalTestCase.getData().get(nullPosition);
                    return new TestCaseSupplier.TestCase(
                        typeDataWithNull,
                        evaluatorToString.evaluatorToString(nullPosition, nulledData, originalTestCase.evaluatorToString()),
                        expectedType.expectedType(nullPosition, DataType.BOOLEAN, originalTestCase),
                        is(false)
                    );
                }));

                if (firstTimeSeenSignature) {
                    var typesWithNull = new ArrayList<>(original.types());
                    typesWithNull.set(nullPosition, DataType.NULL);
                    if (uniqueSignatures.add(typesWithNull)) {
                        suppliers.add(
                            new TestCaseSupplier(
                                "G2: " + toSpaceSeparatedString(typesWithNull) + " null in " + nullPosition,
                                typesWithNull,
                                () -> {
                                    TestCaseSupplier.TestCase originalTestCase = original.get();
                                    var typeDataWithNull = new ArrayList<>(originalTestCase.getData());
                                    typeDataWithNull.set(
                                        nullPosition,
                                        typeDataWithNull.get(nullPosition).isMultiRow() ? MULTI_ROW_NULL : NULL
                                    );
                                    return new TestCaseSupplier.TestCase(
                                        typeDataWithNull,
                                        "ConstantFalse",
                                        expectedType.expectedType(nullPosition, DataType.BOOLEAN, originalTestCase),
                                        is(false)
                                    );
                                }
                            )
                        );
                    }
                }
            }
        }
        return suppliers;
    }

    private static String toSpaceSeparatedString(List<DataType> types) {
        return types.stream().map(Objects::toString).collect(Collectors.joining(" "));
    }
}
