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

public class MvInRangeTests extends AbstractScalarFunctionTestCase {
    public MvInRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static String channels(boolean includeLower, boolean includeUpper) {
        return "[field=Attribute[channel=0], lower=Attribute[channel=1], upper=Attribute[channel=2], includeLower="
            + includeLower
            + ", includeUpper="
            + includeUpper
            + "]";
    }

    // The default (3-argument) form resolves both flags to true.
    private static final String CHANNELS = channels(true, true);

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // One randomized supplier per ordered type, so the coverage guard (resolveType == @Param == switch == tests)
        // is satisfied for every supported type and the per-element-type evaluator is exercised.
        ints(suppliers);
        longType(suppliers, DataType.LONG, ESTestCase::randomLong);
        longType(suppliers, DataType.UNSIGNED_LONG, ESTestCase::randomLong);
        longType(suppliers, DataType.DATETIME, ESTestCase::randomLong);
        longType(suppliers, DataType.DATE_NANOS, ESTestCase::randomNonNegativeLong);
        doubles(suppliers);
        strings(suppliers); // keyword/text, every field x lower x upper combination (they are interchangeable)
        bytesRefType(suppliers, DataType.IP);
        bytesRefType(suppliers, DataType.VERSION);

        // Deterministic pins for the load-bearing semantics: an existential over values, not an envelope overlap.
        addInt(suppliers, List.of(0, 100), 40, 60, false); // [0,100] does NOT intersect [40,60] — the whole point
        addInt(suppliers, List.of(0, 50, 100), 40, 60, true); // 50 is inside
        addInt(suppliers, List.of(2), 2, 3, true); // inclusive lower bound
        addInt(suppliers, List.of(3), 2, 3, true); // inclusive upper bound
        addInt(suppliers, List.of(4), 2, 3, false); // above the range
        addInt(suppliers, List.of(5, 6, 7), 6, 2, false); // lower > upper: empty range matches nothing

        List<TestCaseSupplier> withNulls = anyNullIsNull(
            suppliers,
            (nullPosition, nullValueDataType, original) -> original.expectedType(),
            (nullPosition, nullData, original) -> original
        );

        // Options-bearing (4-argument) suppliers are added AFTER the null wrap: anyNullIsNull would null the options map,
        // producing a NULL literal in the options slot and breaking the (MapExpression) handling. Deterministic pins for
        // every boundary mode at the exact bounds, plus one randomized supplier per element-type family so both branch
        // sides of each kernel run.
        addIntOpt(withNulls, List.of(2), 2, 3, false, true, false); // exclusive lower excludes a value on the lower bound
        addIntOpt(withNulls, List.of(3), 2, 3, false, true, true); // inclusive upper includes a value on the upper bound
        addIntOpt(withNulls, List.of(2), 2, 3, true, false, true); // inclusive lower includes a value on the lower bound
        addIntOpt(withNulls, List.of(3), 2, 3, true, false, false); // exclusive upper excludes a value on the upper bound
        addIntOpt(withNulls, List.of(2, 3), 2, 3, false, false, false); // open range with no interior integer
        addIntOpt(withNulls, List.of(3), 2, 5, false, false, true); // open range with an interior value
        addIntOpt(withNulls, List.of(2), 2, 3, true, true, true); // explicit inclusive equals the default
        addIntOpt(withNulls, List.of(5), 5, 5, true, true, true); // closed [5, 5] matches the point value
        addIntOpt(withNulls, List.of(5), 5, 5, false, false, false); // open (5, 5) is empty — the point does not match
        addIntOpt(withNulls, List.of(5, 6), 5, 6, false, false, false); // open (5, 6) — no integer strictly between

        // Exercise the exclusive branch of each of the other three element-type kernels (long, double, bytes_ref); the
        // 3-argument suppliers above already cover the inclusive branch of every kernel.
        addLongOpt(withNulls, DataType.LONG, List.of(5L), 5L, 9L, false, true, false); // exclusive lower on a lower-bound value
        addLongOpt(withNulls, DataType.LONG, List.of(9L), 5L, 9L, true, false, false); // exclusive upper on an upper-bound value
        addLongOpt(withNulls, DataType.LONG, List.of(7L), 5L, 9L, false, false, true); // open range, interior value
        addLongOpt(withNulls, DataType.DATETIME, List.of(9L), 5L, 9L, true, false, false); // date rides the LONG kernel too
        addDoubleOpt(withNulls, List.of(1.5), 1.5, 3.5, false, true, false); // exclusive lower
        addDoubleOpt(withNulls, List.of(3.5), 1.5, 3.5, true, false, false); // exclusive upper
        addDoubleOpt(withNulls, List.of(2.5), 1.5, 3.5, false, false, true); // open range, interior value
        addBytesRefOpt(withNulls, "b", "b", "y", false, true, false); // exclusive lower on a lower-bound value
        addBytesRefOpt(withNulls, "y", "b", "y", true, false, false); // exclusive upper on an upper-bound value
        addBytesRefOpt(withNulls, "m", "b", "y", false, false, true); // open range, interior value

        return parameterSuppliersFromTypedData(withNulls);
    }

    /** A boolean options map {@code {"include_lower": il, "include_upper": iu}} as a forced-literal argument. */
    private static TestCaseSupplier.TypedData options(boolean includeLower, boolean includeUpper) {
        return new TestCaseSupplier.TypedData(
            new MapExpression(
                Source.EMPTY,
                List.of(
                    Literal.keyword(Source.EMPTY, "include_lower"),
                    new Literal(Source.EMPTY, includeLower, DataType.BOOLEAN),
                    Literal.keyword(Source.EMPTY, "include_upper"),
                    new Literal(Source.EMPTY, includeUpper, DataType.BOOLEAN)
                )
            ),
            DataType.UNSUPPORTED,
            "options"
        ).forceLiteral();
    }

    private static void addIntOpt(
        List<TestCaseSupplier> suppliers,
        List<Integer> field,
        int lower,
        int upper,
        boolean includeLower,
        boolean includeUpper,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                field + " in " + (includeLower ? "[" : "(") + lower + "," + upper + (includeUpper ? "]" : ")"),
                List.of(DataType.INTEGER, DataType.INTEGER, DataType.INTEGER, DataType.UNSUPPORTED),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(field, DataType.INTEGER, "field"),
                        new TestCaseSupplier.TypedData(lower, DataType.INTEGER, "lower"),
                        new TestCaseSupplier.TypedData(upper, DataType.INTEGER, "upper"),
                        options(includeLower, includeUpper)
                    ),
                    "MvInRangeIntEvaluator" + channels(includeLower, includeUpper),
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
        long lower,
        long upper,
        boolean includeLower,
        boolean includeUpper,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                type.typeName() + " " + field + " in " + (includeLower ? "[" : "(") + lower + "," + upper + (includeUpper ? "]" : ")"),
                List.of(type, type, type, DataType.UNSUPPORTED),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(field, type, "field"),
                        new TestCaseSupplier.TypedData(lower, type, "lower"),
                        new TestCaseSupplier.TypedData(upper, type, "upper"),
                        options(includeLower, includeUpper)
                    ),
                    "MvInRangeLongEvaluator" + channels(includeLower, includeUpper),
                    DataType.BOOLEAN,
                    equalTo(expected)
                )
            )
        );
    }

    private static void addDoubleOpt(
        List<TestCaseSupplier> suppliers,
        List<Double> field,
        double lower,
        double upper,
        boolean includeLower,
        boolean includeUpper,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                "double " + field + " in " + (includeLower ? "[" : "(") + lower + "," + upper + (includeUpper ? "]" : ")"),
                List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.UNSUPPORTED),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(field, DataType.DOUBLE, "field"),
                        new TestCaseSupplier.TypedData(lower, DataType.DOUBLE, "lower"),
                        new TestCaseSupplier.TypedData(upper, DataType.DOUBLE, "upper"),
                        options(includeLower, includeUpper)
                    ),
                    "MvInRangeDoubleEvaluator" + channels(includeLower, includeUpper),
                    DataType.BOOLEAN,
                    equalTo(expected)
                )
            )
        );
    }

    private static void addBytesRefOpt(
        List<TestCaseSupplier> suppliers,
        String field,
        String lower,
        String upper,
        boolean includeLower,
        boolean includeUpper,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                "keyword " + field + " in " + (includeLower ? "[" : "(") + lower + "," + upper + (includeUpper ? "]" : ")"),
                List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD, DataType.UNSUPPORTED),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(List.of(new BytesRef(field)), DataType.KEYWORD, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef(lower), DataType.KEYWORD, "lower"),
                        new TestCaseSupplier.TypedData(new BytesRef(upper), DataType.KEYWORD, "upper"),
                        options(includeLower, includeUpper)
                    ),
                    "MvInRangeBytesRefEvaluator" + channels(includeLower, includeUpper),
                    DataType.BOOLEAN,
                    equalTo(expected)
                )
            )
        );
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier("integer", List.of(DataType.INTEGER, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> field = ESTestCase.randomList(1, 6, ESTestCase::randomInt);
            int lo = ESTestCase.randomInt();
            int hi = ESTestCase.randomInt();
            int a = Math.min(lo, hi);
            int b = Math.max(lo, hi);
            boolean expected = field.stream().anyMatch(v -> v >= a && v <= b);
            return testCase(field, a, b, DataType.INTEGER, "Int", expected);
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier("double", List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE), () -> {
            List<Double> field = ESTestCase.randomList(1, 6, ESTestCase::randomDouble);
            double lo = ESTestCase.randomDouble();
            double hi = ESTestCase.randomDouble();
            double a = Math.min(lo, hi);
            double b = Math.max(lo, hi);
            boolean expected = field.stream().anyMatch(v -> v >= a && v <= b);
            return testCase(field, a, b, DataType.DOUBLE, "Double", expected);
        }));
    }

    private static void longType(List<TestCaseSupplier> suppliers, DataType type, Supplier<Long> gen) {
        suppliers.add(new TestCaseSupplier(type.typeName(), List.of(type, type, type), () -> {
            List<Long> field = ESTestCase.randomList(1, 6, gen);
            long lo = gen.get();
            long hi = gen.get();
            long a = Math.min(lo, hi);
            long b = Math.max(lo, hi);
            boolean expected = field.stream().anyMatch(v -> v >= a && v <= b);
            return testCase(field, a, b, type, "Long", expected);
        }));
    }

    // keyword and text are interchangeable, so supply every field x lower x upper combination over the two — mirrors
    // how MvContainsTests enumerates its keyword/text pairs, and keeps the error harness from treating a mix as invalid.
    private static void strings(List<TestCaseSupplier> suppliers) {
        DataType[] stringTypes = { DataType.KEYWORD, DataType.TEXT };
        for (DataType fieldType : stringTypes) {
            for (DataType lowerType : stringTypes) {
                for (DataType upperType : stringTypes) {
                    suppliers.add(
                        new TestCaseSupplier(
                            fieldType + " in [" + lowerType + "," + upperType + "]",
                            List.of(fieldType, lowerType, upperType),
                            () -> {
                                List<BytesRef> field = ESTestCase.randomList(
                                    1,
                                    6,
                                    () -> (BytesRef) randomLiteral(DataType.KEYWORD).value()
                                );
                                BytesRef lo = (BytesRef) randomLiteral(DataType.KEYWORD).value();
                                BytesRef hi = (BytesRef) randomLiteral(DataType.KEYWORD).value();
                                BytesRef a = lo.compareTo(hi) <= 0 ? lo : hi;
                                BytesRef b = lo.compareTo(hi) <= 0 ? hi : lo;
                                boolean expected = field.stream().anyMatch(v -> v.compareTo(a) >= 0 && v.compareTo(b) <= 0);
                                return new TestCaseSupplier.TestCase(
                                    List.of(
                                        new TestCaseSupplier.TypedData(field, fieldType, "field"),
                                        new TestCaseSupplier.TypedData(a, lowerType, "lower"),
                                        new TestCaseSupplier.TypedData(b, upperType, "upper")
                                    ),
                                    "MvInRangeBytesRefEvaluator" + CHANNELS,
                                    DataType.BOOLEAN,
                                    equalTo(expected)
                                );
                            }
                        )
                    );
                }
            }
        }
    }

    private static void bytesRefType(List<TestCaseSupplier> suppliers, DataType type) {
        suppliers.add(new TestCaseSupplier(type.typeName(), List.of(type, type, type), () -> {
            List<BytesRef> field = ESTestCase.randomList(1, 6, () -> (BytesRef) randomLiteral(type).value());
            BytesRef lo = (BytesRef) randomLiteral(type).value();
            BytesRef hi = (BytesRef) randomLiteral(type).value();
            BytesRef a = lo.compareTo(hi) <= 0 ? lo : hi;
            BytesRef b = lo.compareTo(hi) <= 0 ? hi : lo;
            boolean expected = field.stream().anyMatch(v -> v.compareTo(a) >= 0 && v.compareTo(b) <= 0);
            return testCase(field, a, b, type, "BytesRef", expected);
        }));
    }

    private static void addInt(List<TestCaseSupplier> suppliers, List<Integer> field, int lower, int upper, boolean expected) {
        suppliers.add(
            new TestCaseSupplier(
                field + " in [" + lower + "," + upper + "]",
                List.of(DataType.INTEGER, DataType.INTEGER, DataType.INTEGER),
                () -> testCase(field, lower, upper, DataType.INTEGER, "Int", expected)
            )
        );
    }

    private static TestCaseSupplier.TestCase testCase(
        List<?> field,
        Object lower,
        Object upper,
        DataType type,
        String evaluator,
        boolean expected
    ) {
        return new TestCaseSupplier.TestCase(
            List.of(
                new TestCaseSupplier.TypedData(field, type, "field"),
                new TestCaseSupplier.TypedData(lower, type, "lower"),
                new TestCaseSupplier.TypedData(upper, type, "upper")
            ),
            "MvInRange" + evaluator + "Evaluator" + CHANNELS,
            DataType.BOOLEAN,
            equalTo(expected)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvInRange(source, args.get(0), args.get(1), args.get(2), args.size() > 3 ? args.get(3) : null);
    }

    // mv_in_range never returns null: an empty/null field (and a null bound) is false, so an all-null position is false.
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(false);
    }

    /**
     * A two-valued variant of {@link org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase#anyNullIsNull}:
     * because mv_in_range never returns null, a null argument yields {@code false}, and a null-typed argument folds to a
     * constant false. Modeled on the same helper in {@code MvIntersectsTests}.
     */
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

                // The whole argument's data is null, but its type is kept: the per-element-type evaluator still runs and
                // returns false (empty/null value at every position).
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
                        // The argument is a null literal (null type): the expression folds to a constant false.
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
