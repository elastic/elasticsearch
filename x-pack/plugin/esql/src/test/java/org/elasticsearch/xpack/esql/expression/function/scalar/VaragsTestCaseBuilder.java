/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Builds test cases for variable argument functions.
 */
public class VaragsTestCaseBuilder {
    private static final int MAX_WIDTH = 10;

    private Function<String, String> expectedEvaluatorPrefix;
    private Function<String, String> expectedEvaluatorValueMap = Function.identity();
    private Function<String[][], Matcher<Object>> expectedStr;
    private Function<long[][], Matcher<Object>> expectedLong;
    private Function<int[][], Matcher<Object>> expectedInt;
    // TODO double
    private Function<boolean[][], Matcher<Object>> expectedBoolean;

    /**
     * Build the builder.
     * @param expectedEvaluatorPrefix maps from a type name to the name of the matcher
     */
    public VaragsTestCaseBuilder(Function<String, String> expectedEvaluatorPrefix) {
        this.expectedEvaluatorPrefix = expectedEvaluatorPrefix;
    }

    /**
     * Wraps each evaluator's toString.
     */
    public VaragsTestCaseBuilder expectedEvaluatorValueWrap(Function<String, String> expectedEvaluatorValueMap) {
        this.expectedEvaluatorValueMap = expectedEvaluatorValueMap;
        return this;
    }

    public VaragsTestCaseBuilder expectString(Function<Stream<String[]>, Optional<String[]>> expectedStr) {
        this.expectedStr = strings -> {
            if (Arrays.stream(strings).anyMatch(s -> s == null)) {
                return nullValue();
            }
            Optional<String[]> expected = expectedStr.apply(Arrays.stream(strings));
            if (expected.isPresent() == false) {
                return nullValue();
            }
            if (expected.get().length == 1) {
                return equalTo(new BytesRef(expected.get()[0]));
            }
            return equalTo(Arrays.stream(expected.get()).map(BytesRef::new).toList());
        };
        return this;
    }

    public VaragsTestCaseBuilder expectFlattenedString(Function<Stream<String>, Optional<String>> expectedStr) {
        this.expectedStr = strings -> {
            if (Arrays.stream(strings).anyMatch(s -> s == null)) {
                return nullValue();
            }
            Optional<String> expected = expectedStr.apply(Arrays.stream(strings).flatMap(Arrays::stream));
            if (expected.isPresent() == false) {
                return nullValue();
            }
            return equalTo(new BytesRef(expected.get()));
        };
        return this;
    }

    public VaragsTestCaseBuilder expectLong(Function<Stream<long[]>, Optional<long[]>> expectedLong) {
        this.expectedLong = longs -> {
            if (Arrays.stream(longs).anyMatch(l -> l == null)) {
                return nullValue();
            }
            Optional<long[]> expected = expectedLong.apply(Arrays.stream(longs));
            if (expected.isPresent() == false) {
                return nullValue();
            }
            if (expected.get().length == 1) {
                return equalTo(expected.get()[0]);
            }
            return equalTo(Arrays.stream(expected.get()).mapToObj(Long::valueOf).toList());
        };
        return this;
    }

    public VaragsTestCaseBuilder expectFlattenedLong(Function<LongStream, OptionalLong> expectedLong) {
        this.expectedLong = longs -> {
            if (Arrays.stream(longs).anyMatch(l -> l == null)) {
                return nullValue();
            }
            OptionalLong expected = expectedLong.apply(Arrays.stream(longs).flatMapToLong(Arrays::stream));
            if (expected.isPresent() == false) {
                return nullValue();
            }
            return equalTo(expected.getAsLong());
        };
        return this;
    }

    public VaragsTestCaseBuilder expectInt(Function<Stream<int[]>, Optional<int[]>> expectedInt) {
        this.expectedInt = ints -> {
            Optional<int[]> expected = expectedInt.apply(Arrays.stream(ints));
            if (expected.isPresent() == false) {
                return nullValue();
            }
            if (expected.get().length == 1) {
                return equalTo(expected.get()[0]);
            }
            return equalTo(Arrays.stream(expected.get()).mapToObj(Integer::valueOf).toList());
        };
        return this;
    }

    public VaragsTestCaseBuilder expectFlattenedInt(Function<IntStream, OptionalInt> expectedInt) {
        this.expectedInt = ints -> {
            if (Arrays.stream(ints).anyMatch(i -> i == null)) {
                return nullValue();
            }
            OptionalInt expected = expectedInt.apply(Arrays.stream(ints).flatMapToInt(Arrays::stream));
            if (expected.isPresent() == false) {
                return nullValue();
            }
            return equalTo(expected.getAsInt());
        };
        return this;
    }

    public VaragsTestCaseBuilder expectBoolean(Function<Stream<boolean[]>, Optional<boolean[]>> expectedBoolean) {
        this.expectedBoolean = booleans -> {
            Optional<boolean[]> expected = expectedBoolean.apply(Arrays.stream(booleans));
            if (expected.isPresent() == false) {
                return nullValue();
            }
            if (expected.get().length == 1) {
                return equalTo(expected.get()[0]);
            }
            return equalTo(IntStream.range(0, expected.get().length).mapToObj(i -> expected.get()[i]).toList());
        };
        return this;
    }

    public VaragsTestCaseBuilder expectFlattenedBoolean(Function<Stream<Boolean>, Optional<Boolean>> expectedBoolean) {
        this.expectedBoolean = booleans -> {
            if (Arrays.stream(booleans).anyMatch(i -> i == null)) {
                return nullValue();
            }
            Optional<Boolean> expected = expectedBoolean.apply(
                Arrays.stream(booleans).flatMap(bs -> IntStream.range(0, bs.length).mapToObj(i -> bs[i]))
            );
            if (expected.isPresent() == false) {
                return nullValue();
            }
            return equalTo(expected.get());
        };
        return this;
    }

    public List<TestCaseSupplier> suppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // TODO more types
        if (expectedStr != null) {
            strings(suppliers);
        }
        if (expectedLong != null) {
            longs(suppliers);
        }
        if (expectedInt != null) {
            ints(suppliers);
        }
        if (expectedBoolean != null) {
            booleans(suppliers);
        }
        return suppliers;
    }

    private void strings(List<TestCaseSupplier> suppliers) {
        for (int count = 1; count < MAX_WIDTH; count++) {
            for (boolean multivalued : new boolean[] { false, true }) {
                int paramCount = count;
                suppliers.add(
                    new TestCaseSupplier(
                        testCaseName(paramCount, multivalued, DataType.KEYWORD),
                        dataTypes(paramCount, DataType.KEYWORD),
                        () -> stringCase(DataType.KEYWORD, paramCount, multivalued)
                    )
                );
                suppliers.add(
                    new TestCaseSupplier(
                        testCaseName(paramCount, multivalued, DataType.TEXT),
                        dataTypes(paramCount, DataType.TEXT),
                        () -> stringCase(DataType.TEXT, paramCount, multivalued)
                    )
                );
            }
        }
    }

    private TestCaseSupplier.TestCase stringCase(DataType dataType, int paramCount, boolean multivalued) {
        String[][] data = new String[paramCount][];
        List<TestCaseSupplier.TypedData> typedData = new ArrayList<>(paramCount);
        for (int p = 0; p < paramCount; p++) {
            if (multivalued) {
                data[p] = ESTestCase.randomList(1, 4, () -> ESTestCase.randomAlphaOfLength(5)).toArray(String[]::new);
                typedData.add(new TestCaseSupplier.TypedData(Arrays.stream(data[p]).map(BytesRef::new).toList(), dataType, "field" + p));
            } else {
                data[p] = new String[] { ESTestCase.randomAlphaOfLength(5) };
                typedData.add(new TestCaseSupplier.TypedData(new BytesRef(data[p][0]), dataType, "field" + p));
            }
        }
        return testCase(typedData, expectedEvaluatorPrefix.apply("BytesRef"), dataType, expectedStr.apply(data));
    }

    private void longs(List<TestCaseSupplier> suppliers) {
        for (int count = 1; count < MAX_WIDTH; count++) {
            for (boolean multivalued : new boolean[] { false, true }) {
                int paramCount = count;
                suppliers.add(
                    new TestCaseSupplier(
                        testCaseName(paramCount, multivalued, DataType.LONG),
                        dataTypes(paramCount, DataType.LONG),
                        () -> longCase(paramCount, multivalued)
                    )
                );
            }
        }
    }

    private TestCaseSupplier.TestCase longCase(int paramCount, boolean multivalued) {
        long[][] data = new long[paramCount][];
        List<TestCaseSupplier.TypedData> typedData = new ArrayList<>(paramCount);
        for (int p = 0; p < paramCount; p++) {
            if (multivalued) {
                List<Long> d = ESTestCase.randomList(1, 4, () -> ESTestCase.randomLong());
                data[p] = d.stream().mapToLong(Long::longValue).toArray();
                typedData.add(
                    new TestCaseSupplier.TypedData(Arrays.stream(data[p]).mapToObj(Long::valueOf).toList(), DataType.LONG, "field" + p)
                );
            } else {
                data[p] = new long[] { ESTestCase.randomLong() };
                typedData.add(new TestCaseSupplier.TypedData(data[p][0], DataType.LONG, "field" + p));
            }
        }
        return testCase(typedData, expectedEvaluatorPrefix.apply("Long"), DataType.LONG, expectedLong.apply(data));
    }

    private void ints(List<TestCaseSupplier> suppliers) {
        for (int count = 1; count < MAX_WIDTH; count++) {
            for (boolean multivalued : new boolean[] { false, true }) {
                int paramCount = count;
                suppliers.add(
                    new TestCaseSupplier(
                        testCaseName(paramCount, multivalued, DataType.INTEGER),
                        dataTypes(paramCount, DataType.INTEGER),
                        () -> intCase(paramCount, multivalued)
                    )
                );
            }
        }
    }

    private TestCaseSupplier.TestCase intCase(int paramCount, boolean multivalued) {
        int[][] data = new int[paramCount][];
        List<TestCaseSupplier.TypedData> typedData = new ArrayList<>(paramCount);
        for (int p = 0; p < paramCount; p++) {
            if (multivalued) {
                List<Integer> d = ESTestCase.randomList(1, 4, () -> ESTestCase.randomInt());
                data[p] = d.stream().mapToInt(Integer::intValue).toArray();
                typedData.add(new TestCaseSupplier.TypedData(d, DataType.INTEGER, "field" + p));
            } else {
                data[p] = new int[] { ESTestCase.randomInt() };
                typedData.add(new TestCaseSupplier.TypedData(data[p][0], DataType.INTEGER, "field" + p));
            }
        }
        return testCase(typedData, expectedEvaluatorPrefix.apply("Int"), DataType.INTEGER, expectedInt.apply(data));
    }

    private void booleans(List<TestCaseSupplier> suppliers) {
        for (int count = 1; count < MAX_WIDTH; count++) {
            for (boolean multivalued : new boolean[] { false, true }) {
                int paramCount = count;
                suppliers.add(
                    new TestCaseSupplier(
                        testCaseName(paramCount, multivalued, DataType.BOOLEAN),
                        dataTypes(paramCount, DataType.BOOLEAN),
                        () -> booleanCase(paramCount, multivalued)
                    )
                );
            }
        }
    }

    private TestCaseSupplier.TestCase booleanCase(int paramCount, boolean multivalued) {
        boolean[][] data = new boolean[paramCount][];
        List<TestCaseSupplier.TypedData> typedData = new ArrayList<>(paramCount);
        for (int p = 0; p < paramCount; p++) {
            if (multivalued) {
                int size = ESTestCase.between(1, 5);
                data[p] = new boolean[size];
                List<Boolean> paramData = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    data[p][i] = ESTestCase.randomBoolean();
                    paramData.add(data[p][i]);
                }
                typedData.add(new TestCaseSupplier.TypedData(paramData, DataType.BOOLEAN, "field" + p));
            } else {
                data[p] = new boolean[] { ESTestCase.randomBoolean() };
                typedData.add(new TestCaseSupplier.TypedData(data[p][0], DataType.BOOLEAN, "field" + p));
            }
        }
        return testCase(typedData, expectedEvaluatorPrefix.apply("Boolean"), DataType.BOOLEAN, expectedBoolean.apply(data));
    }

    private String testCaseName(int count, boolean multivalued, DataType type) {
        return "("
            + IntStream.range(0, count)
                .mapToObj(i -> "<" + (multivalued ? "mv_" : "") + type.typeName() + ">")
                .collect(Collectors.joining(", "))
            + ")";
    }

    protected TestCaseSupplier.TestCase testCase(
        List<TestCaseSupplier.TypedData> typedData,
        String expectedEvaluatorPrefix,
        DataType expectedType,
        Matcher<Object> expectedValue
    ) {
        return new TestCaseSupplier.TestCase(
            typedData,
            expectedToString(expectedEvaluatorPrefix, typedData.size()),
            expectedType,
            expectedValue
        );
    }

    private String expectedToString(String expectedEvaluatorPrefix, int attrs) {
        return expectedEvaluatorPrefix
            + "Evaluator[values=["
            + IntStream.range(0, attrs)
                .mapToObj(i -> expectedEvaluatorValueMap.apply("Attribute[channel=" + i + "]"))
                .collect(Collectors.joining(", "))
            + "]]";
    }

    private List<DataType> dataTypes(int paramCount, DataType dataType) {
        return IntStream.range(0, paramCount).mapToObj(i -> dataType).toList();
    }
}
