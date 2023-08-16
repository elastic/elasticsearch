/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
public class VaragsTestCases {
    public static List<AbstractFunctionTestCase.TestCaseSupplier> anyNullIsNull(
        Function<String, String> expectedEvaluatorPrefix,
        Function<Stream<String>, String> expectedStr,
        Function<LongStream, OptionalLong> expectedLong,
        Function<IntStream, OptionalInt> expectedInt
    ) {
        return new VaragsTestCases(expectedEvaluatorPrefix, strings -> {
            if (Arrays.stream(strings).anyMatch(s -> s == null)) {
                return nullValue();
            }
            return equalTo(new BytesRef(expectedStr.apply(Arrays.stream(strings))));
        }, longs -> {
            if (Arrays.stream(longs).anyMatch(l -> l == null)) {
                return nullValue();
            }
            return equalTo(expectedLong.apply(Arrays.stream(longs).mapToLong(Long::longValue)).getAsLong());
        }, ints -> {
            if (Arrays.stream(ints).anyMatch(i -> i == null)) {
                return nullValue();
            }
            return equalTo(expectedInt.apply(Arrays.stream(ints).mapToInt(Integer::intValue)).getAsInt());
        }).suppliers();
    }

    private static final int MAX_WIDTH = 10;

    private final List<AbstractFunctionTestCase.TestCaseSupplier> suppliers = new ArrayList<>();

    private final Function<String, String> expectedEvaluatorPrefix;
    private final Function<String[], Matcher<Object>> expectedStr;
    private final Function<Long[], Matcher<Object>> expectedLong;
    private final Function<Integer[], Matcher<Object>> expectedInt;

    public VaragsTestCases(
        Function<String, String> expectedEvaluatorPrefix,
        Function<String[], Matcher<Object>> expectedStr,
        Function<Long[], Matcher<Object>> expectedLong,
        Function<Integer[], Matcher<Object>> expectedInt
    ) {
        this.expectedEvaluatorPrefix = expectedEvaluatorPrefix;
        this.expectedStr = expectedStr;
        this.expectedLong = expectedLong;
        this.expectedInt = expectedInt;
    }

    public List<AbstractFunctionTestCase.TestCaseSupplier> suppliers() {
        // TODO more types
        strings();
        longs();
        ints();
        return suppliers;
    }

    private void strings() {
        IntStream.range(1, MAX_WIDTH)
            .mapToObj(i -> new AbstractFunctionTestCase.TestCaseSupplier(testCaseName(i, "kwd"), () -> strings(DataTypes.KEYWORD, i)))
            .forEach(suppliers::add);
        IntStream.range(1, MAX_WIDTH)
            .mapToObj(i -> new AbstractFunctionTestCase.TestCaseSupplier(testCaseName(i, "txt"), () -> strings(DataTypes.TEXT, i)))
            .forEach(suppliers::add);
    }

    private AbstractFunctionTestCase.TestCase strings(DataType dataType, int i) {
        String[] data = IntStream.range(0, i).mapToObj(v -> ESTestCase.randomAlphaOfLength(5)).toArray(String[]::new);
        int[] n = new int[1];
        List<AbstractFunctionTestCase.TypedData> typedData = Arrays.stream(data)
            .map(d -> new AbstractFunctionTestCase.TypedData(new BytesRef(d), dataType, "field" + n[0]++))
            .toList();
        return testCase(typedData, expectedEvaluatorPrefix.apply("BytesRef"), dataType, expectedStr.apply(data));
    }

    private void longs() {
        IntStream.range(1, MAX_WIDTH)
            .mapToObj(i -> new AbstractFunctionTestCase.TestCaseSupplier(testCaseName(i, "int"), () -> longs(i)))
            .forEach(suppliers::add);
    }

    private AbstractFunctionTestCase.TestCase longs(int i) {
        Long[] data = IntStream.range(0, i).mapToObj(v -> ESTestCase.randomLong()).toArray(Long[]::new);
        int[] n = new int[1];
        List<AbstractFunctionTestCase.TypedData> typedData = Arrays.stream(data)
            .map(d -> new AbstractFunctionTestCase.TypedData(d, DataTypes.LONG, "field" + n[0]++))
            .toList();
        return testCase(typedData, expectedEvaluatorPrefix.apply("Long"), DataTypes.LONG, expectedLong.apply(data));
    }

    private void ints() {
        IntStream.range(1, MAX_WIDTH)
            .mapToObj(i -> new AbstractFunctionTestCase.TestCaseSupplier(testCaseName(i, "int"), () -> ints(i)))
            .forEach(suppliers::add);
    }

    private AbstractFunctionTestCase.TestCase ints(int i) {
        Integer[] data = IntStream.range(0, i).mapToObj(v -> ESTestCase.randomInt()).toArray(Integer[]::new);
        int[] n = new int[1];
        List<AbstractFunctionTestCase.TypedData> typedData = Arrays.stream(data)
            .map(d -> new AbstractFunctionTestCase.TypedData(d, DataTypes.INTEGER, "field" + n[0]++))
            .toList();
        return testCase(typedData, expectedEvaluatorPrefix.apply("Int"), DataTypes.INTEGER, expectedInt.apply(data));
    }

    private String testCaseName(int count, String type) {
        return "(" + IntStream.range(0, count).mapToObj(i -> type).collect(Collectors.joining(", ")) + ")";
    }

    protected AbstractFunctionTestCase.TestCase testCase(
        List<AbstractFunctionTestCase.TypedData> typedData,
        String expectedEvaluatorPrefix,
        DataType expectedType,
        Matcher<Object> expectedValue
    ) {
        return new AbstractFunctionTestCase.TestCase(
            typedData,
            expectedToString(expectedEvaluatorPrefix, typedData.size()),
            expectedType,
            expectedValue
        );
    }

    private String expectedToString(String expectedEvaluatorPrefix, int attrs) {
        return expectedEvaluatorPrefix
            + "Evaluator[values=["
            + IntStream.range(0, attrs).mapToObj(i -> "Attribute[channel=" + i + "]").collect(Collectors.joining(", "))
            + "]]";
    }
}
