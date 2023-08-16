/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nulls;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class CoalesceTests extends AbstractFunctionTestCase {
    private static final int MAX_WIDTH = 10;

    public CoalesceTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Generate the test cases for this test. The tests don't actually include
     * any nulls, but we insert those nulls in {@link #testSimpleWithNulls()}.
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        kwds(suppliers);
        longs(suppliers);
        ints(suppliers);
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void kwds(List<TestCaseSupplier> suppliers) {
        for (int i = 1; i < MAX_WIDTH; i++) {
            suppliers.add(kwds(IntStream.range(0, i).mapToObj(v -> "v" + v).toArray(String[]::new)));
        }
    }

    private static TestCaseSupplier kwds(String... data) {
        int[] i = new int[1];
        List<TypedData> typedData = Arrays.stream(data)
            .map(d -> new TypedData(new BytesRef(d), DataTypes.KEYWORD, "field" + i[0]++))
            .toList();
        return new TestCaseSupplier(testCaseName(Arrays.stream(data)), () -> testCase(typedData, DataTypes.KEYWORD, ElementType.BYTES_REF));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        for (int i = 1; i < MAX_WIDTH; i++) {
            suppliers.add(longs(IntStream.range(0, i).mapToObj(v -> Long.valueOf(v)).toArray(Long[]::new)));
        }
    }

    private static TestCaseSupplier longs(Long... data) {
        int[] i = new int[1];
        List<TypedData> typedData = Arrays.stream(data).map(d -> new TypedData(d, DataTypes.LONG, "field" + i[0]++)).toList();
        return new TestCaseSupplier(testCaseName(Arrays.stream(data)), () -> testCase(typedData, DataTypes.LONG, ElementType.LONG));
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        for (int i = 1; i < MAX_WIDTH; i++) {
            suppliers.add(ints(IntStream.range(0, i).mapToObj(v -> Integer.valueOf(v)).toArray(Integer[]::new)));
        }
    }

    private static TestCaseSupplier ints(Integer... data) {
        int[] i = new int[1];
        List<TypedData> typedData = Arrays.stream(data).map(d -> new TypedData(d, DataTypes.INTEGER, "field" + i[0]++)).toList();
        return new TestCaseSupplier(testCaseName(Arrays.stream(data)), () -> testCase(typedData, DataTypes.INTEGER, ElementType.INT));
    }

    private static String testCaseName(Stream<Object> data) {
        return "(" + data.map(Objects::toString).collect(Collectors.joining(", ")) + ")";
    }

    private static TestCase testCase(List<TypedData> typedData, DataType expectedType, ElementType expectedElementType) {
        return new TestCase(
            typedData,
            expectedToString(expectedElementType, typedData.size()),
            expectedType,
            equalTo(typedData.stream().map(d -> d.data()).filter(d -> d != null).findFirst().orElse(null))
        );
    }

    private static String expectedToString(ElementType resultType, int attrs) {
        return "CoalesceEvaluator[resultType="
            + resultType
            + ", evaluators=["
            + IntStream.range(0, attrs).mapToObj(i -> "Attribute[channel=" + i + "]").collect(Collectors.joining(", "))
            + "]]";
    }

    @Override
    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        for (int i = 0; i < data.size(); i++) {
            if (nullBlock == i) {
                continue;
            }
            Object v = data.get(i);
            if (v == null) {
                continue;
            }
            assertThat(toJavaObject(value, 0), equalTo(v));
            return;
        }
        assertThat(value.isNull(0), equalTo(true));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Coalesce(Source.EMPTY, args.stream().toList());
    }
}
