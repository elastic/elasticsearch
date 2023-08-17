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
import org.elasticsearch.xpack.esql.planner.EvalMapper;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    protected Coalesce build(Source source, List<Expression> args) {
        return new Coalesce(Source.EMPTY, args.stream().toList());
    }

    public void testCoalesceIsLazy() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        FieldAttribute evil = new FieldAttribute(Source.EMPTY, "evil", new EsField("evil", sub.get(0).dataType(), Map.of(), true));
        sub.add(evil);
        Coalesce exp = build(Source.EMPTY, sub);
        Layout.Builder builder = new Layout.Builder();
        buildLayout(builder, exp);
        Layout layout = builder.build();
        assertThat(toJavaObject(exp.toEvaluator(child -> {
            if (child == evil) {
                return () -> page -> { throw new AssertionError("shouldn't be called"); };
            }
            return EvalMapper.toEvaluator(child, layout);
        }).get().eval(row(testCase.getDataValues())), 0), testCase.getMatcher());
    }

    public void testCoalesceNullabilityIsUnknown() {
        assertThat(buildFieldExpression(testCase).nullable(), equalTo(Nullability.UNKNOWN));
    }

    public void testCoalesceKnownNullable() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        sub.add(between(0, sub.size()), new Literal(Source.EMPTY, null, sub.get(0).dataType()));
        Coalesce exp = build(Source.EMPTY, sub);
        // Still UNKNOWN - if it were TRUE then an optimizer would replace it with null
        assertThat(exp.nullable(), equalTo(Nullability.UNKNOWN));
    }

    public void testCoalesceNotNullable() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        sub.add(between(0, sub.size()), randomLiteral(sub.get(0).dataType()));
        Coalesce exp = build(Source.EMPTY, sub);
        // Known not to be nullable because it contains a non-null literal
        assertThat(exp.nullable(), equalTo(Nullability.FALSE));
    }
}
