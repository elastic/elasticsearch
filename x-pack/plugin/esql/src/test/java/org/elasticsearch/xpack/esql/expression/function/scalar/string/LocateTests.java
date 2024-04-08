/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link Locate} function.
 */
public class LocateTests extends AbstractFunctionTestCase {
    public LocateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(
            supplier(
                "keywords",
                DataTypes.KEYWORD,
                DataTypes.KEYWORD,
                () -> randomRealisticUnicodeOfCodepointLength(10),
                () -> randomRealisticUnicodeOfCodepointLength(2),
                () -> 0
            )
        );
        suppliers.add(
            supplier(
                "mixed keyword, text",
                DataTypes.KEYWORD,
                DataTypes.TEXT,
                () -> randomRealisticUnicodeOfCodepointLength(10),
                () -> randomRealisticUnicodeOfCodepointLength(2),
                () -> 0
            )
        );
        suppliers.add(
            supplier(
                "texts",
                DataTypes.TEXT,
                DataTypes.TEXT,
                () -> randomRealisticUnicodeOfCodepointLength(10),
                () -> randomRealisticUnicodeOfCodepointLength(2),
                () -> 0
            )
        );
        suppliers.add(
            supplier(
                "mixed text, keyword",
                DataTypes.TEXT,
                DataTypes.KEYWORD,
                () -> randomRealisticUnicodeOfCodepointLength(10),
                () -> randomRealisticUnicodeOfCodepointLength(2),
                () -> 0
            )
        );
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    public void testToString() {
        assertThat(
            evaluator(
                new Locate(
                    Source.EMPTY,
                    field("str", DataTypes.KEYWORD),
                    field("substr", DataTypes.KEYWORD),
                    field("start", DataTypes.INTEGER)
                )
            ).get(driverContext()).toString(),
            equalTo("LocateEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1], start=Attribute[channel=2]]")
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Locate(source, args.get(0), args.get(1), args.size() < 3 ? null : args.get(2));
    }

    public void testPrefixString() {
        assertThat(process("a tiger", "a t", 0), equalTo(1));
        assertThat(process("a tiger", "a", 0), equalTo(1));
        assertThat(process("界世", "界", 0), equalTo(1));
    }

    public void testSuffixString() {
        assertThat(process("a tiger", "er", 0), equalTo(6));
        assertThat(process("a tiger", "r", 0), equalTo(7));
        assertThat(process("世界", "界", 0), equalTo(2));
    }

    public void testMidString() {
        assertThat(process("a tiger", "ti", 0), equalTo(3));
        assertThat(process("a tiger", "ige", 0), equalTo(4));
        assertThat(process("世界世", "界", 0), equalTo(2));
    }

    public void testOutOfRange() {
        assertThat(process("a tiger", "tigers", 0), equalTo(0));
        assertThat(process("a tiger", "ipa", 0), equalTo(0));
        assertThat(process("世界世", "\uD83C\uDF0D", 0), equalTo(0));
    }

    public void testExactString() {
        assertThat(process("a tiger", "a tiger", 0), equalTo(1));
        assertThat(process("tigers", "tigers", 0), equalTo(1));
        assertThat(process("界世", "界世", 0), equalTo(1));
    }

    public void testSupplementaryCharacter() {
        // some assertions about the supplementary (4-byte) character we'll use for testing
        assert "𠜎".length() == 2;
        assert "𠜎".codePointCount(0, 2) == 1;
        assert "𠜎".getBytes(UTF_8).length == 4;

        assertThat(process("a ti𠜎er", "𠜎er", 0), equalTo(5));
        assertThat(process("a ti𠜎er", "i𠜎e", 0), equalTo(4));
        assertThat(process("a ti𠜎er", "ti𠜎", 0), equalTo(3));
        assertThat(process("a ti𠜎er", "er", 0), equalTo(6));
        assertThat(process("a ti𠜎er", "r", 0), equalTo(7));

        assertThat(process("𠜎a ti𠜎er", "𠜎er", 0), equalTo(6));
        assertThat(process("𠜎a ti𠜎er", "i𠜎e", 0), equalTo(5));
        assertThat(process("𠜎a ti𠜎er", "ti𠜎", 0), equalTo(4));
        assertThat(process("𠜎a ti𠜎er", "er", 0), equalTo(7));
        assertThat(process("𠜎a ti𠜎er", "r", 0), equalTo(8));

        // exact
        assertThat(process("a ti𠜎er", "a ti𠜎er", 0), equalTo(1));
        assertThat(process("𠜎𠜎𠜎abc", "𠜎𠜎𠜎abc", 0), equalTo(1));
        assertThat(process(" 𠜎𠜎𠜎abc", " 𠜎𠜎𠜎abc", 0), equalTo(1));
        assertThat(process("𠜎𠜎𠜎 abc ", "𠜎𠜎𠜎 abc ", 0), equalTo(1));

        // prefix
        assertThat(process("𠜎abc", "𠜎", 0), equalTo(1));
        assertThat(process("𠜎 abc", "𠜎 ", 0), equalTo(1));
        assertThat(process("𠜎𠜎𠜎abc", "𠜎𠜎𠜎", 0), equalTo(1));
        assertThat(process("𠜎𠜎𠜎 abc", "𠜎𠜎𠜎 ", 0), equalTo(1));
        assertThat(process(" 𠜎𠜎𠜎 abc", " 𠜎𠜎𠜎 ", 0), equalTo(1));
        assertThat(process("𠜎 𠜎 𠜎 abc", "𠜎 𠜎 𠜎 ", 0), equalTo(1));

        // suffix
        assertThat(process("abc𠜎", "𠜎", 0), equalTo(4));
        assertThat(process("abc 𠜎", " 𠜎", 0), equalTo(4));
        assertThat(process("abc𠜎𠜎𠜎", "𠜎𠜎𠜎", 0), equalTo(4));
        assertThat(process("abc 𠜎𠜎𠜎", " 𠜎𠜎𠜎", 0), equalTo(4));
        assertThat(process("abc𠜎𠜎𠜎 ", "𠜎𠜎𠜎 ", 0), equalTo(4));

        // out of range
        assertThat(process("𠜎a ti𠜎er", "𠜎a ti𠜎ers", 0), equalTo(0));
        assertThat(process("a ti𠜎er", "aa ti𠜎er", 0), equalTo(0));
        assertThat(process("abc𠜎𠜎", "𠜎𠜎𠜎", 0), equalTo(0));

        assert "🐱".length() == 2 && "🐶".length() == 2;
        assert "🐱".codePointCount(0, 2) == 1 && "🐶".codePointCount(0, 2) == 1;
        assert "🐱".getBytes(UTF_8).length == 4 && "🐶".getBytes(UTF_8).length == 4;
        assertThat(process("🐱Meow!🐶Woof!", "🐱Meow!🐶Woof!", 0), equalTo(1));
        assertThat(process("🐱Meow!🐶Woof!", "Meow!🐶Woof!", 0), equalTo(2));
        assertThat(process("🐱Meow!🐶Woof!", "eow!🐶Woof!", 0), equalTo(3));
    }

    private Integer process(String str, String substr, Integer start) {
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new Locate(
                    Source.EMPTY,
                    field("str", DataTypes.KEYWORD),
                    field("substr", DataTypes.KEYWORD),
                    new Literal(Source.EMPTY, start, DataTypes.INTEGER)
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(str), new BytesRef(substr))))
        ) {
            return block.isNull(0) ? Integer.valueOf(0) : ((Integer) toJavaObject(block, 0));
        }
    }

    private static TestCaseSupplier supplier(
        String name,
        DataType firstType,
        DataType secondType,
        Supplier<String> strValueSupplier,
        Supplier<String> substrValueSupplier,
        Supplier<Integer> startSupplier
    ) {
        return new TestCaseSupplier(name, List.of(firstType, secondType), () -> {
            List<TestCaseSupplier.TypedData> values = new ArrayList<>();
            String expectedToString = "LocateEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1], start=Attribute[channel=2]]";

            String value = strValueSupplier.get();
            values.add(new TestCaseSupplier.TypedData(new BytesRef(value), firstType, "0"));

            String substrValue = substrValueSupplier.get();
            values.add(new TestCaseSupplier.TypedData(new BytesRef(substrValue), secondType, "1"));

            Integer startValue = startSupplier.get();
            values.add(new TestCaseSupplier.TypedData(startValue, DataTypes.INTEGER, "2"));

            int expectedValue = 1 + value.indexOf(substrValue);
            return new TestCaseSupplier.TestCase(values, expectedToString, DataTypes.INTEGER, equalTo(expectedValue));
        });
    }
}
