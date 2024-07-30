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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SubstringTests extends AbstractScalarFunctionTestCase {
    public SubstringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            List.of(new TestCaseSupplier("Substring basic test", List.of(DataType.KEYWORD, DataType.INTEGER, DataType.INTEGER), () -> {
                int start = between(1, 8);
                int length = between(1, 10 - start);
                String text = randomAlphaOfLength(10);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                        new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                        new TestCaseSupplier.TypedData(length, DataType.INTEGER, "end")
                    ),
                    "SubstringEvaluator[str=Attribute[channel=0], start=Attribute[channel=1], length=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(text.substring(start - 1, start + length - 1)))
                );
            }),
                new TestCaseSupplier(
                    "Substring basic test with text input",
                    List.of(DataType.TEXT, DataType.INTEGER, DataType.INTEGER),
                    () -> {
                        int start = between(1, 8);
                        int length = between(1, 10 - start);
                        String text = randomAlphaOfLength(10);
                        return new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef(text), DataType.TEXT, "str"),
                                new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                                new TestCaseSupplier.TypedData(length, DataType.INTEGER, "end")
                            ),
                            "SubstringEvaluator[str=Attribute[channel=0], start=Attribute[channel=1], length=Attribute[channel=2]]",
                            DataType.KEYWORD,
                            equalTo(new BytesRef(text.substring(start - 1, start + length - 1)))
                        );
                    }
                ),
                new TestCaseSupplier("Substring empty string", List.of(DataType.TEXT, DataType.INTEGER, DataType.INTEGER), () -> {
                    int start = between(1, 8);
                    int length = between(1, 10 - start);
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef(""), DataType.TEXT, "str"),
                            new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                            new TestCaseSupplier.TypedData(length, DataType.INTEGER, "end")
                        ),
                        "SubstringEvaluator[str=Attribute[channel=0], start=Attribute[channel=1], length=Attribute[channel=2]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef(""))
                    );
                })
            ),
            (v, p) -> switch (p) {
                case 0 -> "string";
                case 1, 2 -> "integer";
                default -> "";
            }
        );
    }

    public Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        int start = (Integer) typedData.get(1).data();
        int end = (Integer) typedData.get(2).data();
        return equalTo(new BytesRef(str.substring(start - 1, start + end - 1)));
    }

    public void testNoLengthToString() {
        assertThat(
            evaluator(new Substring(Source.EMPTY, field("str", DataType.KEYWORD), field("start", DataType.INTEGER), null)).get(
                driverContext()
            ).toString(),
            equalTo("SubstringNoLengthEvaluator[str=Attribute[channel=0], start=Attribute[channel=1]]")
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Substring(source, args.get(0), args.get(1), args.size() < 3 ? null : args.get(2));
    }

    public void testWholeString() {
        assertThat(process("a tiger", 0, null), equalTo("a tiger"));
        assertThat(process("a tiger", 1, null), equalTo("a tiger"));
    }

    public void testPositiveStartNoLength() {
        assertThat(process("a tiger", 3, null), equalTo("tiger"));
    }

    public void testNegativeStartNoLength() {
        assertThat(process("a tiger", -3, null), equalTo("ger"));
    }

    public void testPositiveStartMassiveLength() {
        assertThat(process("a tiger", 3, 1000), equalTo("tiger"));
    }

    public void testNegativeStartMassiveLength() {
        assertThat(process("a tiger", -3, 1000), equalTo("ger"));
    }

    public void testMassiveNegativeStartNoLength() {
        assertThat(process("a tiger", -300, null), equalTo("a tiger"));
    }

    public void testMassiveNegativeStartSmallLength() {
        assertThat(process("a tiger", -300, 1), equalTo("a"));
    }

    public void testPositiveStartReasonableLength() {
        assertThat(process("a tiger", 1, 3), equalTo("a t"));
    }

    public void testUnicode() {
        final String s = "a\ud83c\udf09tiger";
        assert s.length() == 8 && s.codePointCount(0, s.length()) == 7;
        assertThat(process(s, 3, 1000), equalTo("tiger"));
        assertThat(process(s, -6, 1000), equalTo("\ud83c\udf09tiger"));
        assert "🐱".length() == 2 && "🐶".length() == 2;
        assert "🐱".codePointCount(0, 2) == 1 && "🐶".codePointCount(0, 2) == 1;
        assert "🐱".getBytes(UTF_8).length == 4 && "🐶".getBytes(UTF_8).length == 4;

        for (Integer len : new Integer[] { null, 100, 100000 }) {
            assertThat(process(s, 3, len), equalTo("tiger"));
            assertThat(process(s, -6, len), equalTo("\ud83c\udf09tiger"));

            assertThat(process("🐱Meow!🐶Woof!", 0, len), equalTo("🐱Meow!🐶Woof!"));
            assertThat(process("🐱Meow!🐶Woof!", 1, len), equalTo("🐱Meow!🐶Woof!"));
            assertThat(process("🐱Meow!🐶Woof!", 2, len), equalTo("Meow!🐶Woof!"));
            assertThat(process("🐱Meow!🐶Woof!", 3, len), equalTo("eow!🐶Woof!"));
        }
    }

    public void testNegativeLength() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process("a tiger", 1, -1));
        assertThat(ex.getMessage(), containsString("Length parameter cannot be negative, found [-1]"));
    }

    private String process(String str, int start, Integer length) {
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new Substring(
                    Source.EMPTY,
                    field("str", DataType.KEYWORD),
                    new Literal(Source.EMPTY, start, DataType.INTEGER),
                    length == null ? null : new Literal(Source.EMPTY, length, DataType.INTEGER)
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(str))))
        ) {
            return block.isNull(0) ? null : ((BytesRef) toJavaObject(block, 0)).utf8ToString();
        }
    }

}
