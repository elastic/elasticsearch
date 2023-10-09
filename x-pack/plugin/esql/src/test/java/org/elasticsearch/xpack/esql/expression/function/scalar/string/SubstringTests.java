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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SubstringTests extends AbstractScalarFunctionTestCase {
    public SubstringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Substring basic test", () -> {
            int start = between(1, 8);
            int length = between(1, 10 - start);
            String text = randomAlphaOfLength(10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(length, DataTypes.INTEGER, "end")
                ),
                "SubstringEvaluator[str=Attribute[channel=0], start=Attribute[channel=1], length=Attribute[channel=2]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(text.substring(start - 1, start + length - 1)))
            );
        })));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    public Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        int start = (Integer) typedData.get(1).data();
        int end = (Integer) typedData.get(2).data();
        return equalTo(new BytesRef(str.substring(start - 1, start + end - 1)));
    }

    public void testNoLengthToString() {
        assertThat(
            evaluator(new Substring(Source.EMPTY, field("str", DataTypes.KEYWORD), field("start", DataTypes.INTEGER), null)).get(
                driverContext()
            ).toString(),
            equalTo("SubstringNoLengthEvaluator[str=Attribute[channel=0], start=Attribute[channel=1]]")
        );
    }

    @Override
    protected List<AbstractScalarFunctionTestCase.ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(integers()), optional(integers()));
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
                    field("str", DataTypes.KEYWORD),
                    new Literal(Source.EMPTY, start, DataTypes.INTEGER),
                    length == null ? null : new Literal(Source.EMPTY, length, DataTypes.INTEGER)
                )
            ).get(driverContext());
            Block.Ref ref = eval.eval(row(List.of(new BytesRef(str))))
        ) {
            return ref.block().isNull(0) ? null : ((BytesRef) toJavaObject(ref.block(), 0)).utf8ToString();
        }
    }

}
