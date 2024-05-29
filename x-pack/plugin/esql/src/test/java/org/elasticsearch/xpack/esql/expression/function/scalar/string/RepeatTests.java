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
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RepeatTests extends AbstractFunctionTestCase {
    public RepeatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(
                anyNullIsNull(true, List.of(new TestCaseSupplier("Repeat basic test", List.of(DataTypes.KEYWORD, DataTypes.INTEGER), () -> {
                    int number = between(0, 10);
                    String text = randomAlphaOfLength(10);
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
                            new TestCaseSupplier.TypedData(number, DataTypes.INTEGER, "number")
                        ),
                        "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                        DataTypes.KEYWORD,
                        equalTo(new BytesRef(text.repeat(number)))
                    );
                }), new TestCaseSupplier("Substring basic test with text input", List.of(DataTypes.TEXT, DataTypes.INTEGER), () -> {
                    int number = between(0, 10);
                    String text = randomAlphaOfLength(10);
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.TEXT, "str"),
                            new TestCaseSupplier.TypedData(number, DataTypes.INTEGER, "number")
                        ),
                        "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                        DataTypes.KEYWORD,
                        equalTo(new BytesRef(text.repeat(number)))
                    );
                })))
            )
        );
    }

    public Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        int number = (Integer) typedData.get(1).data();
        return equalTo(new BytesRef(str.repeat(number)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Repeat(source, args.get(0), args.get(1));
    }

    public void testBasic() {
        assertThat(process("moose ", 3), equalTo("moose moose moose "));
    }

    public void testZeroTimes() {
        assertThat(process("moose", 0), equalTo(""));
    }

    public void testLargeString() {
        assertThat(process("moose", 1000), equalTo("moose".repeat(1000)));
    }

    public void testUnicode() {
        final String s = "a\ud83c\udf09tiger";
        assertThat(process(s, 3), equalTo(s + s + s));
    }

    public void testNegativeLength() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process("goose", -1));
        assertThat(ex.getMessage(), containsString("Number parameter cannot be negative, found [-1]"));
    }

    private String process(String str, int number) {
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new Repeat(Source.EMPTY, field("str", DataTypes.KEYWORD), new Literal(Source.EMPTY, number, DataTypes.INTEGER))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(str))))
        ) {
            return block.isNull(0) ? null : ((BytesRef) toJavaObject(block, 0)).utf8ToString();
        }
    }
}
