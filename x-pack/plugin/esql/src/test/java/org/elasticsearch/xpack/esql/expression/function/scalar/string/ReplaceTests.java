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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class ReplaceTests extends AbstractScalarFunctionTestCase {
    public ReplaceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(new TestCaseSupplier("basic", () -> {
            String text = randomAlphaOfLength(10);
            String oldStr = text.substring(1, 2);
            String newStr = randomAlphaOfLength(5);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(oldStr), DataTypes.KEYWORD, "oldStr"),
                    new TestCaseSupplier.TypedData(new BytesRef(newStr), DataTypes.KEYWORD, "newStr")
                ),
                "ReplaceEvaluator[str=Attribute[channel=0], regex=Attribute[channel=1], newStr=Attribute[channel=2]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(text.replaceAll(oldStr, newStr)))
            );
        }));
        // a syntactically wrong regex should yield null. And a warning header
        // but for now we are letting the exception pass through. See also https://github.com/elastic/elasticsearch/issues/100038
        // suppliers.add(new TestCaseSupplier("invalid_regex", () -> {
        // String text = randomAlphaOfLength(10);
        // String invalidRegex = "[";
        // String newStr = randomAlphaOfLength(5);
        // return new TestCaseSupplier.TestCase(
        // List.of(
        // new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
        // new TestCaseSupplier.TypedData(new BytesRef(invalidRegex), DataTypes.KEYWORD, "oldStr"),
        // new TestCaseSupplier.TypedData(new BytesRef(newStr), DataTypes.KEYWORD, "newStr")
        // ),
        // "ReplaceEvaluator[str=Attribute[channel=0], regex=Attribute[channel=1], newStr=Attribute[channel=2]]",
        // DataTypes.KEYWORD,
        // equalTo(null)
        // ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
        // .withWarning("java.util.regex.PatternSyntaxException: Unclosed character class near index 0\r\n[\r\n^");
        // }));
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    public Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        String oldStr = ((BytesRef) typedData.get(1).data()).utf8ToString();
        String newStr = ((BytesRef) typedData.get(2).data()).utf8ToString();
        return equalTo(new BytesRef(str.replaceAll(oldStr, newStr)));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()), required(strings()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Replace(source, args.get(0), args.get(1), args.get(2));
    }

    public void testReplaceString() {
        assertThat(process("a tiger", "a", "pp"), equalTo("pp tiger"));
        assertThat(process("a tiger is always a tiger", "a", "pp"), equalTo("pp tiger is pplwppys pp tiger"));
        assertThat(process("a tiger", "ti ", ""), equalTo("a tiger"));
        assertThat(process("a tiger", " ti", ""), equalTo("ager"));
    }

    public void testReplaceRegex() {
        assertThat(process("what a nice day", "\\s+", "-"), equalTo("what-a-nice-day"));
        assertThat(process("I love cats and cats are amazing.", "\\bcats\\b", "dogs"), equalTo("I love dogs and dogs are amazing."));
    }

    public void testUnicode() {
        final String s = "a\ud83c\udf09tiger";
        assertThat(process(s, "a\ud83c\udf09t", "pp"), equalTo("ppiger"));
        assertThat(process(s, "\ud83c\udf09", "\ud83c\udf09\ud83c\udf09"), equalTo("a\ud83c\udf09\ud83c\udf09tiger"));
    }

    private String process(String str, String oldStr, String newStr) {
        List<Object> list = Arrays.asList(new BytesRef(str), new BytesRef(oldStr), new BytesRef(newStr));
        Block.Ref result = evaluator(
            new Replace(
                Source.EMPTY,
                field("str", DataTypes.KEYWORD),
                field("oldStr", DataTypes.KEYWORD),
                field("newStr", DataTypes.KEYWORD)
            )
        ).get(driverContext()).eval(row(list));
        BytesRef resultValue = (BytesRef) toJavaObject(result.block(), 0);
        return resultValue == null ? null : resultValue.utf8ToString();
    }
}
