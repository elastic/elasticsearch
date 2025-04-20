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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;

import static org.hamcrest.Matchers.equalTo;

public class ReplaceTests extends AbstractScalarFunctionTestCase {
    public ReplaceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType strType : DataType.types()) {
            if (DataType.isString(strType) == false) {
                continue;
            }
            for (DataType oldStrType : DataType.types()) {
                if (DataType.isString(oldStrType) == false) {
                    continue;
                }
                for (DataType newStrType : DataType.types()) {
                    if (DataType.isString(newStrType) == false) {
                        continue;
                    }
                    suppliers.add(new TestCaseSupplier(List.of(strType, oldStrType, newStrType), () -> {
                        String str = randomAlphaOfLength(10);
                        String oldStr = str.substring(1, 2);
                        String newStr = randomAlphaOfLength(5);
                        return testCase(strType, oldStrType, newStrType, str, oldStr, newStr, str.replaceAll(oldStr, newStr));
                    }));
                }
            }
        }
        /*
        final String s = "a\ud83c\udf09tiger";
        assertThat(process(s, "a\ud83c\udf09t", "pp"), equalTo("ppiger"));
        assertThat(process(s, "\ud83c\udf09", "\ud83c\udf09\ud83c\udf09"), equalTo("a\ud83c\udf09\ud83c\udf09tiger"));

         */
        suppliers.add(fixedCase("replace one with two", "a tiger", "a", "pp", "pp tiger"));
        suppliers.add(fixedCase("replace many times", "a tiger is always a tiger", "a", "pp", "pp tiger is pplwppys pp tiger"));
        suppliers.add(fixedCase("replace no times", "a tiger", "ti ", "", "a tiger"));
        suppliers.add(fixedCase("replace with none", "a tiger", " ti", "", "ager"));
        suppliers.add(fixedCase("regex", "what a nice day", "\\s+", "-", "what-a-nice-day"));
        suppliers.add(
            fixedCase("more complex regex", "I love cats and cats are amazing.", "\\bcats\\b", "dogs", "I love dogs and dogs are amazing.")
        );
        suppliers.add(fixedCase("match unicode", "a\ud83c\udf09tiger", "a\ud83c\udf09t", "pp", "ppiger"));
        suppliers.add(
            fixedCase(
                "replace with unicode",
                "a\ud83c\udf09tiger",
                "\ud83c\udf09",
                "\ud83c\udf09\ud83c\udf09",
                "a\ud83c\udf09\ud83c\udf09tiger"
            )
        );

        suppliers.add(new TestCaseSupplier("syntax error", List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD), () -> {
            String text = randomAlphaOfLength(10);
            String invalidRegex = "[";
            String newStr = randomAlphaOfLength(5);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(invalidRegex), DataType.KEYWORD, "oldStr"),
                    new TestCaseSupplier.TypedData(new BytesRef(newStr), DataType.KEYWORD, "newStr")
                ),
                "ReplaceEvaluator[str=Attribute[channel=0], regex=Attribute[channel=1], newStr=Attribute[channel=2]]",
                DataType.KEYWORD,
                equalTo(null)
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning(
                    "Line 1:1: java.util.regex.PatternSyntaxException: Unclosed character class near index 0\n[\n^".replaceAll(
                        "\n",
                        System.lineSeparator()
                    )
                )
                .withFoldingException(
                    PatternSyntaxException.class,
                    "Unclosed character class near index 0\n[\n^".replaceAll("\n", System.lineSeparator())
                );
        }));
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, suppliers);
    }

    private static TestCaseSupplier fixedCase(String name, String str, String oldStr, String newStr, String result) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD),
            () -> testCase(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD, str, oldStr, newStr, result)
        );
    }

    private static TestCaseSupplier.TestCase testCase(
        DataType strType,
        DataType oldStrType,
        DataType newStrType,
        String str,
        String oldStr,
        String newStr,
        String result
    ) {
        return new TestCaseSupplier.TestCase(
            List.of(
                new TestCaseSupplier.TypedData(new BytesRef(str), strType, "str"),
                new TestCaseSupplier.TypedData(new BytesRef(oldStr), oldStrType, "oldStr"),
                new TestCaseSupplier.TypedData(new BytesRef(newStr), newStrType, "newStr")
            ),
            "ReplaceEvaluator[str=Attribute[channel=0], regex=Attribute[channel=1], newStr=Attribute[channel=2]]",
            DataType.KEYWORD,
            equalTo(new BytesRef(result))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Replace(source, args.get(0), args.get(1), args.get(2));
    }

    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        // TODO: This function doesn't serialize the Source, and must be fixed.
        return expression;
    }
}
