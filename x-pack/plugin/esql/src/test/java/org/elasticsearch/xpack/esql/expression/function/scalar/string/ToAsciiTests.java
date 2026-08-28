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

import static org.hamcrest.Matchers.equalTo;

public class ToAsciiTests extends AbstractScalarFunctionTestCase {
    public ToAsciiTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        List<TestCaseSupplier> cases = new ArrayList<>();

        // Test with ASCII printable characters (should not be escaped) - KEYWORD
        cases.add(new TestCaseSupplier("ASCII printable characters keyword", List.of(DataType.KEYWORD), () -> {
            String input = randomAlphaOfLength(between(1, 100));
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(input))
            );
        }));

        // Test with ASCII printable characters (should not be escaped) - TEXT
        cases.add(new TestCaseSupplier("ASCII printable characters text", List.of(DataType.TEXT), () -> {
            String input = randomAlphaOfLength(between(1, 100));
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.TEXT, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(input))
            );
        }));

        // Test with Spanish accents
        cases.add(new TestCaseSupplier("Spanish accents", List.of(DataType.KEYWORD), () -> {
            String input = "Café naïve résumé";
            String expected = "Caf\\\\xe9 na\\\\xefve r\\\\xe9sum\\\\xe9";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        }));

        // Test with control characters
        cases.add(new TestCaseSupplier("control characters", List.of(DataType.KEYWORD), () -> {
            String input = "hello\nworld\r\t\"tab";
            String expected = "hello\\\\nworld\\\\r\\\\t\\\\\"tab";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        }));

        // Test with Chinese characters
        cases.add(new TestCaseSupplier("Chinese characters", List.of(DataType.KEYWORD), () -> {
            String input = "你好世界";
            String expected = "\\\\u4f60\\\\u597d\\\\u4e16\\\\u754c";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        }));

        // Test with Japanese characters
        cases.add(new TestCaseSupplier("Japanese characters", List.of(DataType.KEYWORD), () -> {
            String input = "こんにちは";
            String expected = "\\\\u3053\\\\u3093\\\\u306b\\\\u3061\\\\u306f";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        }));

        // Test with emojis (require 8-digit Unicode escape)
        cases.add(new TestCaseSupplier("emojis", List.of(DataType.KEYWORD), () -> {
            String input = "🚀🔥💧🪨";
            String expected = "\\\\U0001f680\\\\U0001f525\\\\U0001f4a7\\\\U0001faa8";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        }));

        // Test with Greek letters
        cases.add(new TestCaseSupplier("Greek letters", List.of(DataType.KEYWORD), () -> {
            String input = "αβγδε";
            String expected = "\\\\u03b1\\\\u03b2\\\\u03b3\\\\u03b4\\\\u03b5";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        }));

        // Test with mixed content
        cases.add(new TestCaseSupplier("mixed content", List.of(DataType.KEYWORD), () -> {
            String input = "Hello 世界! 🌍";
            String expected = "Hello \\\\u4e16\\\\u754c! \\\\U0001f30d";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        }));

        // Test with empty string
        cases.add(new TestCaseSupplier("empty string", List.of(DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(""), DataType.KEYWORD, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));

        // Test with TEXT type
        cases.add(new TestCaseSupplier("TEXT type", List.of(DataType.TEXT), () -> {
            String input = "Café";
            String expected = "Caf\\\\xe9";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(input), DataType.TEXT, "str")),
                "ToAsciiEvaluator[val=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToAscii(source, args.get(0));
    }
}
