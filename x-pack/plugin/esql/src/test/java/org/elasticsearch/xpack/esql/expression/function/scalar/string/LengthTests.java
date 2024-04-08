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
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class LengthTests extends AbstractScalarFunctionTestCase {
    public LengthTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        cases.addAll(List.of(new TestCaseSupplier("length basic test", () -> {
            BytesRef value = new BytesRef(randomAlphaOfLength(between(0, 10000)));
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(value, DataTypes.KEYWORD, "f")),
                "LengthEvaluator[val=Attribute[channel=0]]",
                DataTypes.INTEGER,
                equalTo(UnicodeUtil.codePointCount(value))
            );
        })));
        cases.addAll(makeTestCases("empty string", () -> "", 0));
        cases.addAll(makeTestCases("single ascii character", () -> "a", 1));
        cases.addAll(makeTestCases("ascii string", () -> "clump", 5));
        cases.addAll(makeTestCases("3 bytes, 1 code point", () -> "☕", 1));
        cases.addAll(makeTestCases("6 bytes, 2 code points", () -> "❗️", 2));
        cases.addAll(makeTestCases("100 random alpha", () -> randomAlphaOfLength(100), 100));
        cases.addAll(makeTestCases("100 random code points", () -> randomUnicodeOfCodepointLength(100), 100));
        return parameterSuppliersFromTypedData(cases);
    }

    private static List<TestCaseSupplier> makeTestCases(String title, Supplier<String> text, int expectedLength) {
        return List.of(
            new TestCaseSupplier(
                title + " with keyword",
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef(text.get()), DataTypes.KEYWORD, "f")),
                    "LengthEvaluator[val=Attribute[channel=0]]",
                    DataTypes.INTEGER,
                    equalTo(expectedLength)
                )
            ),
            new TestCaseSupplier(
                title + " with text",
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef(text.get()), DataTypes.TEXT, "f")),
                    "LengthEvaluator[val=Attribute[channel=0]]",
                    DataTypes.INTEGER,
                    equalTo(expectedLength)
                )
            )
        );
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.INTEGER;
    }

    private Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        return equalTo(UnicodeUtil.codePointCount((BytesRef) typedData.get(0).data()));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Length(source, args.get(0));
    }

}
