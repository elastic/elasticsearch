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
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class ByteLengthTests extends AbstractScalarFunctionTestCase {
    public ByteLengthTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        cases.addAll(List.of(new TestCaseSupplier("byte length basic test", List.of(DataType.KEYWORD), () -> {
            var s = randomAlphaOfLength(between(0, 10000));
            return testCase(s, DataType.KEYWORD, s.length());
        })));
        cases.addAll(makeTestCases("empty string", () -> "", 0));
        cases.addAll(makeTestCases("single ascii character", () -> "a", 1));
        cases.addAll(makeTestCases("ascii string", () -> "clump", 5));
        cases.addAll(makeTestCases("3 bytes, 1 code point", () -> "☕", 3));
        cases.addAll(makeTestCases("6 bytes, 2 code points", () -> "❗️", 6));
        cases.addAll(makeTestCases("100 random alpha", () -> randomAlphaOfLength(100), 100));
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(ENTIRELY_NULL_PRESERVES_TYPE, cases);
    }

    private static List<TestCaseSupplier> makeTestCases(String title, Supplier<String> text, int expectedByteLength) {
        return Stream.of(DataType.KEYWORD, DataType.TEXT)
            .map(
                dataType -> new TestCaseSupplier(
                    title + " with " + dataType,
                    List.of(dataType),
                    () -> testCase(text.get(), dataType, expectedByteLength)
                )
            )
            .toList();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        assert args.size() == 1;
        return new ByteLength(source, args.get(0));
    }

    private static TestCaseSupplier.TestCase testCase(String s, DataType dataType, int expectedByteLength) {
        var bytesRef = new BytesRef(s);
        return new TestCaseSupplier.TestCase(
            List.of(new TestCaseSupplier.TypedData(bytesRef, dataType, "f")),
            "ByteLengthEvaluator[val=Attribute[channel=0]]",
            DataType.INTEGER,
            equalTo(expectedByteLength)
        );
    }

    private static final boolean ENTIRELY_NULL_PRESERVES_TYPE = true;
}
