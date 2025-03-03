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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class HashTests extends AbstractScalarFunctionTestCase {

    public HashTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        for (String algorithm : List.of("MD5", "SHA", "SHA-224", "SHA-256", "SHA-384", "SHA-512")) {
            cases.addAll(createTestCases(algorithm));
        }
        cases.add(new TestCaseSupplier("Invalid algorithm", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            var input = randomAlphaOfLength(10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("invalid"), DataType.KEYWORD, "algorithm"),
                    new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "input")
                ),
                "HashEvaluator[algorithm=Attribute[channel=0], input=Attribute[channel=1]]",
                DataType.KEYWORD,
                is(nullValue())
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.security.NoSuchAlgorithmException: invalid MessageDigest not available")
                .withFoldingException(
                    InvalidArgumentException.class,
                    "invalid algorithm for [source]: invalid MessageDigest not available"
                );
        }));
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, cases);
    }

    private static List<TestCaseSupplier> createTestCases(String algorithm) {
        return List.of(
            createTestCase(algorithm, false, DataType.KEYWORD, DataType.KEYWORD),
            createTestCase(algorithm, false, DataType.KEYWORD, DataType.TEXT),
            createTestCase(algorithm, false, DataType.TEXT, DataType.KEYWORD),
            createTestCase(algorithm, false, DataType.TEXT, DataType.TEXT),
            createTestCase(algorithm, true, DataType.KEYWORD, DataType.KEYWORD),
            createTestCase(algorithm, true, DataType.KEYWORD, DataType.TEXT),
            createTestCase(algorithm, true, DataType.TEXT, DataType.KEYWORD),
            createTestCase(algorithm, true, DataType.TEXT, DataType.TEXT)
        );
    }

    private static TestCaseSupplier createTestCase(String algorithm, boolean forceLiteral, DataType algorithmType, DataType inputType) {
        return new TestCaseSupplier(algorithm, List.of(algorithmType, inputType), () -> {
            var input = randomFrom(TestCaseSupplier.stringCases(inputType)).get();
            return new TestCaseSupplier.TestCase(
                List.of(createTypedData(algorithm, forceLiteral, algorithmType, "algorithm"), input),
                forceLiteral
                    ? "HashConstantEvaluator[algorithm=" + algorithm + ", input=Attribute[channel=0]]"
                    : "HashEvaluator[algorithm=Attribute[channel=0], input=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(hash(algorithm, BytesRefs.toString(input.data()))))
            );
        });
    }

    static void addHashFunctionTestCases(List<TestCaseSupplier> cases, String algorithm) {
        TestCaseSupplier.forUnaryStrings(
            cases,
            "HashConstantEvaluator[algorithm=" + algorithm + ", input=Attribute[channel=0]]",
            DataType.KEYWORD,
            input -> new BytesRef(HashTests.hash(algorithm, BytesRefs.toString(input))),
            List.of()
        );
    }

    private static TestCaseSupplier.TypedData createTypedData(String value, boolean forceLiteral, DataType type, String name) {
        var data = new TestCaseSupplier.TypedData(new BytesRef(value), type, name);
        return forceLiteral ? data.forceLiteral() : data;
    }

    static String hash(String algorithm, String input) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance(algorithm).digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Unknown algorithm: " + algorithm);
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Hash(source, args.get(0), args.get(1));
    }
}
