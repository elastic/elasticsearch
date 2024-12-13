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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
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
import static org.hamcrest.Matchers.startsWith;

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
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.security.NoSuchAlgorithmException: invalid MessageDigest not available")
                .withFoldingException(InvalidArgumentException.class, "invalid algorithm for []: invalid MessageDigest not available");
        }));
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases, (v, p) -> "string");
    }

    private static List<TestCaseSupplier> createTestCases(String algorithm) {
        return List.of(
            createTestCase(algorithm, DataType.KEYWORD, DataType.KEYWORD),
            createTestCase(algorithm, DataType.KEYWORD, DataType.TEXT),
            createTestCase(algorithm, DataType.TEXT, DataType.KEYWORD),
            createTestCase(algorithm, DataType.TEXT, DataType.TEXT),
            createLiteralTestCase(algorithm, DataType.KEYWORD, DataType.KEYWORD),
            createLiteralTestCase(algorithm, DataType.KEYWORD, DataType.TEXT),
            createLiteralTestCase(algorithm, DataType.TEXT, DataType.KEYWORD),
            createLiteralTestCase(algorithm, DataType.TEXT, DataType.KEYWORD)
        );
    }

    private static TestCaseSupplier createTestCase(String algorithm, DataType algorithmType, DataType inputType) {
        return new TestCaseSupplier(algorithm, List.of(algorithmType, inputType), () -> {
            var input = randomAlphaOfLength(10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(algorithm), algorithmType, "algorithm"),
                    new TestCaseSupplier.TypedData(new BytesRef(input), inputType, "input")
                ),
                "HashEvaluator[algorithm=Attribute[channel=0], input=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(hash(algorithm, input)))
            );
        });
    }

    private static TestCaseSupplier createLiteralTestCase(String algorithm, DataType algorithmType, DataType inputType) {
        return new TestCaseSupplier(algorithm, List.of(algorithmType, inputType), () -> {
            var input = randomAlphaOfLength(10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(algorithm), algorithmType, "algorithm").forceLiteral(),
                    new TestCaseSupplier.TypedData(new BytesRef(input), inputType, "input")
                ),
                "HashEvaluator[algorithm=" + algorithm + ", input=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(hash(algorithm, input)))
            );
        });
    }

    private static String hash(String algorithm, String input) {
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

    public void testInvalidAlgorithmLiteral() {
        Source source = new Source(0, 0, "hast(\"invalid\", input)");
        DriverContext driverContext = driverContext();
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> evaluator(
                new Hash(source, new Literal(source, new BytesRef("invalid"), DataType.KEYWORD), field("input", DataType.KEYWORD))
            ).get(driverContext)
        );
        assertThat(e.getMessage(), startsWith("invalid algorithm for [hast(\"invalid\", input)]: invalid MessageDigest not available"));
    }
}
