/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.hash;

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
        for (String alg : List.of("MD5", "SHA", "SHA-224", "SHA-256", "SHA-384", "SHA-512")) {
            cases.addAll(createTestCases(alg));
        }
        cases.add(new TestCaseSupplier("Invalid alg", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            var input = randomAlphaOfLength(10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("invalid"), DataType.KEYWORD, "alg"),
                    new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "input")
                ),
                "HashEvaluator[alg=Attribute[channel=0], input=Attribute[channel=1]]",
                DataType.KEYWORD,
                is(nullValue())
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.security.NoSuchAlgorithmException: invalid MessageDigest not available")
                .withFoldingException(InvalidArgumentException.class, "invalid alg for []: invalid MessageDigest not available");
        }));
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases, (v, p) -> "string");
    }

    private static List<TestCaseSupplier> createTestCases(String alg) {
        return List.of(
            createTestCase(alg, DataType.KEYWORD, DataType.KEYWORD),
            createTestCase(alg, DataType.KEYWORD, DataType.TEXT),
            createTestCase(alg, DataType.TEXT, DataType.KEYWORD),
            createTestCase(alg, DataType.TEXT, DataType.TEXT)
        );
    }

    private static TestCaseSupplier createTestCase(String alg, DataType algType, DataType inputType) {
        return new TestCaseSupplier(alg, List.of(algType, inputType), () -> {
            var input = randomAlphaOfLength(10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(alg), algType, "alg"),
                    new TestCaseSupplier.TypedData(new BytesRef(input), inputType, "input")
                ),
                "HashEvaluator[alg=Attribute[channel=0], input=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(hash(alg, input)))
            );
        });
    }

    private static String hash(String alg, String input) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance(alg).digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Unknown algorithm: " + alg);
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Hash(source, args.get(0), args.get(1));
    }

    public void testInvalidAlgLiteral() {
        Source source = new Source(0, 0, "hast(\"invalid\", input)");
        DriverContext driverContext = driverContext();
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> evaluator(
                new Hash(source, new Literal(source, new BytesRef("invalid"), DataType.KEYWORD), field("str", DataType.KEYWORD))
            ).get(driverContext)
        );
        assertThat(e.getMessage(), startsWith("invalid alg for [hast(\"invalid\", input)]: invalid MessageDigest not available"));
    }
}
