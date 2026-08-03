/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@FunctionName("from_base64")
public class FromBase64Tests extends AbstractScalarFunctionTestCase {
    public FromBase64Tests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        String evaluator = "FromBase64Evaluator[field=Attribute[channel=0]]";

        // Valid base64 → well-formed UTF-8 keyword
        for (DataType dataType : DataType.stringTypes()) {
            suppliers.add(new TestCaseSupplier("empty " + dataType, List.of(dataType), () -> {
                BytesRef input = new BytesRef("");
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(input, dataType, "string")),
                    evaluator,
                    DataType.KEYWORD,
                    equalTo(new BytesRef(decode(input.utf8ToString())))
                );
            }));
            suppliers.add(new TestCaseSupplier("ascii " + dataType, List.of(dataType), () -> {
                String encoded = encode(randomAlphaOfLengthBetween(1, 54));
                BytesRef input = new BytesRef(encoded);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(input, dataType, "string")),
                    evaluator,
                    DataType.KEYWORD,
                    equalTo(new BytesRef(decode(encoded)))
                );
            }));
            suppliers.add(new TestCaseSupplier("unicode " + dataType, List.of(dataType), () -> {
                String encoded = encode(randomRealisticUnicodeOfCodepointLengthBetween(1, 20));
                BytesRef input = new BytesRef(encoded);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(input, dataType, "string")),
                    evaluator,
                    DataType.KEYWORD,
                    equalTo(new BytesRef(decode(encoded)))
                );
            }));
        }

        // Valid base64, but decoded bytes are not well-formed UTF-8 → null + warning
        addNullWarningCase(
            suppliers,
            "truncated lead byte",
            encode(new byte[] { 'a', (byte) 0xF0 }),
            evaluator,
            "decoded value is not valid UTF-8, which is not supported yet"
        );
        addNullWarningCase(
            suppliers,
            "lone lead byte",
            encode(new byte[] { (byte) 0xF0 }),
            evaluator,
            "decoded value is not valid UTF-8, which is not supported yet"
        );

        // Not valid base64 at all → null + warning (decode throws before UTF-8 check)
        addNullWarningCase(suppliers, "invalid alphabet", "not!!base64", evaluator, decodeErrorMessage("not!!base64"));
        addNullWarningCase(
            suppliers,
            "wrong 4-byte ending unit",
            "YfAAAAAAAAAAAAAAAAAAAAAA=",
            evaluator,
            decodeErrorMessage("YfAAAAAAAAAAAAAAAAAAAAAA=")
        );
        addNullWarningCase(
            suppliers,
            "insufficient bits in last unit",
            "YfAAAAAAAAAAAAAAAAAAAAAAA=",
            evaluator,
            decodeErrorMessage("YfAAAAAAAAAAAAAAAAAAAAAAA=")
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static void addNullWarningCase(
        List<TestCaseSupplier> suppliers,
        String name,
        String encoded,
        String evaluator,
        String exceptionMessage
    ) {
        for (DataType dataType : DataType.stringTypes()) {
            suppliers.add(new TestCaseSupplier(name + " " + dataType, List.of(dataType), () -> {
                BytesRef input = new BytesRef(encoded);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(input, dataType, "string")),
                    evaluator,
                    DataType.KEYWORD,
                    nullValue()
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.IllegalArgumentException: " + exceptionMessage);
            }));
        }
    }

    private static String encode(String plain) {
        return encode(plain.getBytes(StandardCharsets.UTF_8));
    }

    private static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static byte[] decode(String encoded) {
        return Base64.getDecoder().decode(encoded.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Message from {@link Base64.Decoder} for an input that must not decode successfully.
     * Uses the same {@code decode(byte[], byte[])} overload as {@link FromBase64#process}.
     */
    private static String decodeErrorMessage(String invalidBase64) {
        byte[] src = invalidBase64.getBytes(StandardCharsets.UTF_8);
        try {
            Base64.getDecoder().decode(src, new byte[src.length]);
            throw new AssertionError("expected invalid base64: " + invalidBase64);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FromBase64(source, args.get(0));
    }
}
