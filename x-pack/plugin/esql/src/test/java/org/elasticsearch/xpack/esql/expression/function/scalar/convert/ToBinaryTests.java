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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

public class ToBinaryTests extends AbstractScalarFunctionTestCase {
    public ToBinaryTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        String stringEvaluator = "ToBinaryFromStringEvaluator[asString=" + read + "]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // BINARY -> BINARY is identity.
        TestCaseSupplier.forUnaryBinary(suppliers, read, DataType.BINARY, b -> b, List.of());

        // KEYWORD / TEXT -> BINARY: the value is treated as base64. Random strings are extremely
        // unlikely to be valid base64, so most random strings will surface as null with a warning.
        // We rely on the explicit base64 cases below to verify happy-path behavior.
        for (DataType inputType : DataType.stringTypes()) {
            TestCaseSupplier.unary(
                suppliers,
                stringEvaluator,
                base64StringCases(inputType),
                DataType.BINARY,
                v -> new BytesRef(Base64.getDecoder().decode(((BytesRef) v).utf8ToString())),
                List.of()
            );
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier.TypedDataSupplier> base64StringCases(DataType type) {
        return List.of(
            new TestCaseSupplier.TypedDataSupplier("<base64 empty>", () -> new BytesRef(""), type),
            new TestCaseSupplier.TypedDataSupplier("<base64 short>", () -> new BytesRef("AQID"), type),
            new TestCaseSupplier.TypedDataSupplier("<base64 padded>", () -> new BytesRef("CQ=="), type),
            new TestCaseSupplier.TypedDataSupplier(
                "<base64 random>",
                () -> new BytesRef(Base64.getEncoder().encodeToString(randomByteArrayOfLength(between(0, 64)))),
                type
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToBinary(source, args.get(0));
    }
}
