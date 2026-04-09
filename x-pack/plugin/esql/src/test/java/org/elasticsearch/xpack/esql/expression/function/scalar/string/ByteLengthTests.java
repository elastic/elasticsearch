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
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class ByteLengthTests extends AbstractScalarFunctionTestCase {
    public ByteLengthTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        UnaryTestCaseHelper base = unary().expectedOutputType(DataType.INTEGER).evaluatorToString("ByteLengthEvaluator[val=%0]");
        base.strings().expectedFromString(s -> new BytesRef(s).length).build(cases);
        base.strings("empty string", () -> "").expectedFromString(s -> 0).build(cases);
        base.strings("single ascii character", () -> "a").expectedFromString(s -> 1).build(cases);
        base.strings("ascii string", () -> "clump").expectedFromString(s -> 5).build(cases);
        base.strings("3 bytes, 1 code point", () -> "☕").expectedFromString(s -> 3).build(cases);
        base.strings("6 bytes, 2 code points", () -> "❗️").expectedFromString(s -> 6).build(cases);
        base.strings("100 random alpha", () -> randomAlphaOfLength(100)).expectedFromString(s -> 100).build(cases);
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ByteLength(source, args.get(0));
    }
}
