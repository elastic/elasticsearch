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
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class TrimTests extends AbstractScalarFunctionTestCase {
    public TrimTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Trim basic test", () -> {
            BytesRef sampleData = addRandomLeadingOrTrailingWhitespaces(randomUnicodeOfLength(8));
            return new TestCase(
                Source.EMPTY,
                List.of(new TypedData(sampleData, randomFrom(strings()), "str")),
                "TrimEvaluator[val=Attribute[channel=0]]",
                equalTo(new BytesRef(sampleData.utf8ToString().trim()))
            );
        })));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Trim(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    public void testTrim() {
        for (int i = 0; i < 64; i++) {
            String expected = randomUnicodeOfLength(8).trim();
            BytesRef result = Trim.process(addRandomLeadingOrTrailingWhitespaces(expected));
            assertThat(result.utf8ToString(), equalTo(expected));
        }
    }

    static BytesRef addRandomLeadingOrTrailingWhitespaces(String expected) {
        StringBuilder builder = new StringBuilder();
        if (randomBoolean()) {
            builder.append(randomWhiteSpace());
            builder.append(expected);
            if (randomBoolean()) {
                builder.append(randomWhiteSpace());
            }
        } else {
            builder.append(expected);
            builder.append(randomWhiteSpace());
        }
        return new BytesRef(builder.toString());
    }

    private static char[] randomWhiteSpace() {
        char[] randomWhitespace = new char[randomIntBetween(1, 8)];
        Arrays.fill(randomWhitespace, (char) randomIntBetween(0, 0x20));
        return randomWhitespace;
    }

}
