/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractTrimTests extends AbstractScalarFunctionTestCase {
    static Iterable<Object[]> parameters(String name, boolean trimLeading, boolean trimTrailing) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType type : strings()) {
            suppliers.add(new TestCaseSupplier("no whitespace/" + type, () -> {
                String text = randomAlphaOfLength(8);
                return testCase(name, type, text, text);
            }));

            for (Map.Entry<String, char[]> whitespaces : List.of(
                Map.entry("spaces", new char[] { ' ' }),
                Map.entry("tabs", new char[] { '\t' }),
                Map.entry("newlines", new char[] { '\n' }),
                Map.entry("line tabulation", new char[] { '\u000B' }),
                Map.entry("form feed", new char[] { '\f' }),
                Map.entry("carriage return", new char[] { '\r' }),
                Map.entry("file separator", new char[] { '\u001C' }),
                Map.entry("group separator", new char[] { '\u001D' }),
                Map.entry("information separator two", new char[] { '\u001E' }),
                Map.entry("information separator one", new char[] { '\u001F' }),
                Map.entry("whitespace", new char[] { ' ', '\t', '\n', '\u000B', '\f', '\r', '\u001C', '\u001D', '\u001E', '\u001F' })
            )) {
                suppliers.add(new TestCaseSupplier(type + "/leading " + whitespaces.getKey(), () -> {
                    String text = randomAlphaOfLength(8);
                    String withWhitespace = randomWhiteSpace(whitespaces.getValue()) + text;
                    return testCase(name, type, withWhitespace, trimLeading ? text : withWhitespace);
                }));
                suppliers.add(new TestCaseSupplier(type + "/trailing " + whitespaces.getKey(), () -> {
                    String text = randomAlphaOfLength(8);
                    String withWhitespace = text + randomWhiteSpace(whitespaces.getValue());
                    return testCase(name, type, withWhitespace, trimTrailing ? text : withWhitespace);
                }));
                suppliers.add(new TestCaseSupplier(type + "/leading and trailing " + whitespaces.getKey(), () -> {
                    String text = randomAlphaOfLength(8);
                    String leadingWhitespace = randomWhiteSpace(whitespaces.getValue());
                    String trailingWhitespace = randomWhiteSpace(whitespaces.getValue());
                    return testCase(
                        name,
                        type,
                        leadingWhitespace + text + trailingWhitespace,
                        (trimLeading ? "" : leadingWhitespace) + text + (trimTrailing ? "" : trailingWhitespace)
                    );
                }));
                suppliers.add(new TestCaseSupplier(type + "/all " + whitespaces.getKey(), () -> {
                    String text = randomWhiteSpace(whitespaces.getValue());
                    return testCase(name, type, text, "");
                }));
            }
        }
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier.TestCase testCase(String name, DataType type, String data, String expected) {
        return new TestCaseSupplier.TestCase(
            List.of(new TestCaseSupplier.TypedData(new BytesRef(data), type, "str")),
            name + "[val=Attribute[channel=0]]",
            type,
            equalTo(new BytesRef(expected))
        );
    }

    @Override
    protected final List<ArgumentSpec> argSpec() {
        return List.of(required(strings()));
    }

    @Override
    protected final DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    private static String randomWhiteSpace(char[] whitespaces) {
        char[] randomWhitespace = new char[randomIntBetween(1, 8)];
        for (int i = 0; i < randomWhitespace.length; i++) {
            randomWhitespace[i] = whitespaces[randomIntBetween(0, whitespaces.length - 1)];
        }
        return new String(randomWhitespace);
    }
}
