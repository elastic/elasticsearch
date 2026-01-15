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
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Tests for the {@link Chicken} Easter egg function.
 * Uses a fake function name to skip documentation generation since this is an Easter egg.
 */
@FunctionName("_chicken_no_docs")
public class ChickenTests extends AbstractScalarFunctionTestCase {
    public ChickenTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();

        cases.add(new TestCaseSupplier("Chicken basic test", List.of(DataType.KEYWORD), () -> {
            String message = "Hello!";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message")),
                "ChickenEvaluator[message=Attribute[channel=0]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken with text input", List.of(DataType.TEXT), () -> {
            String message = "ES|QL rocks!";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(message), DataType.TEXT, "message")),
                "ChickenEvaluator[message=Attribute[channel=0]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken empty message", List.of(DataType.KEYWORD), () -> {
            String message = "";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message")),
                "ChickenEvaluator[message=Attribute[channel=0]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken single line bubble", List.of(DataType.KEYWORD), () -> {
            String message = "Short";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message")),
                "ChickenEvaluator[message=Attribute[channel=0]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken long message wrapping", List.of(DataType.KEYWORD), () -> {
            String message = "This is a really long message that should definitely wrap across multiple lines in the speech bubble";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message")),
                "ChickenEvaluator[message=Attribute[channel=0]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message)
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Chicken(source, args.get(0));
    }

    /**
     * Matcher that verifies chicken output has expected structure without requiring exact match.
     * Since the chicken art is randomly selected, we verify structure rather than exact content.
     */
    private static TypeSafeMatcher<Object> chickenOutputMatcher(String expectedMessage) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Object item) {
                if (item instanceof BytesRef == false) {
                    return false;
                }
                String result = ((BytesRef) item).utf8ToString();

                // Check structural elements
                if (result.startsWith(" _") == false) {
                    return false; // Missing top border
                }
                if (result.contains(" -") == false) {
                    return false; // Missing bottom border
                }
                if (result.contains("\\") == false) {
                    return false; // Missing chicken art (all chickens have backslash)
                }

                // Check message content is present (for non-empty messages)
                // For wrapped messages, check that all words are present
                if (expectedMessage.isEmpty() == false) {
                    for (String word : expectedMessage.split(" ")) {
                        if (result.contains(word) == false) {
                            return false;
                        }
                    }
                }

                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("chicken output containing message '").appendText(expectedMessage).appendText("'");
            }

            @Override
            protected void describeMismatchSafely(Object item, Description mismatchDescription) {
                if (item instanceof BytesRef) {
                    mismatchDescription.appendText("was:\n").appendText(((BytesRef) item).utf8ToString());
                } else {
                    mismatchDescription.appendText("was not a BytesRef: ").appendValue(item);
                }
            }
        };
    }
}
