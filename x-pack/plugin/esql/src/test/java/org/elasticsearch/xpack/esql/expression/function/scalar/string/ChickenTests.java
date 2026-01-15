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

        // Tests with explicit style parameter
        cases.add(new TestCaseSupplier("Chicken with ordinary style", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            String message = "Hello!";
            String style = "ordinary";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message"),
                    new TestCaseSupplier.TypedData(new BytesRef(style), DataType.KEYWORD, "style")
                ),
                "ChickenEvaluator[message=Attribute[channel=0], style=Attribute[channel=1]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.ORDINARY)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken with soup style", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            String message = "Soup time!";
            String style = "soup";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message"),
                    new TestCaseSupplier.TypedData(new BytesRef(style), DataType.KEYWORD, "style")
                ),
                "ChickenEvaluator[message=Attribute[channel=0], style=Attribute[channel=1]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.SOUP)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken with early_state (egg) style", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            String message = "I'm an egg!";
            String style = "early_state";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message"),
                    new TestCaseSupplier.TypedData(new BytesRef(style), DataType.KEYWORD, "style")
                ),
                "ChickenEvaluator[message=Attribute[channel=0], style=Attribute[channel=1]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.EARLY_STATE)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken with text style input", List.of(DataType.KEYWORD, DataType.TEXT), () -> {
            String message = "Racing!";
            String style = "racing";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message"),
                    new TestCaseSupplier.TypedData(new BytesRef(style), DataType.TEXT, "style")
                ),
                "ChickenEvaluator[message=Attribute[channel=0], style=Attribute[channel=1]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.RACING)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken with text message input", List.of(DataType.TEXT, DataType.KEYWORD), () -> {
            String message = "ES|QL rocks!";
            String style = "whistling";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.TEXT, "message"),
                    new TestCaseSupplier.TypedData(new BytesRef(style), DataType.KEYWORD, "style")
                ),
                "ChickenEvaluator[message=Attribute[channel=0], style=Attribute[channel=1]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.WHISTLING)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken with empty message", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            String message = "";
            String style = "stoned";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message"),
                    new TestCaseSupplier.TypedData(new BytesRef(style), DataType.KEYWORD, "style")
                ),
                "ChickenEvaluator[message=Attribute[channel=0], style=Attribute[channel=1]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.STONED)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken with long message wrapping", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            String message = "This is a really long message that should definitely wrap across multiple lines in the speech bubble";
            String style = "laying";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message"),
                    new TestCaseSupplier.TypedData(new BytesRef(style), DataType.KEYWORD, "style")
                ),
                "ChickenEvaluator[message=Attribute[channel=0], style=Attribute[channel=1]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.LAYING)
            );
        }));

        cases.add(new TestCaseSupplier("Chicken with text message and text style", List.of(DataType.TEXT, DataType.TEXT), () -> {
            String message = "Both text types!";
            String style = "thinks_its_a_duck";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.TEXT, "message"),
                    new TestCaseSupplier.TypedData(new BytesRef(style), DataType.TEXT, "style")
                ),
                "ChickenEvaluator[message=Attribute[channel=0], style=Attribute[channel=1]]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.THINKS_ITS_A_DUCK)
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Chicken(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    /**
     * Matcher that verifies chicken output has expected structure.
     * If a specific chicken style is provided, also verifies the art matches.
     */
    private static TypeSafeMatcher<Object> chickenOutputMatcher(String expectedMessage, ChickenArtBuilder expectedStyle) {
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

                // If a specific style is expected, verify it's used
                if (expectedStyle != null) {
                    String expectedArt = expectedStyle.ascii.utf8ToString();
                    if (result.contains(expectedArt) == false) {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("chicken output containing message '").appendText(expectedMessage).appendText("'");
                if (expectedStyle != null) {
                    description.appendText(" with style ").appendText(expectedStyle.name());
                }
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
