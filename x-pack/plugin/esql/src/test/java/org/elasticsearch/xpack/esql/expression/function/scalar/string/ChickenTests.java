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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
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

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;

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
        return parameterSuppliersFromTypedData(testCaseSuppliers());
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> cases = new ArrayList<>();

        // Test with message only (default style = ordinary)
        cases.add(new TestCaseSupplier("Chicken with default style", List.of(DataType.KEYWORD), () -> {
            String message = "Hello!";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message")),
                "ChickenEvaluator[message=Attribute[channel=0], chickenStyle=ORDINARY, width=40]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.ORDINARY)
            );
        }));

        // Test with text message (default style = ordinary)
        cases.add(new TestCaseSupplier("Chicken with text message", List.of(DataType.TEXT), () -> {
            String message = "Hello from text!";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(message), DataType.TEXT, "message")),
                "ChickenEvaluator[message=Attribute[channel=0], chickenStyle=ORDINARY, width=40]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.ORDINARY)
            );
        }));

        return addOptionsTestCases(cases);
    }

    private static List<TestCaseSupplier> addOptionsTestCases(List<TestCaseSupplier> suppliers) {
        List<TestCaseSupplier> result = new ArrayList<>(suppliers);

        // Test with options map containing style
        result.add(new TestCaseSupplier("Chicken with soup style option", List.of(DataType.KEYWORD, UNSUPPORTED), () -> {
            String message = "Soup time!";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message"),
                    new TestCaseSupplier.TypedData(createOptions("soup", null), UNSUPPORTED, "options").forceLiteral()
                ),
                "ChickenEvaluator[message=Attribute[channel=0], chickenStyle=SOUP, width=40]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.SOUP)
            );
        }));

        // Test with options map containing style and width
        result.add(new TestCaseSupplier("Chicken with style and width options", List.of(DataType.KEYWORD, UNSUPPORTED), () -> {
            String message = "Custom width!";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(message), DataType.KEYWORD, "message"),
                    new TestCaseSupplier.TypedData(createOptions("racing", 60), UNSUPPORTED, "options").forceLiteral()
                ),
                "ChickenEvaluator[message=Attribute[channel=0], chickenStyle=RACING, width=60]",
                DataType.KEYWORD,
                chickenOutputMatcher(message, ChickenArtBuilder.RACING)
            );
        }));

        return result;
    }

    private static MapExpression createOptions(String style, Integer width) {
        List<Expression> optionsMap = new ArrayList<>();

        if (style != null) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "style"));
            optionsMap.add(Literal.keyword(Source.EMPTY, style));
        }

        if (width != null) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "width"));
            optionsMap.add(new Literal(Source.EMPTY, width, DataType.INTEGER));
        }

        return optionsMap.isEmpty() ? null : new MapExpression(Source.EMPTY, optionsMap);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression options = args.size() < 2 ? null : args.get(1);
        return new Chicken(source, args.get(0), options);
    }

    @Override
    public void testFold() {
        Expression expression = buildFieldExpression(testCase);
        // Skip testFold if the expression is not foldable (e.g., when options contains MapExpression)
        if (expression.foldable() == false) {
            return;
        }
        super.testFold();
    }

    public void testDefaultStyle() {
        String message = "Default chicken!";
        String result = process(message, null, null);
        assertNotNull(result);
        assertTrue(result.contains(ChickenArtBuilder.ORDINARY.ascii.utf8ToString()));
    }

    public void testCustomStyle() {
        String message = "I'm soup!";
        String result = process(message, "soup", null);
        assertNotNull(result);
        assertTrue(result.contains(ChickenArtBuilder.SOUP.ascii.utf8ToString()));
    }

    public void testCustomWidth() {
        String message = "Wide bubble!";
        String result = process(message, "ordinary", 60);
        assertNotNull(result);
        assertTrue(result.contains(message));
    }

    private String process(String message, String style, Integer width) {
        MapExpression optionsMap = createOptions(style, width);

        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(new Chicken(Source.EMPTY, field("message", DataType.KEYWORD), optionsMap))
                .get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(message))))
        ) {
            if (block.isNull(0)) {
                return null;
            }
            Object result = toJavaObject(block, 0);
            if (result instanceof BytesRef bytesRef) {
                return bytesRef.utf8ToString();
            }
            return result.toString();
        }
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
