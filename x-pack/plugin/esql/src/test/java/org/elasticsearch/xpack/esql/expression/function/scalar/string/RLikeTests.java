/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.TestUtils.randomCasing;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class RLikeTests extends AbstractScalarFunctionTestCase {
    public RLikeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final Function<String, String> escapeString = str -> {
            for (String syntax : new String[] { "\\", ".", "?", "+", "*", "|", "{", "}", "[", "]", "(", ")", "\"", "<", ">", "#", "&" }) {
                str = str.replace(syntax, "\\" + syntax);
            }
            return str;
        };
        return parameters(escapeString, () -> randomAlphaOfLength(1) + "?");
    }

    static Iterable<Object[]> parameters(Function<String, String> escapeString, Supplier<String> optionalPattern) {
        List<TestCaseSupplier> cases = new ArrayList<>();
        cases.add(
            new TestCaseSupplier(
                "null",
                List.of(DataType.NULL, DataType.KEYWORD, DataType.BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(null, DataType.NULL, "e"),
                        new TestCaseSupplier.TypedData(new BytesRef(randomAlphaOfLength(10)), DataType.KEYWORD, "pattern").forceLiteral(),
                        new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "caseInsensitive").forceLiteral()
                    ),
                    "LiteralsEvaluator[lit=null]",
                    DataType.BOOLEAN,
                    nullValue()
                )
            )
        );
        casesForString(cases, "empty string", () -> "", false, escapeString, optionalPattern);
        casesForString(cases, "single ascii character", () -> randomAlphaOfLength(1), true, escapeString, optionalPattern);
        casesForString(cases, "ascii string", () -> randomAlphaOfLengthBetween(2, 100), true, escapeString, optionalPattern);
        casesForString(cases, "3 bytes, 1 code point", () -> "☕", false, escapeString, optionalPattern);
        casesForString(cases, "6 bytes, 2 code points", () -> "❗️", false, escapeString, optionalPattern);
        casesForString(
            cases,
            "100 random code points",
            () -> randomUnicodeOfCodepointLength(100),
            true,
            false, // 8.x's Lucene doesn't support case-insensitive matching for Unicode code points, only ASCII
            escapeString,
            optionalPattern
        );
        return parameterSuppliersFromTypedData(cases);
    }

    record TextAndPattern(String text, String pattern) {}

    private static void casesForString(
        List<TestCaseSupplier> cases,
        String title,
        Supplier<String> textSupplier,
        boolean canGenerateDifferent,
        Function<String, String> escapeString,
        Supplier<String> optionalPattern
    ) {
        casesForString(cases, title, textSupplier, canGenerateDifferent, true, escapeString, optionalPattern);
    }

    private static void casesForString(
        List<TestCaseSupplier> cases,
        String title,
        Supplier<String> textSupplier,
        boolean canGenerateDifferent,
        boolean canGenerateCaseInsensitive,
        Function<String, String> escapeString,
        Supplier<String> optionalPattern
    ) {
        cases(cases, title + " matches self", () -> {
            String text = textSupplier.get();
            return new TextAndPattern(text, escapeString.apply(text));
        }, true);
        if (canGenerateCaseInsensitive) {
            cases(cases, title + " matches self case insensitive", () -> {
                String text = textSupplier.get();
                return new TextAndPattern(randomCasing(text), escapeString.apply(text));
            }, true, true);
        }
        cases(cases, title + " doesn't match self with trailing", () -> {
            String text = textSupplier.get();
            return new TextAndPattern(text, escapeString.apply(text) + randomAlphaOfLength(1));
        }, false);
        if (canGenerateCaseInsensitive) {
            cases(cases, title + " doesn't match self with trailing case insensitive", () -> {
                String text = textSupplier.get();
                return new TextAndPattern(randomCasing(text), escapeString.apply(text) + randomAlphaOfLength(1));
            }, true, false);
        }
        cases(cases, title + " matches self with optional trailing", () -> {
            String text = randomAlphaOfLength(1);
            return new TextAndPattern(text, escapeString.apply(text) + optionalPattern.get());
        }, true);
        if (canGenerateCaseInsensitive) {
            cases(cases, title + " matches self with optional trailing case insensitive", () -> {
                String text = randomAlphaOfLength(1);
                return new TextAndPattern(randomCasing(text), escapeString.apply(text) + optionalPattern.get());
            }, true, true);
        }
        if (canGenerateDifferent) {
            cases(cases, title + " doesn't match different", () -> {
                String text = textSupplier.get();
                String different = escapeString.apply(randomValueOtherThan(text, textSupplier));
                return new TextAndPattern(text, different);
            }, false);
            if (canGenerateCaseInsensitive) {
                cases(cases, title + " doesn't match different case insensitive", () -> {
                    String text = textSupplier.get();
                    Predicate<String> predicate = t -> t.toLowerCase(Locale.ROOT).equals(text.toLowerCase(Locale.ROOT));
                    String different = escapeString.apply(randomValueOtherThanMany(predicate, textSupplier));
                    return new TextAndPattern(text, different);
                }, true, false);
            }
        }
    }

    private static void cases(List<TestCaseSupplier> cases, String title, Supplier<TextAndPattern> textAndPattern, boolean expected) {
        cases(cases, title, textAndPattern, false, expected);
    }

    private static void cases(
        List<TestCaseSupplier> cases,
        String title,
        Supplier<TextAndPattern> textAndPattern,
        boolean caseInsensitive,
        boolean expected
    ) {
        for (DataType type : DataType.stringTypes()) {
            cases.add(new TestCaseSupplier(title + " with " + type.esType(), List.of(type, DataType.KEYWORD, DataType.BOOLEAN), () -> {
                TextAndPattern v = textAndPattern.get();
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(v.text), type, "e"),
                        new TestCaseSupplier.TypedData(new BytesRef(v.pattern), DataType.KEYWORD, "pattern").forceLiteral(),
                        new TestCaseSupplier.TypedData(caseInsensitive, DataType.BOOLEAN, "caseInsensitive").forceLiteral()
                    ),
                    startsWith("AutomataMatchEvaluator[input=Attribute[channel=0], pattern=digraph Automaton {\n"),
                    DataType.BOOLEAN,
                    equalTo(expected)
                );
            }));
            if (caseInsensitive == false) {
                cases.add(new TestCaseSupplier(title + " with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                    TextAndPattern v = textAndPattern.get();
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef(v.text), type, "e"),
                            new TestCaseSupplier.TypedData(new BytesRef(v.pattern), DataType.KEYWORD, "pattern").forceLiteral()
                        ),
                        startsWith("AutomataMatchEvaluator[input=Attribute[channel=0], pattern=digraph Automaton {\n"),
                        DataType.BOOLEAN,
                        equalTo(expected)
                    );
                }));
            }
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return buildRLike(logger, source, args);
    }

    static Expression buildRLike(Logger logger, Source source, List<Expression> args) {
        Expression expression = args.get(0);
        Literal pattern = (Literal) args.get(1);
        Literal caseInsensitive = args.size() > 2 ? (Literal) args.get(2) : null;
        String patternString = ((BytesRef) pattern.fold(FoldContext.small())).utf8ToString();
        boolean caseInsensitiveBool = caseInsensitive != null ? (boolean) caseInsensitive.fold(FoldContext.small()) : false;
        logger.info("pattern={} caseInsensitive={}", patternString, caseInsensitiveBool);

        return caseInsensitiveBool
            ? new RLike(source, expression, new RLikePattern(patternString), true)
            : (randomBoolean()
                ? new RLike(source, expression, new RLikePattern(patternString))
                : new RLike(source, expression, new RLikePattern(patternString), false));
    }

    @AfterClass
    public static void renderNotRLike() throws IOException {
        WildcardLikeTests.renderNot(constructorWithFunctionInfo(RLike.class), "RLIKE", d -> d);
    }
}
