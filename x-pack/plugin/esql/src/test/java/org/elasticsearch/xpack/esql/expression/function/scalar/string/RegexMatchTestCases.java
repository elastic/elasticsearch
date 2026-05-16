/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomUnicodeOfCodepointLength;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThan;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;
import static org.elasticsearch.xpack.esql.core.util.TestUtils.randomCasing;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Shared test-case generator for the {@code RegexMatch} family ({@code RLike}, {@code RLikeList},
 * {@code WildcardLike}, {@code WildcardLikeList}). Each consumer ({@code RLikeTests} etc.) calls
 * {@link #buildCases} with its own pattern-escaping function, optional-trailing-pattern supplier,
 * and an evaluator-name matcher, then wraps the result with
 * {@code AbstractScalarFunctionTestCase.parameterSuppliersFromTypedData}.
 *
 * <p>The matcher parameter is the seam where {@code WildcardLike} diverges from the rest of the
 * family: after this PR, {@code WildcardLike}'s prefix / suffix / contains shapes route through
 * {@code StartsWithEvaluator} / {@code EndsWithEvaluator} / {@code WildcardLikeContainsEvaluator}
 * instead of the family-default {@code AutomataMatchEvaluator}, so it passes a relaxed matcher
 * accepting all four evaluator names. The other three test classes pass
 * {@link #AUTOMATA_MATCH_EVALUATOR}.
 */
final class RegexMatchTestCases {

    private RegexMatchTestCases() {}

    /**
     * Matcher accepting only {@code AutomataMatchEvaluator}-shaped {@code toString} output —
     * the evaluator that {@code RLike} / {@code RLikeList} / {@code WildcardLikeList} all
     * dispatch to. {@code WildcardLikeTests} passes a relaxed matcher that also accepts the
     * {@code StartsWith} / {@code EndsWith} / {@code WildcardLikeContains} fast paths.
     */
    static final Matcher<String> AUTOMATA_MATCH_EVALUATOR = startsWith(
        "AutomataMatchEvaluator[input=Attribute[channel=0], pattern=digraph Automaton {\n"
    );

    /**
     * Build the parametric test-case list every {@code RegexMatch} subclass shares: null operand,
     * empty string, single ASCII char, ASCII string, 3-byte single-codepoint char, 6-byte
     * two-codepoint emoji, 100 random codepoints. Each combines self-match, self-match
     * case-insensitive, trailing-character mismatch, optional-trailing-match, and different-value
     * mismatch variants.
     */
    static List<TestCaseSupplier> buildCases(
        Function<String, String> escapeString,
        Supplier<String> optionalPattern,
        Matcher<String> evaluatorMatcher
    ) {
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
        casesForString(cases, "empty string", () -> "", false, escapeString, optionalPattern, evaluatorMatcher);
        casesForString(
            cases,
            "single ascii character",
            () -> randomAlphaOfLength(1),
            true,
            escapeString,
            optionalPattern,
            evaluatorMatcher
        );
        casesForString(
            cases,
            "ascii string",
            () -> randomAlphaOfLengthBetween(2, 100),
            true,
            escapeString,
            optionalPattern,
            evaluatorMatcher
        );
        casesForString(cases, "3 bytes, 1 code point", () -> "☕", false, escapeString, optionalPattern, evaluatorMatcher);
        casesForString(cases, "6 bytes, 2 code points", () -> "❗️", false, escapeString, optionalPattern, evaluatorMatcher);
        casesForString(
            cases,
            "100 random code points",
            () -> randomUnicodeOfCodepointLength(100),
            true,
            escapeString,
            optionalPattern,
            evaluatorMatcher
        );
        return cases;
    }

    record TextAndPattern(String text, String pattern) {}

    private static void casesForString(
        List<TestCaseSupplier> cases,
        String title,
        Supplier<String> textSupplier,
        boolean canGenerateDifferent,
        Function<String, String> escapeString,
        Supplier<String> optionalPattern,
        Matcher<String> evaluatorMatcher
    ) {
        cases(cases, title + " matches self", () -> {
            String text = textSupplier.get();
            return new TextAndPattern(text, escapeString.apply(text));
        }, true, evaluatorMatcher);
        cases(cases, title + " matches self case insensitive", () -> {
            // RegExp doesn't support case-insensitive matching for Unicodes whose length changes when the case changes.
            // Example: a case-insensitive ES regexp query for the pattern `weiß` won't match the value `WEISS` (but will match `WEIß`).
            // Or `ŉ` (U+0149) vs. `ʼN` (U+02BC U+004E).
            String text, caseChanged;
            for (text = textSupplier.get(), caseChanged = randomCasing(text); text.length() != caseChanged.length();) {
                text = textSupplier.get();
                caseChanged = randomCasing(text);
            }
            return new TextAndPattern(caseChanged, escapeString.apply(text));
        }, true, true, evaluatorMatcher);
        cases(cases, title + " doesn't match self with trailing", () -> {
            String text = textSupplier.get();
            return new TextAndPattern(text, escapeString.apply(text) + randomAlphaOfLength(1));
        }, false, evaluatorMatcher);
        cases(cases, title + " doesn't match self with trailing case insensitive", () -> {
            String text = textSupplier.get();
            return new TextAndPattern(randomCasing(text), escapeString.apply(text) + randomAlphaOfLength(1));
        }, true, false, evaluatorMatcher);
        cases(cases, title + " matches self with optional trailing", () -> {
            String text = randomAlphaOfLength(1);
            return new TextAndPattern(text, escapeString.apply(text) + optionalPattern.get());
        }, true, evaluatorMatcher);
        cases(cases, title + " matches self with optional trailing case insensitive", () -> {
            String text = randomAlphaOfLength(1);
            return new TextAndPattern(randomCasing(text), escapeString.apply(text) + optionalPattern.get());
        }, true, true, evaluatorMatcher);
        if (canGenerateDifferent) {
            cases(cases, title + " doesn't match different", () -> {
                String text = textSupplier.get();
                String different = escapeString.apply(randomValueOtherThan(text, textSupplier));
                return new TextAndPattern(text, different);
            }, false, evaluatorMatcher);
            cases(cases, title + " doesn't match different case insensitive", () -> {
                String text = textSupplier.get();
                Predicate<String> predicate = t -> t.toLowerCase(Locale.ROOT).equals(text.toLowerCase(Locale.ROOT));
                String different = escapeString.apply(randomValueOtherThanMany(predicate, textSupplier));
                return new TextAndPattern(text, different);
            }, true, false, evaluatorMatcher);
        }
    }

    private static void cases(
        List<TestCaseSupplier> cases,
        String title,
        Supplier<TextAndPattern> textAndPattern,
        boolean expected,
        Matcher<String> evaluatorMatcher
    ) {
        cases(cases, title, textAndPattern, false, expected, evaluatorMatcher);
    }

    private static void cases(
        List<TestCaseSupplier> cases,
        String title,
        Supplier<TextAndPattern> textAndPattern,
        boolean caseInsensitive,
        boolean expected,
        Matcher<String> evaluatorMatcher
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
                    evaluatorMatcher,
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
                        evaluatorMatcher,
                        DataType.BOOLEAN,
                        equalTo(expected)
                    );
                }));
            }
        }
    }
}
