/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/106533")
public class RLikeTests extends AbstractFunctionTestCase {
    public RLikeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameters(() -> randomAlphaOfLength(1) + "?");
    }

    static Iterable<Object[]> parameters(Supplier<String> optionalPattern) {
        List<TestCaseSupplier> cases = new ArrayList<>();
        cases.add(
            new TestCaseSupplier(
                "null",
                List.of(DataTypes.NULL, DataTypes.KEYWORD, DataTypes.BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(null, DataTypes.NULL, "e"),
                        new TestCaseSupplier.TypedData(new BytesRef(randomAlphaOfLength(10)), DataTypes.KEYWORD, "pattern").forceLiteral(),
                        new TestCaseSupplier.TypedData(false, DataTypes.BOOLEAN, "caseInsensitive").forceLiteral()
                    ),
                    "LiteralsEvaluator[lit=null]",
                    DataTypes.BOOLEAN,
                    nullValue()
                )
            )
        );
        casesForString(cases, "empty string", () -> "", false, optionalPattern);
        casesForString(cases, "single ascii character", () -> randomAlphaOfLength(1), true, optionalPattern);
        casesForString(cases, "ascii string", () -> randomAlphaOfLengthBetween(2, 100), true, optionalPattern);
        casesForString(cases, "3 bytes, 1 code point", () -> "☕", false, optionalPattern);
        casesForString(cases, "6 bytes, 2 code points", () -> "❗️", false, optionalPattern);
        casesForString(cases, "100 random code points", () -> randomUnicodeOfCodepointLength(100), true, optionalPattern);
        for (DataType type : EsqlDataTypes.types()) {
            if (type == DataTypes.KEYWORD || type == DataTypes.TEXT || type == DataTypes.NULL) {
                continue;
            }
            if (EsqlDataTypes.isRepresentable(type) == false) {
                continue;
            }
            cases.add(
                new TestCaseSupplier(
                    List.of(type, DataTypes.KEYWORD, DataTypes.BOOLEAN),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            new TestCaseSupplier.TypedData(randomLiteral(type).value(), type, "e"),
                            new TestCaseSupplier.TypedData(new BytesRef(randomAlphaOfLength(10)), DataTypes.KEYWORD, "pattern")
                                .forceLiteral(),
                            new TestCaseSupplier.TypedData(false, DataTypes.BOOLEAN, "caseInsensitive").forceLiteral()
                        ),
                        "argument of [] must be [string], found value [e] type [" + type.typeName() + "]"
                    )
                )
            );
        }
        return parameterSuppliersFromTypedData(cases);
    }

    record TextAndPattern(String text, String pattern) {}

    private static void casesForString(
        List<TestCaseSupplier> cases,
        String title,
        Supplier<String> textSupplier,
        boolean canGenerateDifferent,
        Supplier<String> optionalPattern
    ) {
        cases(cases, title + " matches self", () -> {
            String text = textSupplier.get();
            return new TextAndPattern(text, text);
        }, true);
        cases(cases, title + " doesn't match self with trailing", () -> {
            String text = textSupplier.get();
            return new TextAndPattern(text, text + randomAlphaOfLength(1));
        }, false);
        cases(cases, title + " matches self with optional trailing", () -> {
            String text = randomAlphaOfLength(1);
            return new TextAndPattern(text, text + optionalPattern.get());
        }, true);
        if (canGenerateDifferent) {
            cases(cases, title + " doesn't match different", () -> {
                String text = textSupplier.get();
                String different = randomValueOtherThan(text, textSupplier);
                return new TextAndPattern(text, different);
            }, false);
        }
    }

    private static void cases(List<TestCaseSupplier> cases, String title, Supplier<TextAndPattern> textAndPattern, boolean expected) {
        for (DataType type : new DataType[] { DataTypes.KEYWORD, DataTypes.TEXT }) {
            cases.add(new TestCaseSupplier(title + " with " + type.esType(), List.of(type, type, DataTypes.BOOLEAN), () -> {
                TextAndPattern v = textAndPattern.get();
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(v.text), type, "e"),
                        new TestCaseSupplier.TypedData(new BytesRef(v.pattern), type, "pattern").forceLiteral(),
                        new TestCaseSupplier.TypedData(false, DataTypes.BOOLEAN, "caseInsensitive").forceLiteral()
                    ),
                    startsWith("AutomataMatchEvaluator[input=Attribute[channel=0], pattern=digraph Automaton {\n"),
                    DataTypes.BOOLEAN,
                    equalTo(expected)
                );
            }));
        }
    }

    @Override
    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        assumeFalse("generated test cases containing nulls by hand", true);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression expression = args.get(0);
        Literal pattern = (Literal) args.get(1);
        Literal caseInsensitive = (Literal) args.get(2);
        return new RLike(
            source,
            expression,
            new RLikePattern(((BytesRef) pattern.fold()).utf8ToString()),
            (Boolean) caseInsensitive.fold()
        );
    }
}
