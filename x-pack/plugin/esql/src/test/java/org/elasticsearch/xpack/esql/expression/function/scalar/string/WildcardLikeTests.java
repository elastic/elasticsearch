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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.hamcrest.Matcher;
import org.junit.AfterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.DocsV3Support.renderNegatedOperator;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

@FunctionName("like")
public class WildcardLikeTests extends AbstractScalarFunctionTestCase {
    public WildcardLikeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final Function<String, String> escapeString = str -> {
            for (String syntax : new String[] { "\\", "*", "?" }) {
                str = str.replace(syntax, "\\" + syntax);
            }
            return str;
        };
        // The shared suppliers in RLikeTests only emit `<text>*` prefix shapes via the `optionalPattern=() -> "*"` argument,
        // so the WildcardLike fast paths visible here are StartsWith and (for the empty optional) AutomataMatch. Contains
        // is not reachable from these suppliers and so is not in the matcher.
        Matcher<String> evaluatorMatcher = anyOf(
            startsWith("AutomataMatchEvaluator[input=Attribute[channel=0], pattern=digraph Automaton {\n"),
            startsWith("StartsWithEvaluator[str=Attribute[channel=0], prefix="),
            startsWith("EndsWithEvaluator[str=Attribute[channel=0], suffix=")
        );
        List<Object[]> cases = (List<Object[]>) RLikeTests.parameters(escapeString, () -> "*", evaluatorMatcher);

        List<TestCaseSupplier> suppliers = new ArrayList<>();
        addCases(suppliers);

        for (TestCaseSupplier supplier : suppliers) {
            cases.add(new Object[] { supplier });
        }

        return cases;
    }

    private static void addCases(List<TestCaseSupplier> suppliers) {
        for (DataType type : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
            suppliers.add(new TestCaseSupplier(" with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                BytesRef str = new BytesRef(randomAlphaOfLength(5));
                String patternString = randomAlphaOfLength(2);
                BytesRef pattern = new BytesRef(patternString + "*");
                Boolean match = str.utf8ToString().startsWith(patternString);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(str, type, "str"),
                        new TestCaseSupplier.TypedData(pattern, DataType.KEYWORD, "pattern").forceLiteral()
                    ),
                    startsWith("StartsWithEvaluator[str=Attribute[channel=0], prefix="),
                    DataType.BOOLEAN,
                    equalTo(match)
                );
            }));
            // Suffix-only pattern (*literal) — verifies dispatch to EndsWith.
            suppliers.add(new TestCaseSupplier("suffix with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                String suffixString = randomAlphaOfLength(2);
                BytesRef str = new BytesRef(randomAlphaOfLength(3) + suffixString);
                BytesRef pattern = new BytesRef("*" + suffixString);
                Boolean match = str.utf8ToString().endsWith(suffixString);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(str, type, "str"),
                        new TestCaseSupplier.TypedData(pattern, DataType.KEYWORD, "pattern").forceLiteral()
                    ),
                    startsWith("EndsWithEvaluator[str=Attribute[channel=0], suffix="),
                    DataType.BOOLEAN,
                    equalTo(match)
                );
            }));
            // Contains pattern (*literal*) — verifies dispatch to the byte-scan WildcardLikeContainsEvaluator.
            suppliers.add(new TestCaseSupplier("contains match with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                String middle = randomAlphaOfLength(2);
                BytesRef str = new BytesRef(randomAlphaOfLength(2) + middle + randomAlphaOfLength(2));
                BytesRef pattern = new BytesRef("*" + middle + "*");
                Boolean match = str.utf8ToString().contains(middle);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(str, type, "str"),
                        new TestCaseSupplier.TypedData(pattern, DataType.KEYWORD, "pattern").forceLiteral()
                    ),
                    startsWith("WildcardLikeContainsEvaluator[str=Attribute[channel=0], pattern="),
                    DataType.BOOLEAN,
                    equalTo(match)
                );
            }));
            // Contains pattern where the substring is NOT present — exercises the false branch.
            suppliers.add(new TestCaseSupplier("contains no-match with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                String middle = randomAlphaOfLength(3);
                String different = randomValueOtherThanMany(s -> s.contains(middle), () -> randomAlphaOfLength(5));
                BytesRef str = new BytesRef(different);
                BytesRef pattern = new BytesRef("*" + middle + "*");
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(str, type, "str"),
                        new TestCaseSupplier.TypedData(pattern, DataType.KEYWORD, "pattern").forceLiteral()
                    ),
                    startsWith("WildcardLikeContainsEvaluator[str=Attribute[channel=0], pattern="),
                    DataType.BOOLEAN,
                    equalTo(false)
                );
            }));
            // Long-literal contains pattern — value and literal both exceed the SIMD activation
            // threshold inside ESVectorUtil (>= 24 bytes). Ensures the evaluator drives the
            // vectorized first+last-byte filter, not just the scalar fallback.
            suppliers.add(new TestCaseSupplier("long-literal contains match with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                String middle = randomAlphaOfLength(32);
                BytesRef str = new BytesRef(randomAlphaOfLength(16) + middle + randomAlphaOfLength(16));
                BytesRef pattern = new BytesRef("*" + middle + "*");
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(str, type, "str"),
                        new TestCaseSupplier.TypedData(pattern, DataType.KEYWORD, "pattern").forceLiteral()
                    ),
                    startsWith("WildcardLikeContainsEvaluator[str=Attribute[channel=0], pattern="),
                    DataType.BOOLEAN,
                    equalTo(true)
                );
            }));
            // Long-literal miss — same SIMD path, false branch.
            suppliers.add(
                new TestCaseSupplier("long-literal contains no-match with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                    String middle = randomAlphaOfLength(40);
                    String different = randomValueOtherThanMany(s -> s.contains(middle), () -> randomAlphaOfLength(80));
                    BytesRef str = new BytesRef(different);
                    BytesRef pattern = new BytesRef("*" + middle + "*");
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(str, type, "str"),
                            new TestCaseSupplier.TypedData(pattern, DataType.KEYWORD, "pattern").forceLiteral()
                        ),
                        startsWith("WildcardLikeContainsEvaluator[str=Attribute[channel=0], pattern="),
                        DataType.BOOLEAN,
                        equalTo(false)
                    );
                })
            );
            // Literal-shorter-than-value but value is short — exercises the scalar path below the
            // SIMD threshold.
            suppliers.add(
                new TestCaseSupplier("short-literal contains match with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                    BytesRef str = new BytesRef("axyzbc");
                    BytesRef pattern = new BytesRef("*xyz*");
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(str, type, "str"),
                            new TestCaseSupplier.TypedData(pattern, DataType.KEYWORD, "pattern").forceLiteral()
                        ),
                        startsWith("WildcardLikeContainsEvaluator[str=Attribute[channel=0], pattern="),
                        DataType.BOOLEAN,
                        equalTo(true)
                    );
                })
            );
            // Embedded wildcard in the middle (*foo*bar*) — must NOT take the contains fast path;
            // falls through to AutomataMatch.
            suppliers.add(new TestCaseSupplier("multi-wildcard with " + type.esType(), List.of(type, DataType.KEYWORD), () -> {
                String a = randomAlphaOfLength(2);
                String b = randomAlphaOfLength(2);
                BytesRef str = new BytesRef(a + randomAlphaOfLength(1) + b);
                BytesRef pattern = new BytesRef("*" + a + "*" + b + "*");
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(str, type, "str"),
                        new TestCaseSupplier.TypedData(pattern, DataType.KEYWORD, "pattern").forceLiteral()
                    ),
                    startsWith("AutomataMatchEvaluator[input=Attribute[channel=0], pattern=digraph Automaton {\n"),
                    DataType.BOOLEAN,
                    equalTo(true)
                );
            }));
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return buildWildcardLike(source, args);
    }

    Expression buildWildcardLike(Source source, List<Expression> args) {
        Expression expression = args.get(0);
        Literal pattern = (Literal) args.get(1);
        Literal caseInsensitive = args.size() > 2 ? (Literal) args.get(2) : null;
        boolean caseInsesitiveBool = caseInsensitive != null && (boolean) caseInsensitive.fold(FoldContext.small());

        WildcardPattern wildcardPattern = new WildcardPattern(((BytesRef) pattern.fold(FoldContext.small())).utf8ToString());
        return caseInsesitiveBool
            ? new WildcardLike(source, expression, wildcardPattern, true)
            : (randomBoolean()
                ? new WildcardLike(source, expression, wildcardPattern)
                : new WildcardLike(source, expression, wildcardPattern, false));
    }

    @AfterClass
    public static void renderNotLike() throws Exception {
        renderNegatedOperator(
            constructorWithFunctionInfo(WildcardLike.class),
            "LIKE",
            d -> d,
            getTestClass(),
            DocsV3Support.callbacksFromSystemProperty()
        );
    }
}
