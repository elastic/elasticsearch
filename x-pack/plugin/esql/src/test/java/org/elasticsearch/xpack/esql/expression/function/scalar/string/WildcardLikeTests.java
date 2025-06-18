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
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.junit.AfterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.DocsV3Support.renderNegatedOperator;
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
        List<Object[]> cases = (List<Object[]>) RLikeTests.parameters(escapeString, () -> "*");

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
                    startsWith("AutomataMatchEvaluator[input=Attribute[channel=0], pattern=digraph Automaton {\n"),
                    DataType.BOOLEAN,
                    equalTo(match)
                );
            }));
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return buildWildcardLike(source, args);
    }

    static Expression buildWildcardLike(Source source, List<Expression> args) {
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
        renderNegatedOperator(constructorWithFunctionInfo(WildcardLike.class), "LIKE", d -> d, getTestClass());
    }
}
