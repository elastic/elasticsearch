/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.unboundid.util.NotNull;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.junit.AfterClass;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

@FunctionName("like")
public class WildcardLikeTests extends AbstractScalarFunctionTestCase {
    public WildcardLikeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> cases = (List<Object[]>) RLikeTests.parameters(str -> {
            for (String syntax : new String[] { "\\", "*" }) {
                str = str.replace(syntax, "\\" + syntax);
            }
            return str;
        }, () -> "*");

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
        if (args.size() > 2) {
            Literal caseInsensitive = (Literal) args.get(2);
            assertThat(caseInsensitive.fold(FoldContext.small()), equalTo(false));
        }
        return new WildcardLike(source, expression, new WildcardPattern(((BytesRef) pattern.fold(FoldContext.small())).utf8ToString()));
    }

    @AfterClass
    public static void renderNotLike() throws IOException {
        renderNot(constructorWithFunctionInfo(WildcardLike.class), "LIKE", d -> d);
    }

    public static void renderNot(@NotNull Constructor<?> ctor, String name, Function<String, String> description) throws IOException {
        FunctionInfo orig = ctor.getAnnotation(FunctionInfo.class);
        assert orig != null;
        FunctionInfo functionInfo = new FunctionInfo() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return orig.annotationType();
            }

            @Override
            public String operator() {
                return "NOT " + name;
            }

            @Override
            public String[] returnType() {
                return orig.returnType();
            }

            @Override
            public boolean preview() {
                return orig.preview();
            }

            @Override
            public String description() {
                return description.apply(orig.description().replace(name, "NOT " + name));
            }

            @Override
            public String detailedDescription() {
                return "";
            }

            @Override
            public String note() {
                return orig.note().replace(name, "NOT " + name);
            }

            @Override
            public String appendix() {
                return orig.appendix().replace(name, "NOT " + name);
            }

            @Override
            public boolean isAggregation() {
                return orig.isAggregation();
            }

            @Override
            public Example[] examples() {
                // throw away examples
                return new Example[] {};
            }
        };
        renderDocsForOperators("not_" + name.toLowerCase(Locale.ENGLISH), ctor, functionInfo);
    }
}
