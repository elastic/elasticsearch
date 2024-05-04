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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

@FunctionName("like")
public class WildcardLikeTests extends AbstractFunctionTestCase {
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
        for (DataType type : new DataType[] { DataTypes.KEYWORD, DataTypes.TEXT }) {
            suppliers.add(new TestCaseSupplier(" with " + type.esType(), List.of(type, type), () -> {
                BytesRef str = new BytesRef(randomAlphaOfLength(5));
                BytesRef pattern = new BytesRef(randomAlphaOfLength(2));
                Boolean match = str.utf8ToString().startsWith(pattern.utf8ToString());
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(str, type, "str"),
                        new TestCaseSupplier.TypedData(pattern, type, "pattern").forceLiteral()
                    ),
                    startsWith("AutomataMatchEvaluator[input=Attribute[channel=0], pattern=digraph Automaton {\n"),
                    DataTypes.BOOLEAN,
                    equalTo(match)
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
        if (args.size() > 2) {
            Literal caseInsensitive = (Literal) args.get(2);
            assertThat(caseInsensitive.fold(), equalTo(false));
        }
        return new WildcardLike(source, expression, new WildcardPattern(((BytesRef) pattern.fold()).utf8ToString()));
    }
}
