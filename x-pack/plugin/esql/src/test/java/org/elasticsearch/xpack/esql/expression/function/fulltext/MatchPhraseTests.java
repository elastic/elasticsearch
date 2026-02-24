/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;

@FunctionName("match_phrase")
public class MatchPhraseTests extends SingleFieldFullTextFunctionTestCase {

    public MatchPhraseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addFunctionNamedParams(addNullFieldTestCases(testCaseSuppliers()), mapExpressionSupplier()));
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        addStringTestCases(suppliers);
        return addNullFieldTestCases(suppliers);
    }

    private static Supplier<MapExpression> mapExpressionSupplier() {
        return () -> new MapExpression(
            Source.EMPTY,
            List.of(new Literal(Source.EMPTY, "slop", INTEGER), Literal.integer(Source.EMPTY, randomNonNegativeInt()))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return build(new MatchPhrase(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null), args);
    }
}
