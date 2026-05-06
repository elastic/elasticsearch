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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

public class KqlTests extends NoFieldFullTextFunctionTestCase {
    public KqlTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addFunctionNamedParams(getStringTestSupplier(), mapExpressionSupplier()));
    }

    private static Supplier<MapExpression> mapExpressionSupplier() {
        return () -> new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, "case_insensitive"), Literal.TRUE));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Kql(source, args.get(0), args.size() > 1 ? args.get(1) : null, testCase.getConfiguration());
    }
}
