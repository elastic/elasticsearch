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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

/**
 * This class is only used to generates docs for the match operator - all testing is the same as {@link MatchTests}
 */
@FunctionName("match_operator")
public class MatchOperatorTests extends AbstractMatchFullTextFunctionTests {

    public MatchOperatorTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(testCaseSuppliers());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MatchOperator(source, args.get(0), args.get(1));
    }

    @Override
    public void testSerializationOfSimple() {
        // MatchOperator is not a separate function that needs to be serialized, it's serialized via Match
    }
}
