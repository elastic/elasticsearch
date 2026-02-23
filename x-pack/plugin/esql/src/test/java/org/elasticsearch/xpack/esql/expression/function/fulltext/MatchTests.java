/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

@FunctionName("match")
public class MatchTests extends AbstractMatchFullTextFunctionTests {

    public MatchTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addNullFieldTestCases(addFunctionNamedParams(testCaseSuppliers(), mapExpressionSupplier())));
    }

    private static Supplier<MapExpression> mapExpressionSupplier() {
        return () -> new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "fuzziness"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Match match = new Match(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
        // We need to add the QueryBuilder to the match expression, as it is used to implement equals() and hashCode() and
        // thus test the serialization methods. But we can only do this if the parameters make sense .
        if (args.get(0) instanceof FieldAttribute && args.get(1).foldable()) {
            QueryBuilder queryBuilder = TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, match).toQueryBuilder();
            match = (Match) match.replaceQueryBuilder(queryBuilder);
        }
        return match;
    }
}
