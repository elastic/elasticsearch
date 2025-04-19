/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * This is essentially testing MultiMatch as if it was a regular match: it makes a singular list of the supplied "field" parameter.
 */
@FunctionName("multi_match")
public class MultiMatchTests extends MatchTests {

    public MultiMatchTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        // Note we are reversing the order of arguments here.
        MultiMatch mm = new MultiMatch(source, args.get(1), List.of(args.get(0)), args.get(2));
        // We need to add the QueryBuilder to the multi_match expression, as it is used to implement equals() and hashCode() and
        // thus test the serialization methods. But we can only do this if the parameters make sense .
        if (mm.query().foldable() && mm.fields().stream().allMatch(field -> field instanceof FieldAttribute)) {
            QueryBuilder queryBuilder = TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, mm).toQueryBuilder();
            mm.replaceQueryBuilder(queryBuilder);
        }
        return mm;
    }
}
