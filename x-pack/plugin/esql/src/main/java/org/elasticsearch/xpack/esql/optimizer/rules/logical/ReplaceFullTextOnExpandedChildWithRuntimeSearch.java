/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;

import java.util.ArrayList;
import java.util.List;

/**
 * A Lucene query runs on the document a row was read from, which for a column produced by {@code NESTED_EXPAND} is the parent
 * of the child the value belongs to, so an index search of such a column silently matches nothing. This rule makes the search
 * run per row instead, over the values of the child column: the column is wrapped in {@code TO_TEXT} declaring the analyzer
 * that the mapping gives the field, which {@link FullTextFunction#expandedChildAnalyzer} chooses and which turns the search
 * into a runtime search. Searches it does not cover are rejected by the verifier.
 */
public final class ReplaceFullTextOnExpandedChildWithRuntimeSearch extends OptimizerRules.OptimizerRule<Filter> {
    private static final String ANALYZER = "analyzer";

    @Override
    protected LogicalPlan rule(Filter filter) {
        Expression condition = filter.condition()
            .transformDown(SingleFieldFullTextFunction.class, function -> searchPerRow(filter.child(), function));
        return condition == filter.condition() ? filter : filter.with(condition);
    }

    private static Expression searchPerRow(LogicalPlan expansion, SingleFieldFullTextFunction function) {
        if (function.field() instanceof FieldAttribute child) {
            return searchPerRow(expansion, function, child);
        }
        return function;
    }

    private static Expression searchPerRow(LogicalPlan expansion, SingleFieldFullTextFunction function, FieldAttribute child) {
        if (NestedExpand.expands(expansion, child) == false) {
            return function;
        }
        String analyzer = FullTextFunction.expandedChildAnalyzer(function, child);
        if (analyzer == null) {
            return function;
        }
        MapExpression options = new MapExpression(
            child.source(),
            List.of(Literal.keyword(child.source(), ANALYZER), Literal.keyword(child.source(), analyzer))
        );
        List<Expression> children = new ArrayList<>(function.children());
        children.set(children.indexOf(function.field()), new ToText(child.source(), child, options));
        return function.replaceChildren(children);
    }
}
