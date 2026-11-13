/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * Replaces {@code COUNT(field)} with {@code COUNT(*)} when a filter below the aggregation guarantees that every row has exactly
 * one value for the field. Counting rows needs no field values, and it is the form that later rules can push down to Lucene,
 * e.g. {@code PushCountQueryAndTagsToSource} for date histograms.
 * <pre>
 * | WHERE date >= "2015-01-01" AND date &lt; "2015-03-01"
 * | STATS c = COUNT(date) BY d = DATE_TRUNC(10 days, date)
 * </pre>
 * becomes
 * <pre>
 * | WHERE date >= "2015-01-01" AND date &lt; "2015-03-01"
 * | STATS c = COUNT(*) BY d = DATE_TRUNC(10 days, date)
 * </pre>
 * A comparison of the field with a constant is null, and so filters the row out, both when the field is missing and when it is
 * multivalued. Only comparisons that every row must pass count, i.e. the conjuncts of the filters between the aggregation and
 * the source.
 */
public class ReplaceCountFieldWithCountStar extends OptimizerRules.OptimizerRule<Aggregate> {

    public ReplaceCountFieldWithCountStar() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (aggregate instanceof TimeSeriesAggregate) {
            return aggregate;
        }
        return aggregate.transformExpressionsOnly(Count.class, count -> {
            if (count.field() instanceof FieldAttribute field
                && count.hasFilter() == false
                && count.parameters().isEmpty()
                && isSingleValued(field, aggregate.child())) {
                return new Count(
                    count.source(),
                    Literal.keyword(count.field().source(), StringUtils.WILDCARD),
                    count.filter(),
                    count.window()
                );
            }
            return count;
        });
    }

    /**
     * Whether all the rows produced by the plan have exactly one value for the field. Only the filters on the chain of unary
     * plans are considered: a filter on one side of a join, or in one branch of a fork, does not apply to all the rows.
     */
    private static boolean isSingleValued(FieldAttribute field, LogicalPlan plan) {
        while (true) {
            if (plan instanceof Filter filter) {
                for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                    if (isComparisonWithConstant(conjunct, field)) {
                        return true;
                    }
                }
            }
            if (plan instanceof UnaryPlan unary) {
                plan = unary.child();
            } else {
                return false;
            }
        }
    }

    private static boolean isComparisonWithConstant(Expression expression, FieldAttribute field) {
        if (expression instanceof EsqlBinaryComparison comparison) {
            return (comparison.left().semanticEquals(field) && comparison.right().foldable())
                || (comparison.right().semanticEquals(field) && comparison.left().foldable());
        }
        return false;
    }
}
