/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.expression.LocalSurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;

public class LocalSubstituteSurrogateExpressions extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        return context.searchStats() != null ? plan.transformUp(Eval.class, eval -> substitute(eval, context.searchStats())) : plan;
    }

    private LogicalPlan substitute(Eval eval, SearchStats searchStats) {
        // check the filter in children plans
        return eval.transformExpressionsOnly(Function.class, f -> substitute(f, eval, searchStats));
    }

    private List<EsqlBinaryComparison> predicates(Eval eval, Expression field) {
        List<EsqlBinaryComparison> binaryComparisons = new ArrayList<>();
        eval.forEachUp(Filter.class, filter -> {
            Expression condition = filter.condition();
            if (condition instanceof And and) {
                Predicates.splitAnd(and).forEach(e -> addBinaryComparisonOnField(e, field, binaryComparisons));
            } else {
                addBinaryComparisonOnField(condition, field, binaryComparisons);
            }
        });
        return binaryComparisons;
    }

    private void addBinaryComparisonOnField(Expression expression, Expression field, List<EsqlBinaryComparison> binaryComparisons) {
        if (expression instanceof EsqlBinaryComparison esqlBinaryComparison
            && esqlBinaryComparison.right().foldable()
            && esqlBinaryComparison.left().semanticEquals(field)) {
            binaryComparisons.add(esqlBinaryComparison);
        }
    }

    /**
     * Perform the actual substitution with {@code SearchStats} and predicates in the query.
     */
    private Expression substitute(Expression e, Eval eval, SearchStats searchStats) {
        if (e instanceof LocalSurrogateExpression s) {
            List<EsqlBinaryComparison> binaryComparisons = new ArrayList<>();
            // extract relevant predicates from the query
            if (e instanceof DateTrunc dateTrunc) {
                binaryComparisons.addAll(predicates(eval, dateTrunc.field()));
            } else if (e instanceof Bucket bucket) {
                binaryComparisons.addAll(predicates(eval, bucket.field()));
            }
            Expression surrogate = s.surrogate(searchStats, binaryComparisons);
            if (surrogate != null) {
                return surrogate;
            }
        }
        return e;
    }
}
