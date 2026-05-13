/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * A {@link UnionAll} created by rewriting a disjunctive (OR) condition containing IN/NOT IN subqueries.
 * For example:
 * <pre>
 * WHERE a IN (sub1) OR b IN (sub2)
 * </pre>
 * is rewritten to:
 * <pre>
 * UnionAllFromDisjunctiveInSubquery(
 *   Filter(a IN sub1, child),
 *   Filter(NOT(a IN sub1) AND b IN sub2, child)
 * )
 * </pre>
 * This subclass overrides {@link #postOptimizationPlanVerification()} to reject nesting with
 * FROM subqueries, FORK, or other disjunctive IN subqueries, which would produce unsupported
 * nested UnionAll structures.
 */
public class UnionAllFromDisjunctiveInSubquery extends UnionAll {

    public UnionAllFromDisjunctiveInSubquery(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children, output);
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new UnionAllFromDisjunctiveInSubquery(source(), newChildren, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, UnionAllFromDisjunctiveInSubquery::new, children(), output());
    }

    @Override
    public UnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new UnionAllFromDisjunctiveInSubquery(source(), subPlans, output());
    }

    @Override
    public UnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new UnionAllFromDisjunctiveInSubquery(source(), subPlans, output);
    }

    @Override
    public UnionAll refreshOutput() {
        return new UnionAllFromDisjunctiveInSubquery(source(), children(), refreshedOutput());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        return UnionAllFromDisjunctiveInSubquery::checkDisjunctiveInSubqueryNesting;
    }

    /**
     * Disjunctive IN/NOT IN subqueries produce a UnionAll that cannot be nested inside other
     * branching structures (FROM subqueries, FORK, or other disjunctive IN subqueries).
     */
    private static void checkDisjunctiveInSubqueryNesting(LogicalPlan plan, Failures failures) {
        if (plan instanceof UnionAllFromDisjunctiveInSubquery self) {
            self.forEachDown(Fork.class, nested -> {
                if (nested == self) {
                    return;
                }
                String message;
                if (nested instanceof UnionAllFromDisjunctiveInSubquery) {
                    message = "Nested disjunctive (OR) IN subqueries are not supported";
                } else if (nested instanceof UnionAll) {
                    message = "Disjunctive (OR) IN subqueries are not supported inside FROM subqueries";
                } else {
                    message = "Disjunctive (OR) IN subqueries are not supported with FORK";
                }
                failures.add(Failure.fail(self, message));
            });
        }
    }
}
