/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.RegexExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.ql.common.Failures;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.plan.QueryPlan;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.ql.common.Failure.fail;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitOr;

class OptimizerRules {

    private OptimizerRules() {}

    static class DependencyConsistency<P extends QueryPlan<P>> {

        void checkPlan(P p, Failures failures) {
            AttributeSet refs = references(p);
            AttributeSet input = p.inputSet();
            AttributeSet generated = generates(p);
            AttributeSet missing = refs.subtract(input).subtract(generated);
            if (missing.size() > 0) {
                failures.add(fail(p, "Plan [{}] optimized incorrectly due to missing references {}", p.nodeString(), missing));
            }
        }

        protected AttributeSet references(P p) {
            return p.references();
        }

        protected AttributeSet generates(P p) {
            return AttributeSet.EMPTY;
        }
    }

    static class LogicalPlanDependencyCheck extends DependencyConsistency<LogicalPlan> {
        @Override
        protected AttributeSet generates(LogicalPlan logicalPlan) {
            // source-like operators
            if (logicalPlan instanceof EsRelation
                || logicalPlan instanceof LocalRelation
                || logicalPlan instanceof Row
                || logicalPlan instanceof Aggregate) {
                return logicalPlan.outputSet();
            }
            if (logicalPlan instanceof Eval eval) {
                return new AttributeSet(Expressions.asAttributes(eval.fields()));
            }
            if (logicalPlan instanceof RegexExtract extract) {
                return new AttributeSet(extract.extractedFields());
            }
            if (logicalPlan instanceof MvExpand mvExpand) {
                return new AttributeSet(mvExpand.expanded());
            }
            if (logicalPlan instanceof Enrich enrich) {
                return new AttributeSet(Expressions.asAttributes(enrich.enrichFields()));
            }

            return AttributeSet.EMPTY;
        }
    }

    static class PhysicalPlanDependencyCheck extends DependencyConsistency<PhysicalPlan> {
        @Override
        protected AttributeSet generates(PhysicalPlan physicalPlan) {
            // source-like operators
            if (physicalPlan instanceof EsSourceExec
                || physicalPlan instanceof EsStatsQueryExec
                || physicalPlan instanceof EsQueryExec
                || physicalPlan instanceof LocalSourceExec
                || physicalPlan instanceof RowExec
                || physicalPlan instanceof ExchangeExec
                || physicalPlan instanceof ExchangeSourceExec
                || physicalPlan instanceof AggregateExec
                || physicalPlan instanceof ShowExec) {
                return physicalPlan.outputSet();
            }

            if (physicalPlan instanceof FieldExtractExec fieldExtractExec) {
                return new AttributeSet(fieldExtractExec.attributesToExtract());
            }
            if (physicalPlan instanceof EvalExec eval) {
                return new AttributeSet(Expressions.asAttributes(eval.fields()));
            }
            if (physicalPlan instanceof RegexExtractExec extract) {
                return new AttributeSet(extract.extractedFields());
            }
            if (physicalPlan instanceof MvExpandExec mvExpand) {
                return new AttributeSet(mvExpand.expanded());
            }
            if (physicalPlan instanceof EnrichExec enrich) {
                return new AttributeSet(Expressions.asAttributes(enrich.enrichFields()));
            }

            return AttributeSet.EMPTY;
        }

        @Override
        protected AttributeSet references(PhysicalPlan plan) {
            if (plan instanceof AggregateExec aggregate) {
                if (aggregate.getMode() == AggregateExec.Mode.FINAL) {
                    // lousy hack - need to generate the intermediate aggs yet the intermediateAggs method keep creating new IDs on each
                    // call
                    // in practice, the final aggregate should clearly declare the expected properties not hold on the original ones
                    // as they no longer apply
                    return aggregate.inputSet();
                }
            }
            return plan.references();
        }
    }

    /**
     * Combine disjunctions on the same field into an In expression.
     * This rule looks for both simple equalities:
     * 1. a == 1 OR a == 2 becomes a IN (1, 2)
     * and combinations of In
     * 2. a == 1 OR a IN (2) becomes a IN (1, 2)
     * 3. a IN (1) OR a IN (2) becomes a IN (1, 2)
     *
     * This rule does NOT check for type compatibility as that phase has been
     * already be verified in the analyzer.
     */
    public static class CombineDisjunctionsToIn extends org.elasticsearch.xpack.ql.optimizer.OptimizerRules.OptimizerExpressionRule<Or> {
        public CombineDisjunctionsToIn() {
            super(org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection.UP);
        }

        protected In createIn(Expression key, List<Expression> values, ZoneId zoneId) {
            return new In(key.source(), key, values);
        }

        protected Equals createEquals(Expression k, Set<Expression> v, ZoneId finalZoneId) {
            return new Equals(k.source(), k, v.iterator().next(), finalZoneId);
        }

        @Override
        protected Expression rule(Or or) {
            Expression e = or;
            // look only at equals and In
            List<Expression> exps = splitOr(e);

            Map<Expression, Set<Expression>> found = new LinkedHashMap<>();
            ZoneId zoneId = null;
            List<Expression> ors = new LinkedList<>();

            for (Expression exp : exps) {
                if (exp instanceof Equals eq) {
                    // consider only equals against foldables
                    if (eq.right().foldable()) {
                        found.computeIfAbsent(eq.left(), k -> new LinkedHashSet<>()).add(eq.right());
                    } else {
                        ors.add(exp);
                    }
                    if (zoneId == null) {
                        zoneId = eq.zoneId();
                    }
                } else if (exp instanceof In in) {
                    found.computeIfAbsent(in.value(), k -> new LinkedHashSet<>()).addAll(in.list());
                    if (zoneId == null) {
                        zoneId = in.zoneId();
                    }
                } else {
                    ors.add(exp);
                }
            }

            if (found.isEmpty() == false) {
                // combine equals alongside the existing ors
                final ZoneId finalZoneId = zoneId;
                found.forEach(
                    (k, v) -> { ors.add(v.size() == 1 ? createEquals(k, v, finalZoneId) : createIn(k, new ArrayList<>(v), finalZoneId)); }
                );

                // TODO: this makes a QL `or`, not an ESQL `or`
                Expression combineOr = combineOr(ors);
                // check the result semantically since the result might different in order
                // but be actually the same which can trigger a loop
                // e.g. a == 1 OR a == 2 OR null --> null OR a in (1,2) --> literalsOnTheRight --> cycle
                if (e.semanticEquals(combineOr) == false) {
                    e = combineOr;
                }
            }

            return e;
        }
    }
}
