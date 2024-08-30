/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
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

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

class OptimizerRules {

    private OptimizerRules() {}

    static class DependencyConsistency<P extends QueryPlan<P>> {

        void checkPlan(P p, Failures failures) {
            AttributeSet refs = references(p);
            AttributeSet input = p.inputSet();
            AttributeSet generated = generates(p);
            AttributeSet missing = refs.subtract(input).subtract(generated);
            if (missing.isEmpty() == false) {
                failures.add(fail(p, "Plan [{}] optimized incorrectly due to missing references {}", p.nodeString(), missing));
            }

            Set<String> outputAttributeNames = new HashSet<>();
            Set<NameId> outputAttributeIds = new HashSet<>();
            for (Attribute outputAttr : p.output()) {
                if (outputAttributeNames.add(outputAttr.name()) == false || outputAttributeIds.add(outputAttr.id()) == false) {
                    failures.add(
                        fail(
                            p,
                            "Plan [{}] optimized incorrectly due to duplicate output attribute {}",
                            p.nodeString(),
                            outputAttr.toString()
                        )
                    );
                }
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
        protected AttributeSet references(LogicalPlan plan) {
            if (plan instanceof Enrich enrich) {
                // The enrichFields are NamedExpressions, so we compute their references as well when just calling enrich.references().
                // But they are not actually referring to attributes from the input plan - only the match field does.
                return enrich.matchField().references();
            }
            return super.references(plan);
        }

        @Override
        protected AttributeSet generates(LogicalPlan logicalPlan) {
            // source-like operators
            if (logicalPlan instanceof EsRelation
                || logicalPlan instanceof LocalRelation
                || logicalPlan instanceof Row
                || logicalPlan instanceof Aggregate
                || logicalPlan instanceof InlineStats) {
                return logicalPlan.outputSet();
            }
            if (logicalPlan instanceof GeneratingPlan<?> generating) {
                return new AttributeSet(generating.generatedAttributes());
            }
            if (logicalPlan instanceof MvExpand mvExpand) {
                return new AttributeSet(mvExpand.expanded());
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

}
