/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.FilterExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.Arrays;

class QueryFolder extends RuleExecutor<PhysicalPlan> {

    PhysicalPlan fold(PhysicalPlan plan) {
        return execute(plan);
    }

    @Override
    protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
        Batch fold = new Batch("Fold queries",
                new FoldFilter()
        );
        Batch finish = new Batch("Finish query", Limiter.ONCE,
                new PlanOutputToQueryRef()
        );
        
        return Arrays.asList(fold, finish);
    }
    
    private static class FoldFilter extends FoldingRule<FilterExec> {

        @Override
        protected PhysicalPlan rule(FilterExec plan) {
            if (plan.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) plan.child();
                QueryContainer qContainer = exec.queryContainer();

                Query query = ExpressionTranslators.toQuery(plan.condition());

                if (qContainer.query() != null || query != null) {
                    query = ExpressionTranslators.and(plan.source(), qContainer.query(), query);
                }

                qContainer = qContainer.with(query);
                return exec.with(qContainer);
            }
            return plan;
        }
    }
    
    private static class PlanOutputToQueryRef extends FoldingRule<EsQueryExec> {
        @Override
        protected PhysicalPlan rule(EsQueryExec exec) {
            QueryContainer qContainer = exec.queryContainer();

            for (Attribute attr : exec.output()) {
                qContainer = qContainer.addColumn(attr);
            }

            // after all attributes have been resolved
            return exec.with(qContainer);
        }
    }

    abstract static class FoldingRule<SubPlan extends PhysicalPlan> extends Rule<SubPlan, PhysicalPlan> {

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return plan.transformUp(this::rule, typeToken());
        }

        @Override
        protected abstract PhysicalPlan rule(SubPlan plan);
    }
}