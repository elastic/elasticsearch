/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.FilterExec;
import org.elasticsearch.xpack.eql.plan.physical.LimitExec;
import org.elasticsearch.xpack.eql.plan.physical.LocalExec;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.eql.plan.physical.OrderExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.plan.physical.UnplannedExec;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

import java.util.Arrays;
import java.util.List;

class Mapper extends RuleExecutor<PhysicalPlan> {

    PhysicalPlan map(LogicalPlan plan) {
        return execute(planLater(plan));
    }

    @Override
    protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
        Batch conversion = new Batch("Mapping", new SimpleExecMapper());

        return Arrays.asList(conversion);
    }

    private static PhysicalPlan planLater(LogicalPlan plan) {
        return new UnplannedExec(plan.source(), plan);
    }

    private static class SimpleExecMapper extends MapExecRule<LogicalPlan> {

        @Override
        protected PhysicalPlan map(LogicalPlan p) {

            if (p instanceof LocalRelation) {
                return new LocalExec(p.source(), ((LocalRelation) p).executable());
            }

            if (p instanceof Filter) {
                Filter fl = (Filter) p;
                return new FilterExec(p.source(), map(fl.child()), fl.condition());
            }

            if (p instanceof OrderBy) {
                OrderBy o = (OrderBy) p;
                return new OrderExec(p.source(), map(o.child()), o.order());
            }

            if (p instanceof Limit) {
                Limit l = (Limit) p;
                return new LimitExec(p.source(), map(l.child()), l.limit());
            }

            if (p instanceof EsRelation) {
                EsRelation c = (EsRelation) p;
                List<Attribute> output = c.output();
                QueryContainer container = new QueryContainer();
                if (c.frozen()) {
                    container = container.withFrozen();
                }
                return new EsQueryExec(p.source(), c.index().name(), output, container);
            }

            return planLater(p);
        }
    }

    abstract static class MapExecRule<SubPlan extends LogicalPlan> extends Rule<UnplannedExec, PhysicalPlan> {

        private final Class<SubPlan> subPlanToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return plan.transformUp(this::rule, UnplannedExec.class);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected final PhysicalPlan rule(UnplannedExec plan) {
            LogicalPlan subPlan = plan.plan();
            if (subPlanToken.isInstance(subPlan)) {
                return map((SubPlan) subPlan);
            }
            return plan;
        }

        protected abstract PhysicalPlan map(SubPlan plan);
    }
}