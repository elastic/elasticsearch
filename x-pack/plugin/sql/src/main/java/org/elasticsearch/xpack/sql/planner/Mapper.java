/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;
import org.elasticsearch.xpack.sql.plan.logical.Join;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;
import org.elasticsearch.xpack.sql.plan.logical.With;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.sql.plan.physical.CommandExec;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.FilterExec;
import org.elasticsearch.xpack.sql.plan.physical.LimitExec;
import org.elasticsearch.xpack.sql.plan.physical.LocalExec;
import org.elasticsearch.xpack.sql.plan.physical.OrderExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PivotExec;
import org.elasticsearch.xpack.sql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.sql.plan.physical.UnplannedExec;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;

import java.util.Arrays;
import java.util.List;

class Mapper extends RuleExecutor<PhysicalPlan> {

    public PhysicalPlan map(LogicalPlan plan) {
        return execute(planLater(plan));
    }

    @Override
    protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
        Batch conversion = new Batch("Mapping",
                new JoinMapper(),
                new SimpleExecMapper()
                );

        return Arrays.asList(conversion);
    }

    private static PhysicalPlan planLater(LogicalPlan plan) {
        return new UnplannedExec(plan.source(), plan);
    }

    private static class SimpleExecMapper extends MapExecRule<LogicalPlan> {

        @Override
        protected PhysicalPlan map(LogicalPlan p) {
            if (p instanceof Command) {
                return new CommandExec(p.source(), (Command) p);
            }

            if (p instanceof LocalRelation) {
                return new LocalExec(p.source(), ((LocalRelation) p).executable());
            }

            if (p instanceof Project) {
                Project pj = (Project) p;
                return new ProjectExec(p.source(), map(pj.child()), pj.projections());
            }

            if (p instanceof Filter) {
                Filter fl = (Filter) p;
                return new FilterExec(p.source(), map(fl.child()), fl.condition());
            }

            if (p instanceof OrderBy) {
                OrderBy o = (OrderBy) p;
                return new OrderExec(p.source(), map(o.child()), o.order());
            }

            if (p instanceof Aggregate) {
                Aggregate a = (Aggregate) p;
                // analysis and optimizations have converted the grouping into actual attributes
                return new AggregateExec(p.source(), map(a.child()), a.groupings(), a.aggregates());
            }

            if (p instanceof Pivot) {
                Pivot pv = (Pivot) p;
                return new PivotExec(pv.source(), map(pv.child()), pv);
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

            if (p instanceof Limit) {
                Limit l = (Limit) p;
                return new LimitExec(p.source(), map(l.child()), l.limit());
            }
            // TODO: Translate With in a subplan
            if (p instanceof With) {
                throw new UnsupportedOperationException("With should have been translated already");
            }

            return planLater(p);
        }
    }

    private static class JoinMapper extends MapExecRule<Join> {

        @Override
        protected PhysicalPlan map(Join j) {
            return join(j);
        }

        private PhysicalPlan join(Join join) {
            //TODO: pick up on nested/parent-child docs
            // 2. Hash?
            // 3. Cartesian
            // 3. Fallback to nested loop


            throw new UnsupportedOperationException("Don't know how to handle join " + join.nodeString());
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
